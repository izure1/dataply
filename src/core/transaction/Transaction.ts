import type { PageFileSystem } from '../PageFileSystem'
import { BPTreeAsyncTransaction } from 'serializable-bptree'
import { LockManager } from './LockManager'
import { TransactionContext } from './TxContext'
import { PageMVCCStrategy } from '../PageMVCCStrategy'

/**
 * Transaction class.
 * Manages the lifecycle and resources of a database transaction.
 */
export class Transaction {
  /** Transaction ID */
  readonly id: number
  /** List of held lock IDs (LOCK_ID) */
  private heldLocks: Set<string> = new Set()
  /** Held page locks (PageID -> LockID) */
  private pageLocks: Map<number, string> = new Map()
  /** Dirty Pages modified by the transaction: PageID -> Modified Page Buffer */
  private dirtyPages: Map<number, Uint8Array> = new Map()
  /** Undo pages: PageID -> Original Page Buffer (Snapshot) */
  private undoPages: Map<number, Uint8Array> = new Map()
  /** BPTree Transaction instance */
  private bptreeTx?: BPTreeAsyncTransaction<number, number>
  /** Whether the BPTree transaction is dirty */
  private bptreeDirty: boolean = false
  /** List of callbacks to execute on commit */
  private commitHooks: (() => Promise<void>)[] = []
  /** Page MVCC Strategy for disk access */
  private readonly pageStrategy: PageMVCCStrategy
  /** Release function for global write lock, set by DataplyAPI */
  private _writeLockRelease: (() => void) | null = null

  /**
   * @param id Transaction ID
   * @param context Transaction context
   * @param pageStrategy Page MVCC Strategy for disk I/O
   * @param lockManager LockManager instance
   * @param pfs Page File System
   */
  constructor(
    id: number,
    readonly context: TransactionContext,
    pageStrategy: PageMVCCStrategy,
    private readonly lockManager: LockManager,
    private readonly pfs: PageFileSystem
  ) {
    this.id = id
    this.pageStrategy = pageStrategy
  }

  /**
   * Sets the BPTree transaction.
   * @param tx BPTree transaction
   */
  __setBPTreeTransaction(tx: BPTreeAsyncTransaction<number, number>) {
    this.bptreeTx = tx
  }

  /**
   * Returns the BPTree transaction.
   * @returns BPTree transaction
   */
  __getBPTreeTransaction(): BPTreeAsyncTransaction<number, number> | undefined {
    return this.bptreeTx
  }

  /**
   * Marks the BPTree transaction as dirty.
   */
  __markBPTreeDirty() {
    this.bptreeDirty = true
  }

  /**
   * Returns whether the BPTree transaction is dirty.
   * @returns True if dirty
   */
  __isBPTreeDirty() {
    return this.bptreeDirty
  }

  /**
   * Registers a commit hook.
   * @param hook Function to execute
   */
  onCommit(hook: () => Promise<void>) {
    this.commitHooks.push(hook)
  }

  /**
   * Sets the global write lock release function.
   * Called by DataplyAPI.runWithDefaultWrite when acquiring the lock.
   */
  __setWriteLockRelease(release: () => void): void {
    this._writeLockRelease = release
  }

  /**
   * Returns whether this transaction already has a write lock.
   */
  __hasWriteLockRelease(): boolean {
    return this._writeLockRelease !== null
  }

  /**
   * Reads a page. Uses dirty buffer if available, otherwise disk.
   * @param pageId Page ID
   * @returns Page data
   */
  async readPage(pageId: number): Promise<Uint8Array> {
    // Check dirty buffer first
    const dirty = this.dirtyPages.get(pageId)
    if (dirty) {
      return dirty
    }
    // Read from disk via strategy
    return await this.pageStrategy.read(pageId)
  }

  /**
   * Writes a page to the transaction buffer.
   * @param pageId Page ID
   * @param data Page data
   */
  async writePage(pageId: number, data: Uint8Array): Promise<void> {
    // Save undo snapshot if not already saved
    if (!this.undoPages.has(pageId)) {
      const existingData = await this.pageStrategy.read(pageId)
      const snapshot = new Uint8Array(existingData.length)
      snapshot.set(existingData)
      this.undoPages.set(pageId, snapshot)
    }
    // Store in dirty buffer
    this.dirtyPages.set(pageId, data)
  }

  /**
   * Acquires a write lock.
   * @param pageId Page ID
   */
  async __acquireWriteLock(pageId: number): Promise<void> {
    const existingLockId = this.pageLocks.get(pageId)
    if (existingLockId) {
      if (this.heldLocks.has(existingLockId)) {
        return
      }
    }

    const lockId = await this.lockManager.acquireWrite(pageId)
    this.heldLocks.add(lockId)
    this.pageLocks.set(pageId, lockId)
  }

  /**
   * Commits the transaction.
   */
  async commit(): Promise<void> {
    try {
      await this.context.run(this, async () => {
        for (const hook of this.commitHooks) {
          await hook()
        }
      })

      // 0. Acquire global lock to prevent concurrent checkpoint/commit issues
      let shouldTriggerCheckpoint = false
      await this.pfs.runGlobalLock(async () => {
        // 1. WAL Prepare (Phase 1)
        if (this.pfs.wal && this.dirtyPages.size > 0) {
          await this.pfs.wal.prepareCommit(this.dirtyPages)
          // 2. WAL Finalize (Marker)
          await this.pfs.wal.writeCommitMarker()
        }

        // 3. Write dirty pages (this now buffers in the strategy)
        for (const [pageId, data] of this.dirtyPages) {
          await this.pageStrategy.write(pageId, data)
        }

        // 4. Flush & checkpoint
        if (!this.pfs.wal) {
          // WAL이 없으면 해당 트랜잭션의 dirty pages만 즉시 디스크에 기록
          await this.pfs.strategy.flushPages(this.dirtyPages)
        }
        else {
          // WAL Auto-Checkpoint (Determine if needed)
          this.pfs.wal.incrementWrittenPages(this.dirtyPages.size)
          if (this.pfs.wal.shouldCheckpoint(this.pfs.options.walCheckpointThreshold)) {
            shouldTriggerCheckpoint = true
          }
        }
      })

      // 5. Trigger checkpoint outside the commit lock to avoid deadlock (ryoiki is non-reentrant)
      if (shouldTriggerCheckpoint) {
        await this.pfs.checkpoint()
      }

      this.dirtyPages.clear()
      this.undoPages.clear()
      this.releaseAllLocks()
    } finally {
      // Release global write lock so next write transaction can proceed
      if (this._writeLockRelease) {
        this._writeLockRelease()
        this._writeLockRelease = null
      }
    }
  }

  /**
   * Rolls back the transaction.
   */
  async rollback(): Promise<void> {
    try {
      if (this.bptreeTx) {
        this.bptreeTx.rollback()
      }

      // Restore undo pages to cache (not disk - just clear dirty)
      this.dirtyPages.clear()
      this.undoPages.clear()
      this.releaseAllLocks()
    } finally {
      // Release global write lock so next write transaction can proceed
      if (this._writeLockRelease) {
        this._writeLockRelease()
        this._writeLockRelease = null
      }
    }
  }

  /**
   * Returns the dirty pages map.
   */
  __getDirtyPages(): Map<number, Uint8Array> {
    return this.dirtyPages
  }

  /**
   * Releases all locks.
   */
  private releaseAllLocks(): void {
    for (const lockId of this.heldLocks) {
      this.lockManager.release(lockId)
    }
    this.heldLocks.clear()
    this.pageLocks.clear()
  }
}
