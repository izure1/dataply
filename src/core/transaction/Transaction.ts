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
    await this.context.run(this, async () => {
      for (const hook of this.commitHooks) {
        await hook()
      }
    })

    // 1. WAL Prepare (Phase 1)
    if (this.pfs.wal && this.dirtyPages.size > 0) {
      await this.pfs.wal.prepareCommit(this.dirtyPages)
      // 2. WAL Finalize (Marker)
      await this.pfs.wal.writeCommitMarker()
    }

    // 3. Write dirty pages to disk (Checkpoint)
    for (const [pageId, data] of this.dirtyPages) {
      await this.pageStrategy.write(pageId, data)
    }

    // 4. WAL Auto-Checkpoint (Clear if threshold reached)
    if (this.pfs.wal) {
      this.pfs.wal.incrementWrittenPages(this.dirtyPages.size)
      if (this.pfs.wal.shouldCheckpoint(this.pfs.options.walCheckpointThreshold)) {
        await this.pfs.wal.clear()
      }
    }

    this.dirtyPages.clear()
    this.undoPages.clear()
    this.releaseAllLocks()
  }

  /**
   * Rolls back the transaction.
   */
  async rollback(): Promise<void> {
    if (this.bptreeTx) {
      this.bptreeTx.rollback()
    }

    // Restore undo pages to cache (not disk - just clear dirty)
    this.dirtyPages.clear()
    this.undoPages.clear()
    this.releaseAllLocks()
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
