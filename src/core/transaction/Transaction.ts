import type { PageFileSystem } from '../PageFileSystem'
import type { AsyncMVCCTransaction } from 'mvcc-api'
import type { PageMVCCStrategy } from '../PageMVCCStrategy'
import { LockManager } from './LockManager'
import { TransactionContext } from './TxContext'

/**
 * Transaction class.
 * Manages the lifecycle and resources of a database transaction.
 * Internally wraps a nested AsyncMVCCTransaction for snapshot isolation.
 */
export class Transaction {
  /** Transaction ID */
  readonly id: number
  /** List of held lock IDs (LOCK_ID) */
  private heldLocks: Set<string> = new Set()
  /** Held page locks (PageID -> LockID) */
  private pageLocks: Map<number, string> = new Map()
  /** List of callbacks to execute on commit */
  private commitHooks: (() => Promise<void>)[] = []
  /** Nested MVCC Transaction for snapshot isolation (lazy init) */
  private mvccTx: AsyncMVCCTransaction<PageMVCCStrategy, number, Uint8Array> | null = null
  /** Root MVCC Transaction reference */
  private readonly rootTx: AsyncMVCCTransaction<PageMVCCStrategy, number, Uint8Array>
  /** Release function for global write lock, set by DataplyAPI */
  private _writeLockRelease: (() => void) | null = null

  /**
   * @param id Transaction ID
   * @param context Transaction context
   * @param rootTx Root MVCC Transaction
   * @param lockManager LockManager instance
   * @param pfs Page File System
   */
  constructor(
    id: number,
    readonly context: TransactionContext,
    rootTx: AsyncMVCCTransaction<PageMVCCStrategy, number, Uint8Array>,
    private readonly lockManager: LockManager,
    private readonly pfs: PageFileSystem,
  ) {
    this.id = id
    this.rootTx = rootTx
  }

  /**
   * Lazily initializes the nested MVCC transaction.
   * This ensures the snapshot is taken at the time of first access,
   * picking up the latest committed root version.
   */
  private ensureMvccTx(): AsyncMVCCTransaction<PageMVCCStrategy, number, Uint8Array> {
    if (!this.mvccTx) {
      this.mvccTx = this.rootTx.createNested()
    }
    return this.mvccTx
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
   * Reads a page through the MVCC transaction.
   * @param pageId Page ID
   * @returns Page data
   */
  async __readPage(pageId: number): Promise<Uint8Array> {
    const tx = this.ensureMvccTx()
    const data = await tx.read(pageId)
    if (data === null) {
      // 페이지가 없으면 빈 페이지 반환
      return new Uint8Array(this.pfs.pageSize)
    }
    // Copy-on-Read: mvccTx.read()는 root diskCache의 참조를 반환할 수 있음.
    // 호출자가 페이지를 in-place로 변이하면 diskCache가 오염됨.
    // 복사본을 반환하여 방지.
    const copy = new Uint8Array(data.length)
    copy.set(data)
    return copy
  }

  /**
   * Writes a page through the MVCC transaction.
   * @param pageId Page ID
   * @param data Page data
   */
  async __writePage(pageId: number, data: Uint8Array): Promise<void> {
    const tx = this.ensureMvccTx()
    // Copy-on-Write: mvcc-api에 참조 타입 전달 시 복사본 필요
    const exists = await tx.exists(pageId)
    if (exists) {
      const copy = new Uint8Array(data.length)
      copy.set(data)
      await tx.write(pageId, copy)
    } else {
      await tx.create(pageId, data)
    }
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

      const tx = this.ensureMvccTx()

      // 0. Acquire global lock to prevent concurrent checkpoint/commit issues
      let shouldTriggerCheckpoint = false
      await this.pfs.runGlobalLock(async () => {
        // 1. Collect dirty pages from the nested MVCC tx for WAL
        const entries = tx.getResultEntries()
        const dirtyPages = new Map<number, Uint8Array>()
        for (const entry of [...entries.created, ...entries.updated]) {
          dirtyPages.set(entry.key, entry.data)
        }
        const hasDirtyPages = dirtyPages.size > 0

        // 2. WAL Prepare (Phase 1)
        if (this.pfs.wal && hasDirtyPages) {
          await this.pfs.wal.prepareCommit(dirtyPages)
          // WAL Commit Marker
          await this.pfs.wal.writeCommitMarker()
        }

        // 3. Commit nested MVCC tx (merge to root)
        await tx.commit()

        // 4. Commit root MVCC tx (writes to disk via strategy)
        if (hasDirtyPages) {
          await this.rootTx.commit()
        }

        // 5. Checkpoint logic
        if (hasDirtyPages) {
          if (!this.pfs.wal) {
            // WAL이 없으면 즉시 fsync
            await this.pfs.strategy.sync()
          }
          else {
            // WAL Auto-Checkpoint
            this.pfs.wal.incrementWrittenPages(dirtyPages.size)
            if (this.pfs.wal.shouldCheckpoint(this.pfs.options.walCheckpointThreshold)) {
              shouldTriggerCheckpoint = true
            }
          }
        }
      })

      // 6. Trigger checkpoint outside the commit lock to avoid deadlock (ryoiki is non-reentrant)
      if (shouldTriggerCheckpoint) {
        await this.pfs.checkpoint()
      }
    } finally {
      this.releaseAllLocks()
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
      if (this.mvccTx) {
        this.mvccTx.rollback()
      }
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
