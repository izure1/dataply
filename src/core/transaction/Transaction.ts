import { BPTreeAsyncTransaction } from 'serializable-bptree'
import { LockManager } from './LockManager'
import { VirtualFileSystem } from '../VirtualFileSystem'
import { TransactionContext } from './TxContext'

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
  /** Undo Logs: PageID -> Original Page Buffer (Snapshot) */
  private undoPages: Map<number, Uint8Array> = new Map()
  /** Dirty Pages modified by the transaction */
  private dirtyPages: Set<number> = new Set()
  /** BPTree Transaction instance */
  private bptreeTx?: BPTreeAsyncTransaction<number, number>
  /** Whether the BPTree transaction is dirty */
  private bptreeDirty: boolean = false
  /** List of callbacks to execute on commit */
  private commitHooks: (() => Promise<void>)[] = []

  /**
   * @param id Transaction ID
   * @param vfs VFS instance
   * @param lockManager LockManager instance
   */
  constructor(
    id: number,
    readonly context: TransactionContext,
    private readonly vfs: VirtualFileSystem,
    private readonly lockManager: LockManager
  ) {
    this.id = id
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
   * Stores an Undo page.
   * Does not overwrite if the page is already stored (maintains the original snapshot).
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   * @param pageId Page ID
   * @param buffer Page buffer
   */
  __setUndoPage(pageId: number, buffer: Uint8Array) {
    this.undoPages.set(pageId, buffer)
  }

  /**
   * Returns an Undo page.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   * @param pageId Page ID
   * @returns Undo page
   */
  __getUndoPage(pageId: number): Uint8Array | undefined {
    return this.undoPages.get(pageId)
  }

  /**
   * Returns true if the transaction has an Undo page for the given page ID.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   * @param pageId Page ID
   * @returns True if the transaction has an Undo page for the given page ID
   */
  __hasUndoPage(pageId: number): boolean {
    return this.undoPages.has(pageId)
  }

  /**
   * Acquires a write lock.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
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
   * Prepares the transaction for commit (Phase 1 of 2PC).
   * Writes dirty pages to WAL but does not update the database file yet.
   */
  async prepare(): Promise<void> {
    await this.vfs.prepareCommit(this)
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

    await this.vfs.prepareCommit(this)
    await this.vfs.finalizeCommit(this)

    this.releaseAllLocks()
  }

  /**
   * Rolls back the transaction.
   */
  async rollback(): Promise<void> {
    await this.vfs.rollback(this)
    this.releaseAllLocks()
  }

  /**
   * Adds a Dirty Page.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   * @param pageId Page ID
   */
  __addDirtyPage(pageId: number) {
    this.dirtyPages.add(pageId)
  }

  /**
   * Returns the list of Dirty Pages.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   */
  __getDirtyPages(): Set<number> {
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
