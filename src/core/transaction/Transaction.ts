import { LockManager } from './LockManager'
import { VirtualFileSystem } from '../VirtualFileSystem'
import { TxContext } from './TxContext'

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
  /** List of Dirty Pages modified by the transaction */
  private dirtyPages: Set<number> = new Set()
  /** Pending Index Updates: PK -> { newRid, oldRid } */
  private pendingIndexUpdates: Map<number, { newRid: number, oldRid: number }> = new Map()
  /** List of callbacks to execute on commit */
  private commitHooks: (() => Promise<void>)[] = []

  /**
   * @param id Transaction ID
   * @param vfs VFS instance
   * @param lockManager LockManager instance
   */
  constructor(
    id: number,
    private vfs: VirtualFileSystem,
    private lockManager: LockManager
  ) {
    this.id = id
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
   * Adds a Pending Index Update.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   * @param pk PK
   * @param newRid New RID
   * @param oldRid Old RID
   */
  __addPendingIndexUpdate(pk: number, newRid: number, oldRid: number) {
    this.pendingIndexUpdates.set(pk, { newRid, oldRid })
  }

  /**
   * Returns a Pending Index Update.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   * @param pk PK
   * @returns Pending Index Update
   */
  __getPendingIndexUpdate(pk: number) {
    return this.pendingIndexUpdates.get(pk)
  }

  /**
   * Returns all Pending Index Updates.
   * Does not call this method directly. It is called by the `VirtualFileSystem` instance.
   */
  __getPendingIndexUpdates() {
    return this.pendingIndexUpdates
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
    // If prepare was not called externally, call it now (Auto-Commit mode)
    // We can check this by seeing if WAL has been written? 
    // Or we can rely on VFS to handle idempotency of prepareCommit.
    // For now, we assume simple local commit flow executes both.

    await this.vfs.prepareCommit(this)
    await this.vfs.finalizeCommit(this)

    await TxContext.run(this, async () => {
      for (const hook of this.commitHooks) {
        await hook()
      }
    })
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
