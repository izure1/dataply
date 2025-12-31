import { LockManager } from './LockManager'
import { VirtualFileSystem } from '../VirtualFileSystem'

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
   */
  addUndoPage(pageId: number, buffer: Uint8Array) {
    if (!this.undoPages.has(pageId)) {
      this.undoPages.set(pageId, buffer)
    }
  }

  /**
   * Returns an Undo page.
   */
  getUndoPage(pageId: number): Uint8Array | undefined {
    return this.undoPages.get(pageId)
  }

  /**
   * Adds a Pending Index Update.
   */
  addPendingIndexUpdate(pk: number, newRid: number, oldRid: number) {
    this.pendingIndexUpdates.set(pk, { newRid, oldRid })
  }

  /**
   * Returns a Pending Index Update.
   */
  getPendingIndexUpdate(pk: number) {
    return this.pendingIndexUpdates.get(pk)
  }

  /**
   * Returns all Pending Index Updates.
   */
  getPendingIndexUpdates() {
    return this.pendingIndexUpdates
  }

  /**
   * Acquires a write lock.
   * @param pageId Page ID
   */
  async acquireWriteLock(pageId: number): Promise<void> {
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
    await this.vfs.commit(this)
    for (const hook of this.commitHooks) {
      await hook()
    }
    this.releaseAllLocks()
  }

  /**
   * Rolls back the transaction.
   */
  async rollback(): Promise<void> {
    await this.vfs.rollback(this)
    this.releaseAllLocks()
  }

  /** List of Dirty Pages modified by the transaction */
  private dirtyPages: Set<number> = new Set()

  /**
   * Adds a Dirty Page.
   * @param pageId Page ID
   */
  addDirtyPage(pageId: number) {
    this.dirtyPages.add(pageId)
  }

  /**
   * Returns the list of Dirty Pages.
   */
  getDirtyPages(): Set<number> {
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
