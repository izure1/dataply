import { LockManager } from './LockManager'
import { VirtualFileSystem } from '../VirtualFileSystem'

/**
 * 트랜잭션 클래스
 * 데이터베이스 트랜잭션의 생명주기와 리소스를 관리합니다.
 */
export class Transaction {
  /** 트랜잭션 ID */
  readonly id: number
  /** 보유한 락 ID 목록 (LOCK_ID) */
  private heldLocks: Set<string> = new Set()
  /** 보유한 페이지 락 (PageID -> LockID) */
  private pageLocks: Map<number, string> = new Map()

  /** Undo Logs: PageID -> Original Page Buffer (Snapshot) */
  private undoPages: Map<number, Uint8Array> = new Map()
  /** Pending Index Updates: PK -> { newRid, oldRid } */
  private pendingIndexUpdates: Map<number, { newRid: number, oldRid: number }> = new Map()
  /** 커밋 시 실행할 콜백 목록 */
  private commitHooks: (() => Promise<void>)[] = []

  /**
   * @param id 트랜잭션 ID
   * @param vfs VFS 인스턴스
   * @param lockManager LockManager 인스턴스
   */
  constructor(
    id: number,
    private vfs: VirtualFileSystem,
    private lockManager: LockManager
  ) {
    this.id = id
  }

  /**
   * 커밋 훅을 등록합니다.
   * @param hook 실행할 함수
   */
  onCommit(hook: () => Promise<void>) {
    this.commitHooks.push(hook)
  }

  /**
   * Undo 페이지를 저장합니다.
   * 이미 저장된 페이지가 있다면 덮어쓰지 않습니다. (최초의 Snapshot 유지)
   */
  addUndoPage(pageId: number, buffer: Uint8Array) {
    if (!this.undoPages.has(pageId)) {
      this.undoPages.set(pageId, buffer)
    }
  }

  /**
   * Undo 페이지를 반환합니다.
   */
  getUndoPage(pageId: number): Uint8Array | undefined {
    return this.undoPages.get(pageId)
  }

  /**
   * Pending Index Update를 추가합니다.
   */
  addPendingIndexUpdate(pk: number, newRid: number, oldRid: number) {
    this.pendingIndexUpdates.set(pk, { newRid, oldRid })
  }

  /**
   * Pending Index Update를 반환합니다.
   */
  getPendingIndexUpdate(pk: number) {
    return this.pendingIndexUpdates.get(pk)
  }

  /**
   * 모든 Pending Index Update를 반환합니다.
   */
  getPendingIndexUpdates() {
    return this.pendingIndexUpdates
  }

  /**
   * 쓰기 락을 획득합니다.
   * @param pageId 페이지 ID
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
   * 트랜잭션을 커밋합니다.
   */
  async commit(): Promise<void> {
    await this.vfs.commit(this)
    for (const hook of this.commitHooks) {
      await hook()
    }
    this.releaseAllLocks()
  }

  /**
   * 트랜잭션을 롤백합니다.
   */
  async rollback(): Promise<void> {
    await this.vfs.rollback(this)
    this.releaseAllLocks()
  }

  /** 트랜잭션이 수정한 Dirty Page 목록 */
  private dirtyPages: Set<number> = new Set()

  /**
   * Dirty Page를 추가합니다.
   * @param pageId 페이지 ID
   */
  addDirtyPage(pageId: number) {
    this.dirtyPages.add(pageId)
  }

  /**
   * Dirty Page 목록을 반환합니다.
   */
  getDirtyPages(): Set<number> {
    return this.dirtyPages
  }

  /**
   * 모든 락을 해제합니다.
   */
  private releaseAllLocks(): void {
    for (const lockId of this.heldLocks) {
      this.lockManager.release(lockId)
    }
    this.heldLocks.clear()
    this.pageLocks.clear()
  }
}
