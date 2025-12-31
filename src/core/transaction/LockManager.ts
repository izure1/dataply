import { Ryoiki } from 'ryoiki'

/**
 * 락 매니저 클래스
 * 페이지 접근에 대한 동시성을 제어합니다.
 * Ryoiki 라이브러리를 사용하여 구현합니다.
 */
export class LockManager {
  private ryoiki: Ryoiki
  // LockID -> Unlock Function Mapping
  private unlockMap: Map<string, () => void> = new Map()

  constructor() {
    this.ryoiki = new Ryoiki()
  }

  /**
   * 페이지에 대한 읽기(Shared) 락을 요청합니다.
   * Ryoiki는 콜백이 종료되어도 명시적으로 해제하기 전까지 락을 유지합니다.
   * @param pageId 페이지 ID
   * @returns 락 해제 시 사용할 Lock ID
   */
  async acquireRead(pageId: number): Promise<string> {
    return new Promise<string>((resolve) => {
      this.ryoiki.readLock([pageId, pageId + 1], async (lockId) => {
        this.unlockMap.set(lockId, () => this.ryoiki.readUnlock(lockId))
        resolve(lockId)
      })
    })
  }

  /**
   * 페이지에 대한 쓰기(Exclusive) 락을 요청합니다.
   * @param pageId 페이지 ID
   * @returns 락 해제 시 사용할 Lock ID
   */
  async acquireWrite(pageId: number): Promise<string> {
    return new Promise<string>((resolve) => {
      this.ryoiki.writeLock([pageId, pageId + 1], async (lockId) => {
        this.unlockMap.set(lockId, () => this.ryoiki.writeUnlock(lockId))
        resolve(lockId)
      })
    })
  }

  /**
   * 락을 해제합니다.
   * @param lockId 해제할 락 ID
   */
  release(lockId: string): void {
    const unlockFn = this.unlockMap.get(lockId)
    if (unlockFn) {
      unlockFn()
      this.unlockMap.delete(lockId)
    }
  }
}
