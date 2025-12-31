import { Ryoiki } from 'ryoiki'

/**
 * Lock Manager class.
 * Controls concurrency for page access.
 * Implemented using the Ryoiki library.
 */
export class LockManager {
  private ryoiki: Ryoiki
  // LockID -> Unlock Function Mapping
  private unlockMap: Map<string, () => void> = new Map()

  constructor() {
    this.ryoiki = new Ryoiki()
  }

  /**
   * Requests a read (Shared) lock for a page.
   * Ryoiki maintains the lock until explicitly released, even if the callback ends.
   * @param pageId Page ID
   * @returns Lock ID to be used for releasing the lock
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
   * Requests a write (Exclusive) lock for a page.
   * @param pageId Page ID
   * @returns Lock ID to be used for releasing the lock
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
   * Releases a lock.
   * @param lockId Lock ID to release
   */
  release(lockId: string): void {
    const unlockFn = this.unlockMap.get(lockId)
    if (unlockFn) {
      unlockFn()
      this.unlockMap.delete(lockId)
    }
  }
}
