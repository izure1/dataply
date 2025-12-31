import { LockManager } from '../src/core/transaction/LockManager'

describe('LockManager', () => {
  let lockManager: LockManager

  beforeEach(() => {
    lockManager = new LockManager()
  })

  test('should acquire and release read lock', async () => {
    const pageId = 1
    const lockId = await lockManager.acquireRead(pageId)
    expect(lockId).toBeDefined()
    lockManager.release(lockId)
  })

  test('should allow multiple read locks on the same page', async () => {
    const pageId = 1
    const lockId1 = await lockManager.acquireRead(pageId)
    const lockId2 = await lockManager.acquireRead(pageId)

    expect(lockId1).toBeDefined()
    expect(lockId2).toBeDefined()
    expect(lockId1).not.toBe(lockId2)

    lockManager.release(lockId1)
    lockManager.release(lockId2)
  })

  test('should block write lock if read lock is held', async () => {
    const pageId = 1
    const readLockId = await lockManager.acquireRead(pageId)

    let writeLockAcquired = false
    const writeLockPromise = lockManager.acquireWrite(pageId).then((lockId) => {
      writeLockAcquired = true
      return lockId
    })

    // 잠시 대기하여 Write Lock이 획득되지 않음을 확인
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(writeLockAcquired).toBe(false)

    // Read Lock 해제 후 Write Lock 획득 확인
    lockManager.release(readLockId)
    const writeLockId = await writeLockPromise
    expect(writeLockAcquired).toBe(true)

    lockManager.release(writeLockId)
  })

  test('should block read lock if write lock is held', async () => {
    const pageId = 1
    const writeLockId = await lockManager.acquireWrite(pageId)

    let readLockAcquired = false
    const readLockPromise = lockManager.acquireRead(pageId).then((lockId) => {
      readLockAcquired = true
      return lockId
    })

    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(readLockAcquired).toBe(false)

    lockManager.release(writeLockId)
    const readLockId = await readLockPromise
    expect(readLockAcquired).toBe(true)

    lockManager.release(readLockId)
  })

  test('should block write lock if another write lock is held', async () => {
    const pageId = 1
    const writeLockId1 = await lockManager.acquireWrite(pageId)

    let writeLockAcquired2 = false
    const writeLockPromise2 = lockManager.acquireWrite(pageId).then((lockId) => {
      writeLockAcquired2 = true
      return lockId
    })

    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(writeLockAcquired2).toBe(false)

    lockManager.release(writeLockId1)
    const writeLockId2 = await writeLockPromise2
    expect(writeLockAcquired2).toBe(true)

    lockManager.release(writeLockId2)
  })
})
