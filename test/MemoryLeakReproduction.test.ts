
import { Dataply } from '../src/core/Dataply'
import { DataplyAPI } from '../src/core/DataplyAPI'
import { Transaction } from '../src/core/transaction/Transaction'
import path from 'node:path'
import fs from 'node:fs'

// DataplyAPI의 protected streamWithDefault를 테스트하기 위해 확장
class TestDataplyAPI extends DataplyAPI {
  public async *testStream<T>(callback: (tx: Transaction) => AsyncGenerator<T>) {
    yield* this.streamWithDefault(callback)
  }

  public getLockManager() {
    return this.lockManager
  }
}

describe('Memory Leak Reproduction', () => {
  const testDir = path.join(__dirname, 'temp_leak_test')
  const dbPath = path.join(testDir, 'leak.db')

  beforeAll(async () => {
    if (!fs.existsSync(testDir)) await fs.promises.mkdir(testDir)
  })

  afterAll(async () => {
    if (fs.existsSync(testDir)) {
      try { await fs.promises.rm(testDir, { recursive: true, force: true }) } catch (e) { }
    }
  })

  test('streamWithDefault should leak locks when async generator is interrupted', async () => {
    const api = new TestDataplyAPI(dbPath, { pageSize: 4096, pageCacheCapacity: 100 })
    await api.init()

    const lockManager = api.getLockManager()

    // 1. 스트림 실행 및 중도 하차
    const stream = api.testStream(async function* (tx) {
      // 페이지 1에 대해 쓰기 락 획득 시도 (insert 등 수행 시 발생)
      await (tx as any).__acquireWriteLock(1)
      yield 'first'
      yield 'second'
    })

    for await (const _ of stream) {
      break // 루프 중단!
    }

    // 2. 락 매니저 상태 확인
    // 수정 후에는 락이 정상적으로 해제되어 unlockMap.size가 0이어야 함
    const unlockMap = (lockManager as any).unlockMap
    expect(unlockMap.size).toBe(0) // 누수 해결 확인

    await api.close()
  })
})
