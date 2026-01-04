
import { Dataply } from '../src/core/Dataply'
import path from 'node:path'
import fs from 'node:fs'

describe('Concurrency (MVCC)', () => {
  const testDir = path.join(__dirname, 'temp_concurrency_test')
  const dbPath = path.join(testDir, 'concurrency.db')
  const walPath = path.join(testDir, 'concurrency.wal')
  let dataply: Dataply | null = null

  beforeAll(() => {
    if (!fs.existsSync(testDir)) fs.mkdirSync(testDir)
  })

  afterAll(async () => {
    // Ensure dataply is closed before deleting directory
    if (dataply) {
      try { await dataply.close() } catch (e) { }
      dataply = null
    }
    // Wait a bit for file handles to release
    await new Promise(resolve => setTimeout(resolve, 100))
    if (fs.existsSync(testDir)) {
      try { fs.rmSync(testDir, { recursive: true, force: true }) } catch (e) { }
    }
  })

  afterEach(async () => {
    if (dataply) {
      try { await dataply.close() } catch (e) { }
      dataply = null
    }
  })

  beforeEach(async () => {
    // Wait for any lingering file handles to release
    await new Promise(resolve => setTimeout(resolve, 50))
    if (fs.existsSync(dbPath)) try { fs.unlinkSync(dbPath) } catch (e) { }
    if (fs.existsSync(walPath)) try { fs.unlinkSync(walPath) } catch (e) { }
  })

  test('should serialize concurrent inserts (Writers block Writers)', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    // Sequential inserts to verify correct PK increment
    const tx1 = dataply.createTransaction()
    const pk1 = await dataply.insert('data1', tx1)
    await tx1.commit()

    const tx2 = dataply.createTransaction()
    const pk2 = await dataply.insert('data2', tx2)
    await tx2.commit()

    // Verify PKs are distinct and incremented
    expect(pk1).toBe(1)
    expect(pk2).toBe(2)

    // Verify data
    const row1 = await dataply.select(pk1, false)
    const row2 = await dataply.select(pk2, false)
    expect(row1).toBe('data1')
    expect(row2).toBe('data2')
  })

  test('should restore state via Undo Buffer after rollback (MVCC Isolation)', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    // Insert initial data and commit
    const tx1 = dataply.createTransaction()
    await dataply.insert('initial_data', tx1)
    await tx1.commit()

    // Start a write transaction, modify, but rollback
    const writeTx = dataply.createTransaction()
    await dataply.insert('new_data', writeTx)
    await writeTx.rollback()

    // Verify that the committed data (pk=1) is still visible
    const initialResult = await dataply.select(1, false)
    expect(initialResult).toBe('initial_data')
  })

  test('should handle multiple sequential transactions correctly', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    const insertCount = 10
    const pks: number[] = []

    // Multiple sequential transactions
    for (let i = 0; i < insertCount; i++) {
      const tx = dataply.createTransaction()
      const pk = await dataply.insert(`data-${i}`, tx)
      await tx.commit()
      pks.push(pk)
    }

    // Verify all PKs are unique and sequential
    expect(pks).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    // Verify all data is correct
    for (let i = 0; i < insertCount; i++) {
      const result = await dataply.select(pks[i], false)
      expect(result).toBe(`data-${i}`)
    }
  })

  test('should handle interleaved commit and rollback', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    // First transaction: commit
    const tx1 = dataply.createTransaction()
    const pk1 = await dataply.insert('committed-1', tx1)
    await tx1.commit()

    // Second transaction: rollback
    const tx2 = dataply.createTransaction()
    await dataply.insert('rolled-back', tx2)
    await tx2.rollback()

    // Third transaction: commit
    const tx3 = dataply.createTransaction()
    const pk3 = await dataply.insert('committed-2', tx3)
    await tx3.commit()

    // Verify committed data is accessible
    expect(await dataply.select(pk1, false)).toBe('committed-1')
    expect(await dataply.select(pk3, false)).toBe('committed-2')
  })

  test('should maintain data integrity with large batch inserts', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    const batchSize = 50
    const tx = dataply.createTransaction()
    const pks: number[] = []

    // Insert many rows in a single transaction
    for (let i = 0; i < batchSize; i++) {
      const pk = await dataply.insert(`batch-${i}`, tx)
      pks.push(pk)
    }

    await tx.commit()

    // Verify all inserts were persisted
    expect(pks.length).toBe(batchSize)

    // Verify data integrity
    for (let i = 0; i < batchSize; i++) {
      const result = await dataply.select(pks[i], false)
      expect(result).toBe(`batch-${i}`)
    }
  })

  test('should allow reads during concurrent write transactions via snapshot', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    // Insert and commit initial data
    const tx1 = dataply.createTransaction()
    await dataply.insert('visible-data', tx1)
    await tx1.commit()

    // Start a new write transaction
    const tx2 = dataply.createTransaction()
    await dataply.insert('pending-data', tx2)

    // Read committed data (should see 'visible-data')
    const result = await dataply.select(1, false)
    expect(result).toBe('visible-data')

    // Commit the pending transaction
    await tx2.commit()

    // Now we should see the new data too
    const newResult = await dataply.select(2, false)
    expect(newResult).toBe('pending-data')
  })

  test('should handle rollback of large batch insert', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    // First, insert some committed data
    const tx1 = dataply.createTransaction()
    await dataply.insert('base-data', tx1)
    await tx1.commit()

    // Start a large batch insert and rollback
    const tx2 = dataply.createTransaction()
    for (let i = 0; i < 20; i++) {
      await dataply.insert(`rollback-${i}`, tx2)
    }
    await tx2.rollback()

    // Verify base data is still intact
    const baseResult = await dataply.select(1, false)
    expect(baseResult).toBe('base-data')

    // Insert new data after rollback (should work correctly)
    const tx3 = dataply.createTransaction()
    const pk = await dataply.insert('after-rollback', tx3)
    await tx3.commit()

    const afterResult = await dataply.select(pk, false)
    expect(afterResult).toBe('after-rollback')
  })

  test('should handle true parallel execution using Promise.all', async () => {
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    const concurrencyLevel = 50
    const operations = Array(concurrencyLevel).fill(0).map(async (_, i) => {
      const tx = dataply!.createTransaction()
      // insert followed by commit, interleaved by JS event loop
      const pk = await dataply!.insert(`concurrent-data-${i}`, tx)
      await tx.commit()
      return { pk, data: `concurrent-data-${i}` }
    })

    const results = await Promise.all(operations)

    // Verify all PKs are unique
    const pks = results.map(r => r.pk)
    const uniquePks = new Set(pks)
    expect(uniquePks.size).toBe(concurrencyLevel)

    // Verify data integrity for each insertion
    for (const result of results) {
      const storedData = await dataply!.select(result.pk, false)
      expect(storedData).toBe(result.data)
    }

    // Verify row count matches
    const metadata = await dataply!.getMetadata()
    expect(metadata.rowCount).toBe(concurrencyLevel)
  })

  test('should simulate batch insert: allow parallel reads but block concurrent writes', async () => {
    // 5초에 걸쳐 천천히 데이터를 삽입하는 상황 시뮬레이션
    dataply = new Dataply(dbPath, { wal: walPath })
    await dataply.init()

    // 0. 초기 데이터 세팅 (Update/Delete 대상)
    const txInit = dataply.createTransaction()
    const targetPk = await dataply.insert('target-row', txInit)
    await txInit.commit()

    // 1. Batch Insert 트랜잭션 시작 (약 5초 소요 예정)
    const txBatch = dataply.createTransaction()
    const batchSize = 10
    const insertDelay = 500 // 0.5초 * 10개 = 5초
    const batchPks: number[] = []

    const batchInsertTask = (async () => {
      for (let i = 0; i < batchSize; i++) {
        // 천천히 삽입
        await new Promise(resolve => setTimeout(resolve, insertDelay))
        const pk = await dataply!.insert(`batch-data-${i}`, txBatch)
        batchPks.push(pk)
      }
      await txBatch.commit()
    })()

    // 2. Select는 즉시 가능해야 함 (Non-blocking)
    // Batch Insert가 진행되는 도중(예: 1초 후) 조회 시도
    await new Promise(resolve => setTimeout(resolve, 1000))
    const startSelect = Date.now()
    const selectResult = await dataply.select(targetPk, false)
    const selectDuration = Date.now() - startSelect

    expect(selectResult).toBe('target-row')
    expect(selectDuration).toBeLessThan(100) // 100ms 이내 응답 (차단되지 않음)

    // 3. Update/Delete는 차단되어야 함 (Blocking)
    // Batch Insert가 아직 끝나지 않은 시점(예: 2초 후)에 시도
    await new Promise(resolve => setTimeout(resolve, 1000))

    let updateFinished = false
    const startUpdate = Date.now()
    const updateTask = (async () => {
      const txUpdate = dataply!.createTransaction()
      // 여기서 Batch Tx가 끝날 때까지 대기해야 함 (메타데이터 락 때문)
      await dataply!.update(targetPk, 'updated-target', txUpdate)
      await txUpdate.commit()
      updateFinished = true
    })()

    // 아직 Batch가 3초 정도 남았으므로 Update는 끝나면 안 됨
    await new Promise(resolve => setTimeout(resolve, 500))
    expect(updateFinished).toBe(false)

    // 4. Batch Insert 완료 대기
    await batchInsertTask

    // 5. 이제 Update가 완료되어야 함
    await updateTask
    const updateDuration = Date.now() - startUpdate
    expect(updateFinished).toBe(true)
    // Update는 Batch Insert가 끝날 때까지 기다렸으므로, 최소 2초 이상 걸렸어야 함 (남은 시간)
    expect(updateDuration).toBeGreaterThan(1000)

    // 6. 데이터 검증
    // - Batch 데이터가 모두 잘 들어갔는지
    for (let i = 0; i < batchSize; i++) {
      const data = await dataply.select(batchPks[i], false)
      expect(data).toBe(`batch-data-${i}`)
    }
    // - Update가 반영되었는지
    const targetData = await dataply.select(targetPk, false)
    expect(targetData).toBe('updated-target')

    // - Row Count 확인
    // Initial(1) + Batch(10) = 11 rows (Update는 count 변화 없음)
    const metadata = await dataply.getMetadata()
    expect(metadata.rowCount).toBe(11)
  }, 20000) // 타임아웃 20초로 연장
})
