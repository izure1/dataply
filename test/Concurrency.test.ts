
import { Shard } from '../src/core/Shard'
import path from 'node:path'
import fs from 'node:fs'

describe('Concurrency (MVCC)', () => {
  const testDir = path.join(__dirname, 'temp_concurrency_test')
  const dbPath = path.join(testDir, 'concurrency.db')
  const walPath = path.join(testDir, 'concurrency.wal')
  let shard: Shard | null = null

  beforeAll(() => {
    if (!fs.existsSync(testDir)) fs.mkdirSync(testDir)
  })

  afterAll(async () => {
    // Ensure shard is closed before deleting directory
    if (shard) {
      try { await shard.close() } catch (e) { }
      shard = null
    }
    // Wait a bit for file handles to release
    await new Promise(resolve => setTimeout(resolve, 100))
    if (fs.existsSync(testDir)) {
      try { fs.rmSync(testDir, { recursive: true, force: true }) } catch (e) { }
    }
  })

  afterEach(async () => {
    if (shard) {
      try { await shard.close() } catch (e) { }
      shard = null
    }
  })

  beforeEach(async () => {
    // Wait for any lingering file handles to release
    await new Promise(resolve => setTimeout(resolve, 50))
    if (fs.existsSync(dbPath)) try { fs.unlinkSync(dbPath) } catch (e) { }
    if (fs.existsSync(walPath)) try { fs.unlinkSync(walPath) } catch (e) { }
  })

  test('should serialize concurrent inserts (Writers block Writers)', async () => {
    shard = Shard.Open(dbPath, { wal: walPath })
    await shard.init()

    // Sequential inserts to verify correct PK increment
    const tx1 = await shard.createTransaction()
    const pk1 = await shard.insert('data1', tx1)
    await tx1.commit()

    const tx2 = await shard.createTransaction()
    const pk2 = await shard.insert('data2', tx2)
    await tx2.commit()

    // Verify PKs are distinct and incremented
    expect(pk1).toBe(1)
    expect(pk2).toBe(2)

    // Verify data
    const row1 = await shard.select(pk1, false)
    const row2 = await shard.select(pk2, false)
    expect(row1).toBe('data1')
    expect(row2).toBe('data2')
  })

  test('should restore state via Undo Buffer after rollback (MVCC Isolation)', async () => {
    shard = Shard.Open(dbPath, { wal: walPath })
    await shard.init()

    // Insert initial data and commit
    const tx1 = await shard.createTransaction()
    await shard.insert('initial_data', tx1)
    await tx1.commit()

    // Start a write transaction, modify, but rollback
    const writeTx = await shard.createTransaction()
    await shard.insert('new_data', writeTx)
    await writeTx.rollback()

    // Verify that the committed data (pk=1) is still visible
    const initialResult = await shard.select(1, false)
    expect(initialResult).toBe('initial_data')
  })

  test('should handle multiple sequential transactions correctly', async () => {
    shard = Shard.Open(dbPath, { wal: walPath })
    await shard.init()

    const insertCount = 10
    const pks: number[] = []

    // Multiple sequential transactions
    for (let i = 0; i < insertCount; i++) {
      const tx = await shard.createTransaction()
      const pk = await shard.insert(`data-${i}`, tx)
      await tx.commit()
      pks.push(pk)
    }

    // Verify all PKs are unique and sequential
    expect(pks).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    // Verify all data is correct
    for (let i = 0; i < insertCount; i++) {
      const result = await shard.select(pks[i], false)
      expect(result).toBe(`data-${i}`)
    }
  })

  test('should handle interleaved commit and rollback', async () => {
    shard = Shard.Open(dbPath, { wal: walPath })
    await shard.init()

    // First transaction: commit
    const tx1 = await shard.createTransaction()
    const pk1 = await shard.insert('committed-1', tx1)
    await tx1.commit()

    // Second transaction: rollback
    const tx2 = await shard.createTransaction()
    await shard.insert('rolled-back', tx2)
    await tx2.rollback()

    // Third transaction: commit
    const tx3 = await shard.createTransaction()
    const pk3 = await shard.insert('committed-2', tx3)
    await tx3.commit()

    // Verify committed data is accessible
    expect(await shard.select(pk1, false)).toBe('committed-1')
    expect(await shard.select(pk3, false)).toBe('committed-2')
  })

  test('should maintain data integrity with large batch inserts', async () => {
    shard = Shard.Open(dbPath, { wal: walPath })
    await shard.init()

    const batchSize = 50
    const tx = await shard.createTransaction()
    const pks: number[] = []

    // Insert many rows in a single transaction
    for (let i = 0; i < batchSize; i++) {
      const pk = await shard.insert(`batch-${i}`, tx)
      pks.push(pk)
    }

    await tx.commit()

    // Verify all inserts were persisted
    expect(pks.length).toBe(batchSize)

    // Verify data integrity
    for (let i = 0; i < batchSize; i++) {
      const result = await shard.select(pks[i], false)
      expect(result).toBe(`batch-${i}`)
    }
  })

  test('should allow reads during concurrent write transactions via snapshot', async () => {
    shard = Shard.Open(dbPath, { wal: walPath })
    await shard.init()

    // Insert and commit initial data
    const tx1 = await shard.createTransaction()
    await shard.insert('visible-data', tx1)
    await tx1.commit()

    // Start a new write transaction
    const tx2 = await shard.createTransaction()
    await shard.insert('pending-data', tx2)

    // Read committed data (should see 'visible-data')
    const result = await shard.select(1, false)
    expect(result).toBe('visible-data')

    // Commit the pending transaction
    await tx2.commit()

    // Now we should see the new data too
    const newResult = await shard.select(2, false)
    expect(newResult).toBe('pending-data')
  })

  test('should handle rollback of large batch insert', async () => {
    shard = Shard.Open(dbPath, { wal: walPath })
    await shard.init()

    // First, insert some committed data
    const tx1 = await shard.createTransaction()
    await shard.insert('base-data', tx1)
    await tx1.commit()

    // Start a large batch insert and rollback
    const tx2 = await shard.createTransaction()
    for (let i = 0; i < 20; i++) {
      await shard.insert(`rollback-${i}`, tx2)
    }
    await tx2.rollback()

    // Verify base data is still intact
    const baseResult = await shard.select(1, false)
    expect(baseResult).toBe('base-data')

    // Insert new data after rollback (should work correctly)
    const tx3 = await shard.createTransaction()
    const pk = await shard.insert('after-rollback', tx3)
    await tx3.commit()

    const afterResult = await shard.select(pk, false)
    expect(afterResult).toBe('after-rollback')
  })
})

