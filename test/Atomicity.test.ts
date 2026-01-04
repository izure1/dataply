
import { Dataply } from '../src/core/Dataply'
import { Transaction } from '../src/core/transaction/Transaction'
import fs from 'node:fs'
import path from 'node:path'

describe('Atomicity (Transaction with Dataply API)', () => {
  const testDir = path.join(__dirname, 'temp_atomicity_test')
  const dbPath = path.join(testDir, 'test.db')
  const walPath = path.join(testDir, 'dataply.wal')
  const pageSize = 4096 // Dataply minimum requirement is 4096

  let dataply: Dataply

  beforeAll(async () => {
    if (!fs.existsSync(testDir)) {
      await fs.promises.mkdir(testDir)
    }
  })

  afterAll(async () => {
    if (fs.existsSync(testDir)) {
      await fs.promises.rm(testDir, { recursive: true, force: true })
    }
  })

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) await fs.promises.unlink(dbPath)
    if (fs.existsSync(walPath)) await fs.promises.unlink(walPath)

    dataply = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply.init()
  })

  afterEach(async () => {
    try {
      await dataply.close()
    } catch (e) {
      // Ignore if already closed
    }
    if (fs.existsSync(dbPath)) await fs.promises.unlink(dbPath)
    if (fs.existsSync(walPath)) await fs.promises.unlink(walPath)
  })

  test('should rollback changes (undo memory)', async () => {
    const tx = dataply.createTransaction()

    // 1. Insert Data 1
    const pk1 = await dataply.insert(new Uint8Array([1, 2, 3]), tx)

    // 2. Insert Data 2
    const pk2 = await dataply.insert(new Uint8Array([4, 5, 6]), tx)

    // Verify invisible to other tx (optional, but current isolation might expose them)
    // Checking internal state verified earlier via VFS test.
    // Here we check rollback result.

    // 3. Rollback
    await tx.rollback()

    // 4. Verify rollback
    // Data should not exist
    const read1 = await dataply.select(pk1, true)
    const read2 = await dataply.select(pk2, true)

    expect(read1).toBeNull()
    expect(read2).toBeNull()
  })

  test('should commit changes (persist to Disk)', async () => {
    const tx = dataply.createTransaction()

    const data = new Uint8Array([10, 20, 30])
    const pk = await dataply.insert(data, tx)

    await tx.commit()

    // Close and reopen to verify persistence
    await dataply.close()

    // Reopen
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    const readData = await dataply2.select(pk, true)
    expect(readData).toEqual(data)

    await dataply2.close()
  })

  test('should handle multiple sequential transactions', async () => {
    // Tx 1: Insert A -> Commit
    const tx1 = dataply.createTransaction()
    const pk1 = await dataply.insert('Data 1', tx1)
    await tx1.commit()

    // Tx 2: Insert B -> Commit
    const tx2 = dataply.createTransaction()
    const pk2 = await dataply.insert('Data 2', tx2)
    await tx2.commit()

    // Tx 3: Insert C -> Rollback
    const tx3 = dataply.createTransaction()
    const pk3 = await dataply.insert('Data 3', tx3)
    await tx3.rollback()

    // Verify
    const read1 = await dataply.select(pk1, false)
    const read2 = await dataply.select(pk2, false)
    const read3 = await dataply.select(pk3, false)

    expect(read1).toBe('Data 1')
    expect(read2).toBe('Data 2')
    expect(read3).toBeNull() // Data 3 should be gone
  })

  test('should rollback properly when modifying same row multiple times', async () => {
    // Note: Dataply.update is not deeply implemented to modify row inplace in data page in a complex way yet,
    // but Dataply.update overwrites the row.

    const tx1 = dataply.createTransaction()
    const pk = await dataply.insert(new Uint8Array([10]), tx1)
    await tx1.commit()

    const tx2 = dataply.createTransaction()
    // Modify 1: [10] -> [20]
    await dataply.update(pk, new Uint8Array([20]), tx2)

    // Modify 2: [20] -> [30]
    await dataply.update(pk, new Uint8Array([30]), tx2)

    // Verify current state within tx2?
    // User API `select` takes implicit tx or explicit tx.
    // If we pass tx2, we should see own writes.
    const readInTx = await dataply.select(pk, true, tx2)
    expect(readInTx).toEqual(new Uint8Array([30]))

    // Rollback
    await tx2.rollback()

    // Should return to [10]
    const readAfterRollback = await dataply.select(pk, true)
    expect(readAfterRollback).toEqual(new Uint8Array([10]))
  })
})
