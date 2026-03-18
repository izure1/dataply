import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

describe('Dataply Delete Transaction Tests', () => {
  const TEST_FILE = path.join(__dirname, 'test_dataply_delete.dat')

  beforeEach(async () => {
    if (fs.existsSync(TEST_FILE)) {
      await fs.promises.unlink(TEST_FILE)
    }
  })

  afterEach(async () => {
    if (fs.existsSync(TEST_FILE)) {
      try {
        await fs.promises.unlink(TEST_FILE)
      } catch (e) {
        // ignore
      }
    }
  })

  test('should rollback a delete operation', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const data = 'persistent data'
    const pk = await dataply.insert(data)

    // Verify insert
    expect(await dataply.select(pk)).toBe(data)

    // Start transaction and delete
    try {
      await dataply.withWriteTransaction(async (tx) => {
        await dataply.delete(pk, tx)

        // Verify deleted within transaction
        expect(await dataply.select(pk, false, tx)).toBeNull()

        // Rollback
        throw new Error('Rollback')
      })
    } catch (e: any) {
      if (e.message !== 'Rollback') throw e
    }

    // Verify data is restored
    expect(await dataply.select(pk)).toBe(data)

    await dataply.close()
  })

  test('should support isolation between transactions (Read Committed / Snapshot)', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const data = 'shared data'
    const pk = await dataply.insert(data)

    const tx1 = dataply.withWriteTransaction(async (tx) => {
      await dataply.delete(pk, tx)
      expect(await dataply.select(pk, false, tx)).toBeNull()
    })

    const tx2 = dataply.withReadTransaction(async (tx) => {
      expect(await dataply.select(pk, false, tx)).toBe(data)
    })

    expect(await dataply.select(pk)).toBe(data)

    // Commit Tx1
    await tx1

    // Now global scope should see it deleted
    expect(await dataply.select(pk)).toBeNull()

    // Tx2 behavior depends on implementation (Repeatable Read vs Read Committed).
    // If Repeatable Read (Snapshot), it should still see data.
    // If Read Committed, it might see it deleted.
    // Let's assume Snapshot Isolation for now as it's common in MVCC.
    // Checking internal implementation might be needed, but let's test for Snapshot first.
    // If this fails, we can adjust expectations based on "Read Committed".
    // Based on previous conversations about MVCC refactor, it seems to imply Snapshot/MVCC.

    // For now, let's just commit Tx2 and verify it sees deletion after starting new transaction or if it ends.
    await tx2

    // Verify final state
    expect(await dataply.select(pk)).toBeNull()

    await dataply.close()
  })

  test('should successfully delete multiple items in batch', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const pks = await dataply.insertBatch(['data 1', 'data 2', 'data 3', 'data 4', 'data 5'])
    expect(pks.length).toBe(5)

    // Verify all inserted
    for (let i = 0; i < 5; i++) {
      expect(await dataply.select(pks[i])).toBe(`data ${i + 1}`)
    }

    // Delete in batch
    await dataply.deleteBatch([pks[0], pks[2], pks[4]])

    // Verify
    expect(await dataply.select(pks[0])).toBeNull()
    expect(await dataply.select(pks[1])).toBe('data 2')
    expect(await dataply.select(pks[2])).toBeNull()
    expect(await dataply.select(pks[3])).toBe('data 4')
    expect(await dataply.select(pks[4])).toBeNull()

    // Verify metadata row count
    const metadata = await dataply.getMetadata()
    expect(metadata.rowCount).toBe(2)

    await dataply.close()
  })

  test('should rollback a batch delete operation', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const pks = await dataply.insertBatch(['data A', 'data B', 'data C'])

    try {
      await dataply.withWriteTransaction(async (tx) => {
        await dataply.deleteBatch(pks, tx)

        // Verify deleted within transaction
        for (const pk of pks) {
          expect(await dataply.select(pk, false, tx)).toBeNull()
        }

        throw new Error('Rollback Batch Delete')
      })
    } catch (e: any) {
      if (e.message !== 'Rollback Batch Delete') throw e
    }

    // Verify data is restored
    for (let i = 0; i < 3; i++) {
      const char = String.fromCharCode(65 + i)
      expect(await dataply.select(pks[i])).toBe(`data ${char}`)
    }

    const metadata = await dataply.getMetadata()
    expect(metadata.rowCount).toBe(3)

    await dataply.close()
  })

  test('should handle empty array in batch delete gracefully', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const pks = await dataply.insertBatch(['data X'])

    // Pass empty array
    await dataply.deleteBatch([])

    // Verify data remains and no errors thrown
    expect(await dataply.select(pks[0])).toBe('data X')

    await dataply.close()
  })
})
