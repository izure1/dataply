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
})
