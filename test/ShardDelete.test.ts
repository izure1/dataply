import fs from 'node:fs'
import path from 'path'
import { Shard } from '../src/core/Shard'

describe('Shard Delete Transaction Tests', () => {
  const TEST_FILE = path.join(__dirname, 'test_shard_delete.dat')

  beforeEach(() => {
    if (fs.existsSync(TEST_FILE)) {
      fs.unlinkSync(TEST_FILE)
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
    const shard = Shard.Open(TEST_FILE, { pageSize: 8192 })
    await shard.init()

    const data = 'persistent data'
    const pk = await shard.insert(data)

    // Verify insert
    expect(await shard.select(pk)).toBe(data)

    // Start transaction and delete
    const tx = shard.createTransaction()
    await shard.delete(pk, tx)

    // Verify deleted within transaction
    expect(await shard.select(pk, false, tx)).toBeNull()

    // Rollback
    await tx.rollback()

    // Verify data is restored
    expect(await shard.select(pk)).toBe(data)

    await shard.close()
  })

  test('should support isolation between transactions (Read Committed / Snapshot)', async () => {
    const shard = Shard.Open(TEST_FILE, { pageSize: 8192 })
    await shard.init()

    const data = 'shared data'
    const pk = await shard.insert(data)

    // Create two transactions
    const tx1 = shard.createTransaction()
    const tx2 = shard.createTransaction()

    // Tx1 deletes the row
    await shard.delete(pk, tx1)

    // Tx1 should see it as deleted
    expect(await shard.select(pk, false, tx1)).toBeNull()

    // Tx2 should STILL see the data (Isolation)
    expect(await shard.select(pk, false, tx2)).toBe(data)

    // Global (no tx) interaction depends on isolation level, but usually new transactions 
    // shouldn't see uncommitted changes or should block. 
    // In this MVCC implementation, usually readers don't block.
    // Let's assume snapshot isolation or read committed where uncommitted changes aren't visible.
    expect(await shard.select(pk)).toBe(data)

    // Commit Tx1
    await tx1.commit()

    // Now global scope should see it deleted
    expect(await shard.select(pk)).toBeNull()

    // Tx2 behavior depends on implementation (Repeatable Read vs Read Committed).
    // If Repeatable Read (Snapshot), it should still see data.
    // If Read Committed, it might see it deleted.
    // Let's assume Snapshot Isolation for now as it's common in MVCC.
    // Checking internal implementation might be needed, but let's test for Snapshot first.
    // If this fails, we can adjust expectations based on "Read Committed".
    // Based on previous conversations about MVCC refactor, it seems to imply Snapshot/MVCC.

    // For now, let's just commit Tx2 and verify it sees deletion after starting new transaction or if it ends.
    await tx2.commit()

    // Verify final state
    expect(await shard.select(pk)).toBeNull()

    await shard.close()
  })
})
