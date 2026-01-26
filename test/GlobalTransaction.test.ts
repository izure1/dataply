import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'
import { GlobalTransaction } from '../src/core/transaction/GlobalTransaction'

const TEST_DIR = path.join(__dirname, 'global_tx_test_data')

describe('GlobalTransaction', () => {
  let db1Path: string
  let db2Path: string
  let db1: Dataply
  let db2: Dataply

  beforeEach(async () => {
    if (fs.existsSync(TEST_DIR)) {
      await fs.promises.rm(TEST_DIR, { recursive: true, force: true })
    }
    await fs.promises.mkdir(TEST_DIR)

    db1Path = path.join(TEST_DIR, 'db1.dataply')
    db2Path = path.join(TEST_DIR, 'db2.dataply')

    db1 = new Dataply(db1Path, { wal: path.join(TEST_DIR, 'db1.wal') })
    db2 = new Dataply(db2Path, { wal: path.join(TEST_DIR, 'db2.wal') })

    await db1.init()
    await db2.init()
  })

  afterEach(async () => {
    await db1.close()
    await db2.close()

    if (fs.existsSync(TEST_DIR)) {
      await fs.promises.rm(TEST_DIR, { recursive: true, force: true })
    }
  })

  test('should commit across multiple instances atomically', async () => {
    const tx1 = db1.createTransaction()
    const tx2 = db2.createTransaction()

    const globalTx = new GlobalTransaction()
    globalTx.add(tx1)
    globalTx.add(tx2)

    await db1.insert('data1', tx1)
    await db2.insert('data2', tx2)

    await globalTx.commit()

    // Verify data persistence after restart
    await db1.close()
    await db2.close()

    db1 = new Dataply(db1Path, { wal: path.join(TEST_DIR, 'db1.wal') })
    db2 = new Dataply(db2Path, { wal: path.join(TEST_DIR, 'db2.wal') })
    await db1.init()
    await db2.init()

    const result1 = await db1.select(1, false)
    const result2 = await db2.select(1, false)

    expect(result1).toBe('data1')
    expect(result2).toBe('data2')
  })

  test('should rollback all instances if rollback is called', async () => {
    const tx1 = db1.createTransaction()
    const tx2 = db2.createTransaction()

    const globalTx = new GlobalTransaction()
    globalTx.add(tx1)
    globalTx.add(tx2)

    await db1.insert('data1', tx1)
    await db2.insert('data2', tx2)

    await globalTx.rollback()

    const result1 = await db1.select(1, false)
    const result2 = await db2.select(1, false)

    expect(result1).toBe(null)
    expect(result2).toBe(null)
  })



  test('should succeed even if one instance does not have WAL configured (Atomicity compromised for that instance)', async () => {
    let db3: Dataply | undefined
    let db3Reload: Dataply | undefined

    try {
      // db3 without WAL
      const db3Path = path.join(TEST_DIR, 'db3.dataply')
      db3 = new Dataply(db3Path) // No WAL
      await db3.init()

      const tx1 = db1.createTransaction()
      const tx3 = db3.createTransaction()

      const globalTx = new GlobalTransaction()
      globalTx.add(tx1)
      globalTx.add(tx3)

      await db1.insert('data1', tx1)
      await db3.insert('data3', tx3)

      // Should succeed without error
      await globalTx.commit()

      await db1.close()
      await db3.close()
      db3 = undefined // preventing double close

      // Verify data
      db1 = new Dataply(db1Path, { wal: path.join(TEST_DIR, 'db1.wal') })
      db3Reload = new Dataply(db3Path)
      await db1.init()
      await db3Reload.init()

      expect(await db1.select(1, false)).toBe('data1')
      expect(await db3Reload.select(1, false)).toBe('data3')

    } finally {
      if (db3) await db3.close().catch(() => { })
      if (db3Reload) await db3Reload.close().catch(() => { })
    }
  })
})
