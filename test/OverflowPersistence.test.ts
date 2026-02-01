import fs from 'node:fs'
import path from 'node:path'
import { DataplyAPI } from '../src/core/DataplyAPI'

const DB_PATH = path.join(__dirname, 'overflow_persistence_test.db')

describe('Overflow Persistence Test', () => {
  beforeEach(() => {
    if (fs.existsSync(DB_PATH)) {
      fs.unlinkSync(DB_PATH)
    }
  })

  afterEach(() => {
    if (fs.existsSync(DB_PATH)) {
      fs.unlinkSync(DB_PATH)
    }
  })

  test('should persist overflow data after db close and open', async () => {
    // 1. Initialize DB and Insert Data
    let db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const data = "overflow data persistence test"
    let pk: number

    const tx1 = db.createTransaction()
    try {
      // Force insert as overflow
      pk = await db.insertAsOverflow(data, true, tx1)
      await tx1.commit()
    } catch (e) {
      await tx1.rollback()
      throw e
    }

    await db.close()

    // 2. Re-open DB and Select Data
    db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const tx2 = db.createTransaction()
    try {
      const selected = await db.select(pk, false, tx2)
      expect(selected).toBe(data)

      // Row count verification
      const rowTableEngine = (db as any).rowTableEngine
      const count = await rowTableEngine.getRowCount(tx2)
      expect(count).toBe(1)
    } finally {
      await tx2.commit()
    }

    await db.close()
  })

  test('should persist empty string as overflow data after db close and open', async () => {
    // 1. Initialize DB and Insert Data
    let db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const data = ""
    let pk: number

    const tx1 = db.createTransaction()
    try {
      // Force insert as overflow
      pk = await db.insertAsOverflow(data, true, tx1)
      await tx1.commit()
    } catch (e) {
      await tx1.rollback()
      throw e
    }

    await db.close()

    // 2. Re-open DB and Select Data
    db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const tx2 = db.createTransaction()
    try {
      const selected = await db.select(pk, false, tx2)
      expect(selected).toBe(data)

      // Row count verification
      const rowTableEngine = (db as any).rowTableEngine
      const count = await rowTableEngine.getRowCount(tx2)
      expect(count).toBe(1)
    } finally {
      await tx2.commit()
    }

    await db.close()
  })
})
