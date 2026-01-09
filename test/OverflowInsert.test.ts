import fs from 'node:fs'
import path from 'node:path'
import { DataplyAPI } from '../src/core/DataplyAPI'
import { Row } from '../src/core/Row'
import { KeyManager } from '../src/core/KeyManager'
import { DataPageManager } from '../src/core/Page'
import { type RowTableEngine } from '../src/core/RowTableEngine'
import { type PageFileSystem } from '../src/core/PageFileSystem'

const DB_PATH = path.join(__dirname, 'overflow_test.db')

describe('Overflow Insert Test', () => {
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

  test('should force insert as overflow even for small data', async () => {
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const data = "small data"
    const pk = await db.insertAsOverflow(data)

    // 1. Functional Verification: Select should return the data correctly
    const selected = await db.select(pk)
    expect(selected).toBe(data)

    // 2. Whitebox Verification: Check internal structure
    // We need to access private/protected members, so casting to any
    const apiAny = db as any
    const engine = apiAny.rowTableEngine as RowTableEngine
    const pfs = apiAny.pfs as PageFileSystem

    // Create a transaction for reading
    // accessing protected createTransaction indirectly or just public wrapper if possible.
    // DataplyAPI has public createTransaction()
    const tx = db.createTransaction()

    try {
      // Get RID (Need to access private method of engine or use bptree directly if accessible)
      // engine.getRidByPK is private.
      const rid = await engine['getRidByPK'](pk, tx)
      expect(rid).not.toBeNull()

      // Decode RID to PageID and SlotIndex
      const keyManager = new KeyManager()
      const ridBuffer = new Uint8Array(6)
      keyManager.setBufferFromKey(rid as number, ridBuffer)

      const pageId = keyManager.getPageId(ridBuffer)
      const slotIndex = keyManager.getSlotIndex(ridBuffer)

      // Get Page
      const page = await pfs.get(pageId, tx)

      // Get Row
      const dataPageManager = new DataPageManager()
      const rowData = dataPageManager.getRow(page as any, slotIndex)

      const rowManager = new Row()

      // Verify Overflow Flag is set
      expect(rowManager.getOverflowFlag(rowData)).toBe(true)

      // Verify Body Size is 4 bytes (Page ID size)
      expect(rowManager.getBodySize(rowData)).toBe(4)

    } finally {
      await tx.commit() // Read-only tx commit
    }

    await db.close()
  })
})
