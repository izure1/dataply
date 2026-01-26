
import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

describe('Recovery Checksum with Dataply API', () => {
  const TEST_DIR = path.join(__dirname, 'temp_recovery_checksum_test')
  const DB_FILE = path.join(TEST_DIR, 'test_recovery_checksum.db')
  const WAL_FILE = path.join(TEST_DIR, 'test_recovery_checksum.wal')
  const PAGE_SIZE = 4096

  const cleanup = async () => {
    for (let i = 0; i < 5; i++) {
      try {
        if (fs.existsSync(TEST_DIR)) {
          await fs.promises.rm(TEST_DIR, { recursive: true, force: true })
        }
        await fs.promises.mkdir(TEST_DIR)
        break
      } catch (e: any) {
        if ((e.code === 'EBUSY' || e.code === 'ENOTEMPTY') && i < 4) {
          await new Promise(resolve => setTimeout(resolve, 100))
          continue
        }
        throw e
      }
    }
  }

  beforeEach(async () => {
    await cleanup()
  })

  afterEach(async () => {
    // Just remove
    try {
      if (fs.existsSync(TEST_DIR)) {
        await fs.promises.rm(TEST_DIR, { recursive: true, force: true })
      }
    } catch (e) { }
  })

  test('should ignore corrupted pages in WAL during recovery', async () => {
    // 1. Create Dataply and generate WAL
    let dataply = new Dataply(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await dataply.init()

    const data = new Uint8Array([10, 20, 30])
    const pk = await dataply.insert(data)

    const tx = dataply.createTransaction()
    const pk2 = await dataply.insert(new Uint8Array([99]), tx)
    await tx.commit()

    await dataply.close() // Clean start DB.

    const { WALManager } = require('../src/core/WALManager')
    const walManager = new WALManager(WAL_FILE, PAGE_SIZE)
    await walManager.open()

    // Create a page buffer
    const pageId = 1 // First data page
    const pageData = new Uint8Array(PAGE_SIZE)
    const { DataPageManager } = require('../src/core/Page')
    const mgr = new DataPageManager()
    mgr.initial(pageData, 1, pageId, -1, PAGE_SIZE - 24)

    const body = mgr.getBody(pageData)
    body.fill(88) // 'X'

    mgr.updateChecksum(pageData) // Valid checksum

    // Append to WAL
    const pages = new Map()
    pages.set(pageId, pageData)
    await walManager.append(pages) // Valid entry
    await walManager.writeCommitMarker()

    await walManager.close()

    // Corrupt the WAL file
    const walBuf = fs.readFileSync(WAL_FILE)
    // WAL Format: [PageId (4)][Data (PageSize)]
    // Corrupt invalidates the PAGE Checksum (inside Data).
    // Offset = 4 + Header + some index
    walBuf[4 + 50] = 0xFF
    fs.writeFileSync(WAL_FILE, walBuf)

    // 3. Act: Open Dataply (Trigger Recovery)
    const dataply2 = new Dataply(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await dataply2.init() // This runs recovery.

    // 4. Assert
    // Verify using internal PFS to see if page content is overwritten by WAL
    const pfs = (dataply2 as any).api.pfs
    const page = await pfs.get(pageId, dataply2.createTransaction())

    // Check a random byte in body
    const bodyStart = 24 // approximate header size
    expect(page[bodyStart + 10]).not.toBe(88)

    await dataply2.close()

  })

  test('should recover valid pages correctly', async () => {
    // 1. Setup WAL with VALID page
    const s = new Dataply(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await s.init()
    await s.close()

    const { WALManager } = require('../src/core/WALManager')
    const { DataPageManager } = require('../src/core/Page')

    const walManager = new WALManager(WAL_FILE, PAGE_SIZE)
    await walManager.open()

    const pageId = 1
    const pageData = new Uint8Array(PAGE_SIZE)
    const mgr = new DataPageManager()
    mgr.initial(pageData, 1, pageId, -1, PAGE_SIZE - 24)
    const body = mgr.getBody(pageData)
    body.fill(77) // 'M'
    mgr.updateChecksum(pageData)

    const pages = new Map()
    pages.set(pageId, pageData)
    await walManager.append(pages)
    await walManager.writeCommitMarker()
    await walManager.close()

    // 2. Open Dataply
    const dataply = new Dataply(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await dataply.init()

    // 3. Verify
    const pfs = (dataply as any).api.pfs
    const page = await pfs.get(pageId, dataply.createTransaction())

    expect(page[100]).toBe(77) // Should have recovered

    await dataply.close()
  })
})
