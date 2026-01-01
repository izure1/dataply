
import fs from 'node:fs'
import path from 'path'
import { Shard } from '../src/core/Shard'

describe('Recovery Checksum with Shard API', () => {
  const TEST_DIR = path.join(__dirname, 'temp_recovery_checksum_test')
  const DB_FILE = path.join(TEST_DIR, 'test_recovery_checksum.db')
  const WAL_FILE = path.join(TEST_DIR, 'test_recovery_checksum.wal')
  const PAGE_SIZE = 4096

  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR)
  })

  afterEach(async () => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
  })

  test('should ignore corrupted pages in WAL during recovery', async () => {
    // 1. Create Shard and generate WAL
    let shard = new Shard(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await shard.init()

    const data = new Uint8Array([10, 20, 30])
    const pk = await shard.insert(data)

    const tx = shard.createTransaction()
    const pk2 = await shard.insert(new Uint8Array([99]), tx)
    await tx.commit()

    await shard.close() // Clean start DB.

    const { LogManager } = require('../src/core/LogManager')
    const logManager = new LogManager(WAL_FILE, PAGE_SIZE)
    await logManager.open()

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
    await logManager.append(pages) // Valid entry

    await logManager.close()

    // Corrupt the WAL file
    const walBuf = fs.readFileSync(WAL_FILE)
    // WAL Format: [PageId (4)][Data (PageSize)]
    // Corrupt invalidates the PAGE Checksum (inside Data).
    // Offset = 4 + Header + some index
    walBuf[4 + 50] = 0xFF
    fs.writeFileSync(WAL_FILE, walBuf)

    // 3. Act: Open Shard (Trigger Recovery)
    const shard2 = new Shard(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await shard2.init() // This runs recovery.

    // 4. Assert
    // Verify using internal PFS to see if page content is overwritten by WAL
    const pfs = (shard2 as any).api.pfs
    const page = await pfs.get(pageId, shard2.createTransaction())

    // Check a random byte in body
    const bodyStart = 24 // approximate header size
    expect(page[bodyStart + 10]).not.toBe(88)

    await shard2.close()

  })

  test('should recover valid pages correctly', async () => {
    // 1. Setup WAL with VALID page
    const s = new Shard(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await s.init()
    await s.close()

    const { LogManager } = require('../src/core/LogManager')
    const { DataPageManager } = require('../src/core/Page')

    const logManager = new LogManager(WAL_FILE, PAGE_SIZE)
    await logManager.open()

    const pageId = 1
    const pageData = new Uint8Array(PAGE_SIZE)
    const mgr = new DataPageManager()
    mgr.initial(pageData, 1, pageId, -1, PAGE_SIZE - 24)
    const body = mgr.getBody(pageData)
    body.fill(77) // 'M'
    mgr.updateChecksum(pageData)

    const pages = new Map()
    pages.set(pageId, pageData)
    await logManager.append(pages)
    await logManager.close()

    // 2. Open Shard
    const shard = new Shard(DB_FILE, { pageSize: PAGE_SIZE, wal: WAL_FILE })
    await shard.init()

    // 3. Verify
    const pfs = (shard as any).api.pfs
    const page = await pfs.get(pageId, shard.createTransaction())

    expect(page[100]).toBe(77) // Should have recovered

    await shard.close()
  })
})
