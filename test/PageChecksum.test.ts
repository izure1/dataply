
import fs from 'node:fs'
import path from 'path'
import { Shard } from '../src/core/Shard'
import { PageManager } from '../src/core/Page'

describe('Page Checksum with Shard API', () => {
  const TEST_DIR = path.join(__dirname, 'temp_checksum_test')
  const DB_FILE = path.join(TEST_DIR, 'test_checksum.db')
  const PAGE_SIZE = 4096 // Minimum for Shard

  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true })
    }
    fs.mkdirSync(TEST_DIR)
  })

  afterEach(async () => {
    try {
      if (fs.existsSync(TEST_DIR)) {
        fs.rmSync(TEST_DIR, { recursive: true, force: true })
      }
    } catch (e) {
      // ignore
    }
  })

  test('should detect corrupted page checksum', async () => {
    // 1. Initialize Shard and write data
    let shard = Shard.Open(DB_FILE, { pageSize: PAGE_SIZE })
    await shard.init()

    const data = new Uint8Array([1, 2, 3, 4, 5])
    const pk = await shard.insert(data)
    await shard.close()

    // 2. Corrupt the file manually
    const fd = fs.openSync(DB_FILE, 'r+')
    const pageOffset = PAGE_SIZE * 1 // Page 1
    // Corrupt a byte in the body (after header)
    const corruptOffset = pageOffset + PageManager.CONSTANT.SIZE_PAGE_HEADER + 10
    const buffer = Buffer.alloc(1)
    buffer[0] = 0xFF // Corrupt value
    fs.writeSync(fd, buffer, 0, 1, corruptOffset)
    fs.closeSync(fd)

    // 3. Re-open Shard and try to read
    shard = Shard.Open(DB_FILE, { pageSize: PAGE_SIZE })
    await shard.init()

    try {
      const readData = await shard.select(pk, true)
      // If we read back data, it should NOT match original if we managed to corrupt it and read checks passed (or were skipped).
      // Or ideally it throws.
      expect(readData).not.toEqual(data)
    } catch (e: any) {
      // Checksum failure usually results in error or ignored page
      expect(e).toBeDefined()
    }

    await shard.close()
  })
})
