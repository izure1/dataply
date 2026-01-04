import fs from 'node:fs'
import path from 'node:path'
import { VirtualFileSystem } from '../src/core/VirtualFileSystem'
import { Transaction } from '../src/core/transaction/Transaction'
import { LockManager } from '../src/core/transaction/LockManager'

describe('VirtualFileSystem', () => {
  const TEST_FILE = path.join(__dirname, 'test_vfs.dat')
  let fd: number
  let vfs: VirtualFileSystem
  let lockManager: LockManager

  afterEach(async () => {
    if (vfs) {
      await vfs.close()
    }
    // vfs.close()는 sync만 수행하므로 파일 핸들을 닫아줘야 함
    if (fd) {
      try {
        fs.closeSync(fd)
      } catch (e) {
        // 이미 닫힌 경우 무시
      }
    }
    if (fs.existsSync(TEST_FILE)) {
      try {
        fs.unlinkSync(TEST_FILE)
      } catch (e) {
        // 이미 닫힌 경우 무시 (혹은 윈도우에서 파일 잠금 이슈 등)
      }
    }
  })

  test('should write and read back data using cache', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)
    lockManager = new LockManager()
    const tx = new Transaction(1, vfs, lockManager)

    const data1 = Buffer.from('Hello World 1234') // 16 bytes
    await vfs.write(0, data1, tx)

    const read1 = await vfs.read(0, 16, tx)
    // Use toEqual for Buffer comparison
    expect(read1).toEqual(data1)
    await tx.commit()
  })

  test('should handle cross-page writes and reads', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)
    lockManager = new LockManager()
    const tx = new Transaction(1, vfs, lockManager)

    // Page 0: 0-15
    // Page 1: 16-31
    const data = Buffer.from('ABCDEFGHIJKLMNOP') // 16 bytes
    // Write at offset 10 (crosses from page 0 to 1)
    await vfs.write(10, data, tx)

    const read = await vfs.read(10, 16, tx)
    expect(read).toEqual(data)

    // Verify full content logic
    // Bytes 0-9 were not written, so should be 0 (from fresh page alloc/read)
    const header = await vfs.read(0, 10, tx)
    expect(header).toEqual(Buffer.alloc(10))
    await tx.commit()
  })

  test('should sync dirty pages to disk', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)
    lockManager = new LockManager()
    const tx = new Transaction(1, vfs, lockManager)

    const data = Buffer.from('PersistMePlease!')
    await vfs.write(0, data, tx)

    // With transaction, data is NOT immediately on disk until commit/sync
    // But we want to verify it gets there eventually.
    // Let's commit to sync to disk.
    await tx.commit()

    const fdRead = fs.openSync(TEST_FILE, 'r')
    const bufBefore = Buffer.alloc(16)
    fs.readSync(fdRead, bufBefore, 0, 16, 0)
    fs.closeSync(fdRead)

    expect(bufBefore).toEqual(data)

    await vfs.sync()

    const fdAfter = fs.openSync(TEST_FILE, 'r')
    const bufAfter = Buffer.alloc(16)
    fs.readSync(fdAfter, bufAfter, 0, 16, 0)
    fs.closeSync(fdAfter)

    expect(bufAfter).toEqual(data)
  })

  test('should handle consecutive appends correctly (tracking file size)', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)
    lockManager = new LockManager()
    const tx = new Transaction(1, vfs, lockManager)

    const data1 = Buffer.from('AAAA')
    const data2 = Buffer.from('BBBB')

    // Append 1
    await vfs.append(data1, tx)
    // Append 2 (Should append after AAAA, not overwrite)
    await vfs.append(data2, tx)

    // Read 8 bytes
    const read = await vfs.read(0, 8, tx)
    expect(read).toEqual(Buffer.concat([data1, data2]))
    await tx.commit()
  })

  test('append should extend file size and update cache using write internally', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)
    lockManager = new LockManager()
    const tx = new Transaction(1, vfs, lockManager)

    // 1. Initial State
    expect(fs.fstatSync(fd).size).toBe(0)

    // 2. Append Data (smaller than page size)
    const data = new Uint8Array([1, 2, 3, 4, 5])
    await vfs.append(data, tx)

    // 3. Verify Cache/Read
    const readBack = await vfs.read(0, 5, tx)
    expect(readBack).toEqual(data)

    // 4. Verify Sync & Disk Size
    await tx.commit() // Commit triggers write to disk

    const stats = fs.fstatSync(fd)
    // Sync calls _writeAsync with pageSize. So it will likely write full page bytes (filling with zeros).
    expect(stats.size).toBe(pageSize)

    // Verify content on disk
    const diskBuffer = Buffer.alloc(pageSize)
    fs.readSync(fd, diskBuffer, 0, pageSize, 0)
    expect(diskBuffer.subarray(0, 5)).toEqual(Buffer.from(data))
  })

  test('append across page boundaries', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)
    lockManager = new LockManager()
    const tx = new Transaction(1, vfs, lockManager)

    // 1. Fill first page
    const page1Data = new Uint8Array(pageSize).fill(1)
    await vfs.append(page1Data, tx)

    // 2. Append spanning to second page
    const page2Data = new Uint8Array(10).fill(2)
    await vfs.append(page2Data, tx)

    // 3. Read back verifying continuity
    const fullData = await vfs.read(0, pageSize + 10, tx)
    expect(fullData.subarray(0, pageSize)).toEqual(page1Data)
    expect(fullData.subarray(pageSize)).toEqual(page2Data)

    // 4. Sync
    await tx.commit()
    expect(fs.fstatSync(fd).size).toBe(pageSize * 2)
  })
})
