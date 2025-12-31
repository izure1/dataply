import fs from 'node:fs'
import path from 'path'
import { VirtualFileSystem } from '../src/core/VirtualFileSystem'

describe('VirtualFileSystem', () => {
  const TEST_FILE = path.join(__dirname, 'test_vfs.dat')
  let fd: number
  let vfs: VirtualFileSystem

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

    const data1 = Buffer.from('Hello World 1234') // 16 bytes
    await vfs.write(0, data1)

    const read1 = await vfs.read(0, 16)
    // Use toEqual for Buffer comparison
    expect(read1).toEqual(data1)
  })

  test('should handle cross-page writes and reads', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)

    // Page 0: 0-15
    // Page 1: 16-31
    const data = Buffer.from('ABCDEFGHIJKLMNOP') // 16 bytes
    // Write at offset 10 (crosses from page 0 to 1)
    await vfs.write(10, data)

    const read = await vfs.read(10, 16)
    expect(read).toEqual(data)

    // Verify full content logic
    // Bytes 0-9 were not written, so should be 0 (from fresh page alloc/read)
    const header = await vfs.read(0, 10)
    expect(header).toEqual(Buffer.alloc(10))
  })

  test('should sync dirty pages to disk', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)

    const data = Buffer.from('PersistMePlease!')
    await vfs.write(0, data)

    // Without transaction, VFS auto-commits (writes immediately to disk)
    // So data should already be on disk
    const fdRead = fs.openSync(TEST_FILE, 'r')
    const bufBefore = Buffer.alloc(16)
    fs.readSync(fdRead, bufBefore, 0, 16, 0)
    fs.closeSync(fdRead)

    // Auto-commit means disk already has data
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

    const data1 = Buffer.from('AAAA')
    const data2 = Buffer.from('BBBB')

    // Append 1
    await vfs.append(data1)
    // Append 2 (Should append after AAAA, not overwrite)
    await vfs.append(data2)

    // Read 8 bytes
    const read = await vfs.read(0, 8)
    expect(read).toEqual(Buffer.concat([data1, data2]))
  })

  test('append should extend file size and update cache using write internally', async () => {
    const pageSize = 16
    fd = fs.openSync(TEST_FILE, 'w+')
    vfs = new VirtualFileSystem(fd, pageSize)

    // 1. Initial State
    expect(fs.fstatSync(fd).size).toBe(0)

    // 2. Append Data (smaller than page size)
    const data = new Uint8Array([1, 2, 3, 4, 5])
    await vfs.append(data)

    // 3. Verify Cache/Read
    const readBack = await vfs.read(0, 5)
    expect(readBack).toEqual(data)

    // 4. Verify Sync & Disk Size
    await vfs.sync()

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

    // 1. Fill first page
    const page1Data = new Uint8Array(pageSize).fill(1)
    await vfs.append(page1Data)

    // 2. Append spanning to second page
    const page2Data = new Uint8Array(10).fill(2)
    await vfs.append(page2Data)

    // 3. Read back verifying continuity
    const fullData = await vfs.read(0, pageSize + 10)
    expect(fullData.subarray(0, pageSize)).toEqual(page1Data)
    expect(fullData.subarray(pageSize)).toEqual(page2Data)

    // 4. Sync
    await vfs.sync()
    expect(fs.fstatSync(fd).size).toBe(pageSize * 2)
  })
})
