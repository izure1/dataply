
import { WALManager } from '../src/core/WALManager'
import fs from 'node:fs'
import path from 'node:path'

describe('WALManager', () => {
  const testDir = path.join(__dirname, 'temp_walmanager_test')
  const walPath = path.join(testDir, 'test.wal')
  const pageSize = 1024 // 1KB for testing

  beforeAll(async () => {
    if (!fs.existsSync(testDir)) {
      await fs.promises.mkdir(testDir)
    }
  })

  afterAll(async () => {
    if (fs.existsSync(testDir)) {
      await fs.promises.rm(testDir, { recursive: true, force: true })
    }
  })

  afterEach(async () => {
    if (fs.existsSync(walPath)) {
      await fs.promises.unlink(walPath)
    }
  })

  test('should append pages to log', async () => {
    const walManager = new WALManager(walPath, pageSize)
    walManager.open()

    const page1Data = new Uint8Array(pageSize).fill(1)
    const page2Data = new Uint8Array(pageSize).fill(2)

    const pages = new Map<number, Uint8Array>()
    pages.set(1, page1Data)
    pages.set(2, page2Data)

    await walManager.append(pages)
    await walManager.writeCommitMarker()
    walManager.close()

    // Verify file size: (4 + 1024) * 2 + (4 + 1024) = 1028 * 3 = 3084 bytes
    const stats = fs.statSync(walPath)
    expect(stats.size).toBe(3084)
  })

  test('should read all pages from log', async () => {
    const walManager = new WALManager(walPath, pageSize)
    walManager.open()

    const page1Data = new Uint8Array(pageSize).fill(1)
    const page2Data = new Uint8Array(pageSize).fill(2)
    // Update page 1 later
    const page1DataUpdated = new Uint8Array(pageSize).fill(3)


    // First transaction
    const pages1 = new Map<number, Uint8Array>()
    pages1.set(1, page1Data)
    pages1.set(2, page2Data)
    await walManager.append(pages1)
    await walManager.writeCommitMarker()

    // Second transaction (update page 1)
    const pages2 = new Map<number, Uint8Array>()
    pages2.set(1, page1DataUpdated)
    await walManager.append(pages2)
    await walManager.writeCommitMarker()

    walManager.close()

    // Re-open and read
    const walManagerReader = new WALManager(walPath, pageSize)
    const restoredPages = walManagerReader.readAllSync()
    walManagerReader.close()

    expect(restoredPages.size).toBe(2)
    expect(restoredPages.get(1)).toEqual(page1DataUpdated) // Should be latest
    expect(restoredPages.get(2)).toEqual(page2Data)
  })

  test('should clear log', async () => {
    const walManager = new WALManager(walPath, pageSize)
    walManager.open()

    const pages = new Map<number, Uint8Array>()
    pages.set(1, new Uint8Array(pageSize).fill(1))
    await walManager.append(pages)
    await walManager.writeCommitMarker()

    await walManager.clear()

    // Check file size
    const stat = fs.statSync(walPath)
    expect(stat.size).toBe(0)

    const restored = walManager.readAllSync()
    expect(restored.size).toBe(0)

    walManager.close()
  })

  test('should persist data after closing and reopening', async () => {
    const walManager = new WALManager(walPath, pageSize)
    walManager.open()
    const pages = new Map<number, Uint8Array>()
    const page1Data = new Uint8Array(pageSize).fill(10)
    pages.set(1, page1Data)

    await walManager.append(pages)
    await walManager.writeCommitMarker()
    walManager.close()

    // Reopen
    const walManager2 = new WALManager(walPath, pageSize)
    walManager2.open()

    const restored = walManager2.readAllSync()
    expect(restored.size).toBe(1)
    expect(restored.get(1)).toEqual(page1Data)

    walManager2.close()
  })

  test('should handle large number of pages', async () => {
    const walManager = new WALManager(walPath, pageSize)
    walManager.open()
    const pages = new Map<number, Uint8Array>()
    const count = 100
    for (let i = 0; i < count; i++) {
      pages.set(i, new Uint8Array(pageSize).fill(i % 255))
    }

    await walManager.append(pages)
    await walManager.writeCommitMarker()

    const restored = walManager.readAllSync()
    walManager.close()

    expect(restored.size).toBe(count)
    for (let i = 0; i < count; i++) {
      expect(restored.get(i)).toEqual(new Uint8Array(pageSize).fill(i % 255))
    }
  })
})

