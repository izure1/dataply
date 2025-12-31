
import { LogManager } from '../src/core/LogManager'
import fs from 'node:fs'
import path from 'node:path'

describe('LogManager', () => {
  const testDir = path.join(__dirname, 'temp_logmanager_test')
  const walPath = path.join(testDir, 'test.wal')
  const pageSize = 1024 // 1KB for testing

  beforeAll(() => {
    if (!fs.existsSync(testDir)) {
      fs.mkdirSync(testDir)
    }
  })

  afterAll(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true })
    }
  })

  afterEach(() => {
    if (fs.existsSync(walPath)) {
      fs.unlinkSync(walPath)
    }
  })

  test('should append pages to log', async () => {
    const logManager = new LogManager(walPath, pageSize)
    logManager.open()

    const page1Data = new Uint8Array(pageSize).fill(1)
    const page2Data = new Uint8Array(pageSize).fill(2)

    const pages = new Map<number, Uint8Array>()
    pages.set(1, page1Data)
    pages.set(2, page2Data)

    await logManager.append(pages)
    logManager.close()

    // Verify file size: (4 + 1024) * 2 = 2056 bytes
    const stats = fs.statSync(walPath)
    expect(stats.size).toBe(2056)
  })

  test('should read all pages from log', async () => {
    const logManager = new LogManager(walPath, pageSize)
    logManager.open()

    const page1Data = new Uint8Array(pageSize).fill(1)
    const page2Data = new Uint8Array(pageSize).fill(2)
    // Update page 1 later
    const page1DataUpdated = new Uint8Array(pageSize).fill(3)

    // First transaction
    const pages1 = new Map<number, Uint8Array>()
    pages1.set(1, page1Data)
    pages1.set(2, page2Data)
    await logManager.append(pages1)

    // Second transaction (update page 1)
    const pages2 = new Map<number, Uint8Array>()
    pages2.set(1, page1DataUpdated)
    await logManager.append(pages2)

    logManager.close()

    // Re-open and read
    const logManagerReader = new LogManager(walPath, pageSize)
    const restoredPages = logManagerReader.readAllSync()
    logManagerReader.close()

    expect(restoredPages.size).toBe(2)
    expect(restoredPages.get(1)).toEqual(page1DataUpdated) // Should be latest
    expect(restoredPages.get(2)).toEqual(page2Data)
  })

  test('should clear log', async () => {
    const logManager = new LogManager(walPath, pageSize)
    logManager.open()

    const pages = new Map<number, Uint8Array>()
    pages.set(1, new Uint8Array(pageSize).fill(1))
    await logManager.append(pages)

    await logManager.clear()

    // Check file size
    const stat = fs.statSync(walPath)
    expect(stat.size).toBe(0)

    const restored = logManager.readAllSync()
    expect(restored.size).toBe(0)

    logManager.close()
  })

  test('should persist data after closing and reopening', async () => {
    const logManager = new LogManager(walPath, pageSize)
    logManager.open()
    const pages = new Map<number, Uint8Array>()
    const page1Data = new Uint8Array(pageSize).fill(10)
    pages.set(1, page1Data)

    await logManager.append(pages)
    logManager.close()

    // Reopen
    const logManager2 = new LogManager(walPath, pageSize)
    logManager2.open()

    const restored = logManager2.readAllSync()
    expect(restored.size).toBe(1)
    expect(restored.get(1)).toEqual(page1Data)

    logManager2.close()
  })

  test('should handle large number of pages', async () => {
    const logManager = new LogManager(walPath, pageSize)
    logManager.open()
    const pages = new Map<number, Uint8Array>()
    const count = 100
    for (let i = 0; i < count; i++) {
      pages.set(i, new Uint8Array(pageSize).fill(i % 255))
    }

    await logManager.append(pages)

    const restored = logManager.readAllSync()
    logManager.close()

    expect(restored.size).toBe(count)
    for (let i = 0; i < count; i++) {
      expect(restored.get(i)).toEqual(new Uint8Array(pageSize).fill(i % 255))
    }
  })
})
