import fs from 'node:fs'
import path from 'node:path'
import { PageFileSystem } from '../src/core/PageFileSystem'
import { MetadataPageManager, PageManager, PageManagerFactory } from '../src/core/Page'
import { MetadataPage } from '../src/types'
import { Transaction } from '../src/core/transaction/Transaction'
import { TransactionContext } from '../src/core/transaction/TxContext'
import { LockManager } from '../src/core/transaction/LockManager'

describe('PageFileSystem', () => {
  const TEST_FILE = path.join(__dirname, 'test_pfs.dat')
  const PAGE_SIZE = 1024
  const PAGE_CACHE_CAPACITY = 10000
  let fd: number
  let pfs: PageFileSystem
  let tx: Transaction
  let txContext: TransactionContext
  let lockManager: LockManager

  beforeEach(() => {
    // 파일 생성 및 초기화
    fd = fs.openSync(TEST_FILE, 'w+')
    pfs = new PageFileSystem(fd, PAGE_SIZE, PAGE_CACHE_CAPACITY)
    lockManager = new LockManager()
    txContext = new TransactionContext()
    tx = new Transaction(1, txContext, pfs.vfsInstance, lockManager)
  })

  afterEach(async () => {
    if (pfs) {
      await pfs.close()
    }
    if (fd) {
      try {
        fs.closeSync(fd)
      } catch (e) {
        // 이미 닫힌 경우 무시
      }
    }
    if (fs.existsSync(TEST_FILE)) {
      try {
        await fs.promises.unlink(TEST_FILE)
      } catch (e) {
        // 이미 닫힌 경우 무시
      }
    }
  })

  test('should read page header and body correctly', async () => {
    // 페이지 1에 데이터 쓰기 (pfs 경유)
    const pageIndex = 1
    const buffer = new Uint8Array(PAGE_SIZE)
    const data = 'Hello PageFileSystem'

    // 헤더 영역 제외하고 바디에 데이터 쓰기
    const bodyOffset = PageManager.CONSTANT.SIZE_PAGE_HEADER
    for (let i = 0; i < data.length; i++) {
      buffer[bodyOffset + i] = data.charCodeAt(i)
    }

    // Use pfs.setPage instead of fs.writeSync to respect VFS cache
    await pfs.setPage(pageIndex, buffer, tx)

    const header = await pfs.getHeader(pageIndex, tx)
    expect(header.length).toBe(PageManager.CONSTANT.SIZE_PAGE_HEADER)

    const page = await pfs.get(pageIndex, tx)
    const readString = Buffer.from(page.subarray(bodyOffset, bodyOffset + data.length)).toString()
    expect(readString).toBe(data)
  })

  test('should append new page and update metadata', async () => {
    // 1. Initialize Metadata Page (Page 0)
    const mgr = new PageManagerFactory().getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_METADATA) as MetadataPageManager
    const metadata = mgr.create(PAGE_SIZE, 0) as MetadataPage
    mgr.setMagicString(metadata)
    mgr.setPageCount(metadata, 1) // Page 0 is used by Metadata itself
    mgr.setPageSize(metadata, PAGE_SIZE)
    mgr.setFreePageId(metadata, -1)

    await pfs.setPage(0, metadata, tx)

    // 2. Append new page
    const newPageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA, tx)

    expect(newPageId).toBe(1)

    // 3. Verify Metadata update
    const updatedMetadata = await pfs.getMetadata(tx)
    expect(mgr.getPageCount(updatedMetadata)).toBe(2)

    // 4. Verify New Page
    const newPage = await pfs.get(newPageId, tx)
    const pageManager = new PageManagerFactory().getManager(newPage)
    expect(pageManager.getPageId(newPage)).toBe(1)
    expect(pageManager.getPageType(newPage)).toBe(PageManager.CONSTANT.PAGE_TYPE_DATA)
  })

  describe('writePageContent', () => {
    const initMetadata = async () => {
      const mgr = new PageManagerFactory().getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_METADATA) as MetadataPageManager
      const metadata = mgr.create(PAGE_SIZE, 0) as MetadataPage
      mgr.setMagicString(metadata)
      mgr.setPageCount(metadata, 1)
      mgr.setPageSize(metadata, PAGE_SIZE)
      mgr.setFreePageId(metadata, -1)
      await pfs.setPage(0, metadata, tx)
    }

    test('should write content within a single page', async () => {
      await initMetadata()
      const pageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA, tx)

      const data = Buffer.from('Hello World')
      await pfs.writePageContent(pageId, data, 0, tx)

      const body = await pfs.getBody(pageId, false, tx)
      expect(body.subarray(0, data.length)).toEqual(data)
    })

    test('should overflow to next page when data exceeds capacity', async () => {
      await initMetadata()
      const pageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA, tx)

      const headerSize = PageManager.CONSTANT.SIZE_PAGE_HEADER
      const bodySize = PAGE_SIZE - headerSize

      // Create data larger than one page body
      const data = Buffer.alloc(bodySize + 50)
      data.fill('A')

      await pfs.writePageContent(pageId, data, 0, tx)

      // Verify recursive read
      const fullBody = await pfs.getBody(pageId, true, tx)
      expect(fullBody.length).toBe(data.length)
      expect(fullBody).toEqual(data)

      // Verify page count increased
      const count = await pfs.getPageCount(tx)
      expect(count).toBeGreaterThan(2) // Metadata(0) + Data(1) + Overflow(2)
    })

    test('should write at specific offset', async () => {
      await initMetadata()
      const pageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA, tx)

      const initialData = Buffer.from('Hello World')
      await pfs.writePageContent(pageId, initialData, 0, tx)

      const overwriteData = Buffer.from('Dataply')
      await pfs.writePageContent(pageId, overwriteData, 6, tx) // "Hello Dataply"

      const body = await pfs.getBody(pageId, false, tx)
      const result = Buffer.from(body.subarray(0, 13)).toString()
      expect(result).toBe('Hello Dataply')
    })
  })
})
