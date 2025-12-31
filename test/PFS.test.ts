import fs from 'node:fs'
import path from 'path'
import { PageFileSystem } from '../src/core/PageFileSystem'
import { MetadataPageManager, PageManager, PageManagerFactory } from '../src/core/Page'
import { MetadataPage } from '../src/types'


describe('PageFileSystem', () => {
  const TEST_FILE = path.join(__dirname, 'test_pfs.dat')
  const PAGE_SIZE = 1024
  let fd: number
  let pfs: PageFileSystem


  beforeEach(() => {
    // 파일 생성 및 초기화
    fd = fs.openSync(TEST_FILE, 'w+')
    pfs = new PageFileSystem(fd, PAGE_SIZE)
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
        fs.unlinkSync(TEST_FILE)
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
    await pfs.setPage(pageIndex, buffer)

    const header = await pfs.getHeader(pageIndex)
    expect(header.length).toBe(PageManager.CONSTANT.SIZE_PAGE_HEADER)

    const page = await pfs.get(pageIndex)
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

    await pfs.setPage(0, metadata)

    // 2. Append new page
    const newPageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA)

    expect(newPageId).toBe(1)

    // 3. Verify Metadata update
    const updatedMetadata = await pfs.getMetadata()
    expect(mgr.getPageCount(updatedMetadata)).toBe(2)

    // 4. Verify New Page
    const newPage = await pfs.get(newPageId)
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
      await pfs.setPage(0, metadata)
    }

    test('should write content within a single page', async () => {
      await initMetadata()
      const pageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA)

      const data = Buffer.from('Hello World')
      await pfs.writePageContent(pageId, data)

      const body = await pfs.getBody(pageId)
      expect(body.subarray(0, data.length)).toEqual(data)
    })

    test('should overflow to next page when data exceeds capacity', async () => {
      await initMetadata()
      const pageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA)

      const headerSize = PageManager.CONSTANT.SIZE_PAGE_HEADER
      const bodySize = PAGE_SIZE - headerSize

      // Create data larger than one page body
      const data = Buffer.alloc(bodySize + 50)
      data.fill('A')

      await pfs.writePageContent(pageId, data)

      // Verify recursive read
      const fullBody = await pfs.getBody(pageId, true)
      expect(fullBody.length).toBe(data.length)
      expect(fullBody).toEqual(data)

      // Verify page count increased
      const count = await pfs.getPageCount()
      expect(count).toBeGreaterThan(2) // Metadata(0) + Data(1) + Overflow(2)
    })

    test('should write at specific offset', async () => {
      await initMetadata()
      const pageId = await pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_DATA)

      const initialData = Buffer.from('Hello World')
      await pfs.writePageContent(pageId, initialData)

      const overwriteData = Buffer.from('Shard')
      await pfs.writePageContent(pageId, overwriteData, 6) // "Hello Shard"

      const body = await pfs.getBody(pageId)
      const result = Buffer.from(body.subarray(0, 11)).toString()
      expect(result).toBe('Hello Shard')
    })
  })
})
