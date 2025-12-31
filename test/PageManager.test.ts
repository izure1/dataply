import { PageManagerFactory, DataPageManager } from '../src/core/Page'

describe('PageManager', () => {
  let factory: PageManagerFactory
  let manager: DataPageManager
  let buffer: Uint8Array

  beforeEach(() => {
    factory = new PageManagerFactory()
    manager = new DataPageManager()
    buffer = new Uint8Array(1024) // Only need slightly more than 16 bytes
  })

  test('should set and get PageType (1 byte)', () => {
    manager.setPageType(buffer, 5)
    expect(manager.getPageType(buffer)).toBe(5)
  })

  test('should set and get PageId (4 bytes)', () => {
    const pageId = 0x12345678
    manager.setPageId(buffer, pageId)
    expect(manager.getPageId(buffer)).toBe(pageId)

    const chunk = buffer.subarray(
      DataPageManager.CONSTANT.OFFSET_PAGE_ID,
      DataPageManager.CONSTANT.OFFSET_PAGE_ID + DataPageManager.CONSTANT.SIZE_PAGE_ID
    )
    expect(chunk[0]).toBe(0x78)
    expect(chunk[1]).toBe(0x56)
    expect(chunk[2]).toBe(0x34)
    expect(chunk[3]).toBe(0x12)
  })

  test('should set and get NextPageId (4 bytes)', () => {
    const nextId = 0xAABBCCDD
    manager.setNextPageId(buffer, nextId)
    const read = manager.getNextPageId(buffer)
    expect(read).toBe(nextId)

    const chunk = buffer.subarray(
      DataPageManager.CONSTANT.OFFSET_NEXT_PAGE_ID,
      DataPageManager.CONSTANT.OFFSET_NEXT_PAGE_ID + DataPageManager.CONSTANT.SIZE_NEXT_PAGE_ID
    )
    expect(chunk[0]).toBe(0xDD)
    expect(chunk[1]).toBe(0xCC)
    expect(chunk[2]).toBe(0xBB)
    expect(chunk[3]).toBe(0xAA)
  })

  test('should set and get InsertedRowCount (4 bytes)', () => {
    const count = 1000
    manager.setInsertedRowCount(buffer, count)
    expect(manager.getInsertedRowCount(buffer)).toBe(count)
  })

  test('should set and get RemainingCapacity (4 bytes)', () => {
    const cap = 4096
    manager.setRemainingCapacity(buffer, cap)
    expect(manager.getRemainingCapacity(buffer)).toBe(cap)
  })

  test('should work with independent fields in same buffer', () => {
    manager.setPageType(buffer, 1)
    manager.setPageId(buffer, 10)
    manager.setNextPageId(buffer, 20)

    expect(manager.getPageType(buffer)).toBe(1)
    expect(manager.getPageId(buffer)).toBe(10)
    expect(manager.getNextPageId(buffer)).toBe(20)
  })

  test('should initialize page correctly', () => {
    manager.initial(
      buffer,
      DataPageManager.CONSTANT.PAGE_TYPE_DATA,
      100,
      200,
      1024
    )

    expect(manager.getPageType(buffer)).toBe(DataPageManager.CONSTANT.PAGE_TYPE_DATA)
    expect(manager.getPageId(buffer)).toBe(100)
    expect(manager.getNextPageId(buffer)).toBe(200)
    expect(manager.getRemainingCapacity(buffer)).toBe(1024)
  })

  describe('PageManagerFactory', () => {
    test('should identify EmptyPage', () => {
      manager.setPageType(buffer, DataPageManager.CONSTANT.PAGE_TYPE_EMPTY)
      expect(factory.isEmptyPage(buffer)).not.toBe(false)
      expect(factory.isDataPage(buffer)).toBe(false)
    })

    test('should identify DataPage', () => {
      manager.setPageType(buffer, DataPageManager.CONSTANT.PAGE_TYPE_DATA)
      expect(factory.isDataPage(buffer)).not.toBe(false)
      expect(factory.isEmptyPage(buffer)).toBe(false)
    })

    test('should identify IndexPage', () => {
      manager.setPageType(buffer, DataPageManager.CONSTANT.PAGE_TYPE_INDEX)
      expect(factory.isIndexPage(buffer)).not.toBe(false)
    })

    test('should identify MetadataPage', () => {
      manager.setPageType(buffer, DataPageManager.CONSTANT.PAGE_TYPE_METADATA)
      expect(factory.isMetadataPage(buffer)).not.toBe(false)
    })

    test('should identify BitmapPage', () => {
      manager.setPageType(buffer, DataPageManager.CONSTANT.PAGE_TYPE_BITMAP)
      expect(factory.isBitmapPage(buffer)).not.toBe(false)
    })

    test('should identify OverflowPage', () => {
      manager.setPageType(buffer, DataPageManager.CONSTANT.PAGE_TYPE_OVERFLOW)
      expect(factory.isOverflowPage(buffer)).not.toBe(false)
    })
  })
})
