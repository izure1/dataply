import { DataPage, IndexPage, BitmapPage, OverflowPage, MetadataPage, EmptyPage, UnknownPage } from '../types'
import { bytesToNumber, numberToBytes, crc32 } from '../utils'
import { Row } from './Row'

/**
 * Manages common methods for each type of page.
 */
export abstract class PageManager {
  static readonly CONSTANT = {
    PAGE_TYPE_UNKNOWN: 0,
    PAGE_TYPE_EMPTY: 1,
    PAGE_TYPE_METADATA: 2,
    PAGE_TYPE_BITMAP: 3,
    PAGE_TYPE_INDEX: 4,
    PAGE_TYPE_DATA: 5,
    PAGE_TYPE_OVERFLOW: 6,
    SIZE_PAGE_HEADER: 100, // 페이지 헤더 크기. 페이지의 정보를 담고 있으며, 추후 여분을 위해 100으로 설정했습니다.
    SIZE_PAGE_TYPE: 1,
    SIZE_PAGE_ID: 4,
    SIZE_NEXT_PAGE_ID: 4,
    SIZE_INSERTED_ROW_COUNT: 4,
    SIZE_REMAINING_CAPACITY: 4,
    SIZE_SLOT_OFFSET: 2,
    SIZE_CHECKSUM: 4,
    OFFSET_PAGE_TYPE: 0,
    OFFSET_PAGE_ID: 1,
    OFFSET_NEXT_PAGE_ID: 5,
    OFFSET_INSERTED_ROW_COUNT: 9,
    OFFSET_REMAINING_CAPACITY: 13,
    OFFSET_CHECKSUM: 17,
  } as const

  /**
   * Returns the page type.
   * @param page Page data
   * @returns Page type
   */
  static GetPageType(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_PAGE_TYPE,
      PageManager.CONSTANT.SIZE_PAGE_TYPE
    )
  }

  /**
   * Returns the page type.
   * @param page Page data
   * @returns Page type
   */
  getPageType(page: Uint8Array): number {
    return PageManager.GetPageType(page)
  }

  /**
   * Returns the page ID.
   * @param page Page data
   * @returns Page ID
   */
  getPageId(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_PAGE_ID,
      PageManager.CONSTANT.SIZE_PAGE_ID
    )
  }

  /**
   * Returns the ID of the next connected page.
   * @param page Page data
   * @returns Next connected page ID
   */
  getNextPageId(page: Uint8Array): number {
    const id = bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_NEXT_PAGE_ID,
      PageManager.CONSTANT.SIZE_NEXT_PAGE_ID
    )
    return id === 0xFFFFFFFF ? -1 : id
  }

  /**
   * Returns the remaining capacity of the page.
   * @param page Page data
   * @returns Remaining capacity
   */
  getRemainingCapacity(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_REMAINING_CAPACITY,
      PageManager.CONSTANT.SIZE_REMAINING_CAPACITY
    )
  }

  /**
   * Returns the checksum of the page.
   * @param page Page data
   * @returns Checksum
   */
  getChecksum(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_CHECKSUM,
      PageManager.CONSTANT.SIZE_CHECKSUM
    )
  }

  /**
   * Sets the page type.
   * @param page Page data
   * @param pageType Page type
   */
  setPageType(page: Uint8Array, pageType: number): void {
    numberToBytes(
      pageType,
      page,
      PageManager.CONSTANT.OFFSET_PAGE_TYPE,
      PageManager.CONSTANT.SIZE_PAGE_TYPE
    )
  }

  /**
   * Sets the page ID.
   * @param page Page data
   * @param pageId Page ID
   */
  setPageId(page: Uint8Array, pageId: number): void {
    numberToBytes(
      pageId,
      page,
      PageManager.CONSTANT.OFFSET_PAGE_ID,
      PageManager.CONSTANT.SIZE_PAGE_ID
    )
  }

  /**
   * Sets the ID of the next connected page.
   * @param page Page data
   * @param nextPageId Next connected page ID
   */
  setNextPageId(page: Uint8Array, nextPageId: number): void {
    numberToBytes(
      nextPageId,
      page,
      PageManager.CONSTANT.OFFSET_NEXT_PAGE_ID,
      PageManager.CONSTANT.SIZE_NEXT_PAGE_ID
    )
  }

  /**
   * Sets the remaining capacity of the page.
   * @param page Page data
   * @param remainingCapacity Remaining capacity
   */
  setRemainingCapacity(page: Uint8Array, remainingCapacity: number): void {
    numberToBytes(
      remainingCapacity,
      page,
      PageManager.CONSTANT.OFFSET_REMAINING_CAPACITY,
      PageManager.CONSTANT.SIZE_REMAINING_CAPACITY
    )
  }

  /**
   * Sets the checksum of the page.
   * @param page Page data
   * @param checksum Checksum
   */
  setChecksum(page: Uint8Array, checksum: number): void {
    numberToBytes(
      checksum,
      page,
      PageManager.CONSTANT.OFFSET_CHECKSUM,
      PageManager.CONSTANT.SIZE_CHECKSUM
    )
  }

  /**
   * Updates the checksum of the page.
   * Calculates the checksum of the page body (excluding the header) and sets it in the header.
   * @param page Page data
   */
  updateChecksum(page: Uint8Array): void {
    const body = this.getBody(page)
    const checksum = crc32(body)
    this.setChecksum(page, checksum)
  }

  /**
   * Verifies the checksum of the page.
   * Calculates the checksum of the page body and compares it with the checksum stored in the header.
   * @param page Page data
   * @returns boolean indicating if the checksum is valid
   */
  verifyChecksum(page: Uint8Array): boolean {
    const body = this.getBody(page)
    const checksum = crc32(body)
    const storedChecksum = this.getChecksum(page)
    return checksum === storedChecksum
  }

  /**
   * Sets the page header.
   * @param page Page data
   * @param header Page header
   */
  setHeader(page: Uint8Array, header: Uint8Array): void {
    page.set(header)
  }

  /**
   * Sets the page body.
   * @param page Page data
   * @param body Page body
   */
  setBody(page: Uint8Array, body: Uint8Array): void {
    page.set(body, PageManager.CONSTANT.SIZE_PAGE_HEADER)
  }

  /**
   * Initializes the page.
   * @param page Page data
   * @param pageType Page type
   * @param pageId Page ID
   * @param nextPageId Next connected page ID
   * @param remainingCapacity Remaining capacity
   */
  initial(
    page: Uint8Array,
    pageType: number,
    pageId: number,
    nextPageId: number,
    remainingCapacity: number
  ): void {
    this.setPageType(page, pageType)
    this.setPageId(page, pageId)
    this.setNextPageId(page, nextPageId)
    this.setRemainingCapacity(page, remainingCapacity)
  }

  /**
   * Returns the body of the page.
   * @param page Page data
   * @returns Page body
   */
  getBody(page: Uint8Array): Uint8Array {
    return page.subarray(PageManager.CONSTANT.SIZE_PAGE_HEADER)
  }

  /**
   * Returns the page type.
   */
  abstract get pageType(): number

  /**
   * Creates a new page.
   * @param pageSize Page size
   * @param pageId Page ID
   * @returns Created page data
   */
  create(pageSize: number, pageId: number): Uint8Array {
    const page = new Uint8Array(pageSize)
    const headerSize = PageManager.CONSTANT.SIZE_PAGE_HEADER
    const remainingCapacity = pageSize - headerSize

    this.initial(
      page,
      this.pageType,
      pageId,
      -1,
      remainingCapacity
    )
    return page
  }
}

/**
 * Represents that a page is empty.
 */
export class EmptyPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_EMPTY
  }

  /**
   * Checks if the page type is `EmptyPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `EmptyPage`
   */
  static IsEmptyPage(page: Uint8Array): page is EmptyPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_EMPTY
  }

  /**
   * Checks if the page type is `EmptyPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `EmptyPage`
   */
  isEmptyPage(page: Uint8Array): page is EmptyPage {
    return EmptyPageManager.IsEmptyPage(page)
  }
}

/**
 * Represents a data page.
 * A data page is where actual data is stored.
 * Rows are stored in data pages. The position where a row is stored is determined by the slot offset.
 * Slot offsets occupy 2 bytes each starting from the end of the page.
 * 
 * If the row size is larger than the remaining capacity of the page, a new page is created and used.
 * If the row size is larger than the page size itself, it is stored in an overflow page. 
 * In this case, the row has its overflow flag set in the header, and the body stores the ID of the overflow page.
 */
export class DataPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_DATA
  }

  protected readonly row = new Row()

  /**
   * Checks if the page type is `DataPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `DataPage`
   */
  static IsDataPage(page: Uint8Array): page is DataPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_DATA
  }

  /**
   * Checks if the page type is `DataPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `DataPage`
   */
  isDataPage(page: Uint8Array): page is DataPage {
    return DataPageManager.IsDataPage(page)
  }

  /**
   * Returns the offset of a row within the page.
   * @param page Page data
   * @param slotIndex Slot index
   * @returns Row offset within the page
   */
  getRowOffset(page: DataPage, slotIndex: number): number {
    return bytesToNumber(
      page,
      (page.length - PageManager.CONSTANT.SIZE_SLOT_OFFSET) - (slotIndex * PageManager.CONSTANT.SIZE_SLOT_OFFSET),
      PageManager.CONSTANT.SIZE_SLOT_OFFSET
    )
  }

  /**
   * Returns the row from the page.
   * @param page Page data
   * @param slotIndex Slot index
   * @returns Row data within the page
   */
  getRow(page: DataPage, slotIndex: number): Uint8Array {
    const offset = this.getRowOffset(page, slotIndex)
    const headerSize = Row.CONSTANT.SIZE_HEADER
    const bodySize = this.row.getBodySize(page.subarray(offset))
    const row = page.subarray(offset, offset + headerSize + bodySize)
    return row
  }

  /**
   * Returns the number of rows inserted into the page.
   * @param page Page data
   * @returns Number of rows inserted
   */
  getInsertedRowCount(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_INSERTED_ROW_COUNT,
      PageManager.CONSTANT.SIZE_INSERTED_ROW_COUNT
    )
  }

  /**
   * Sets the offset of a row within the page.
   * @param page Page data
   * @param slotIndex Slot index
   * @param offset Row offset within the page
   */
  setRowOffset(page: DataPage, slotIndex: number, offset: number): void {
    numberToBytes(
      offset,
      page,
      (page.length - PageManager.CONSTANT.SIZE_SLOT_OFFSET) -
      (slotIndex * PageManager.CONSTANT.SIZE_SLOT_OFFSET),
      PageManager.CONSTANT.SIZE_SLOT_OFFSET
    )
  }

  /**
   * Sets the number of rows inserted into the page.
   * @param page Page data
   * @param insertedRowCount Number of rows inserted
   */
  setInsertedRowCount(page: Uint8Array, insertedRowCount: number): void {
    numberToBytes(
      insertedRowCount,
      page,
      PageManager.CONSTANT.OFFSET_INSERTED_ROW_COUNT,
      PageManager.CONSTANT.SIZE_INSERTED_ROW_COUNT
    )
  }

  /**
   * Calculates if there is space for a row in the page and determines insertability.
   * If insertable, returns the slot index to be inserted.
   * If not, returns -1.
   * @param page Page data
   * @param row Row data
   * @returns Slot index for the row
   */
  getNextSlotIndex(page: DataPage, row: Uint8Array): number {
    const slotOffsetSize = PageManager.CONSTANT.SIZE_SLOT_OFFSET
    const remainingCapacity = this.getRemainingCapacity(page)
    const totalSize = row.length + slotOffsetSize
    return remainingCapacity >= totalSize ? this.getInsertedRowCount(page) : -1
  }

  /**
   * Returns the position for the next row to be inserted.
   * @param page Page data
   * @returns Next insert position
   */
  getNextInsertPosition(page: DataPage): number {
    const insertedRowCount = this.getInsertedRowCount(page)
    if (insertedRowCount === 0) {
      return DataPageManager.CONSTANT.SIZE_PAGE_HEADER
    }
    const lastRowIndex = insertedRowCount - 1
    const lastRowOffset = this.getRowOffset(page, lastRowIndex)
    const lastRow = this.getRow(page, lastRowIndex)

    return lastRowOffset + lastRow.length
  }

  /**
   * Inserts a row into the page. `getNextSlotIndex` should be called beforehand to verify availability.
   * @param page Page data
   * @param row Row data
   */
  insert(page: DataPage, row: Uint8Array): void {
    const slotOffsetSize = PageManager.CONSTANT.SIZE_SLOT_OFFSET
    const remainingCapacity = this.getRemainingCapacity(page)
    const totalSize = row.length + slotOffsetSize
    if (remainingCapacity < totalSize) {
      throw new Error('Not enough space to insert row')
    }
    const insertedRowCount = this.getInsertedRowCount(page)
    const offset = this.getNextInsertPosition(page)
    page.set(row, offset)
    this.setRowOffset(page, insertedRowCount, offset)
    this.setInsertedRowCount(page, insertedRowCount + 1)
    this.setRemainingCapacity(page, remainingCapacity - totalSize)
  }
}

/**
 * Represents an index page.
 * An index page is used to store page indices.
 */
export class IndexPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_INDEX
  }

  static readonly CONSTANT = {
    ...PageManager.CONSTANT,
    OFFSET_INDEX_ID: 100,
    OFFSET_PARENT_INDEX_ID: 104,
    OFFSET_NEXT_INDEX_ID: 108,
    OFFSET_PREV_INDEX_ID: 112,
    OFFSET_IS_LEAF: 116,
    OFFSET_KEYS_COUNT: 117,
    OFFSET_VALUES_COUNT: 121,
    OFFSET_KEYS_AND_VALUES: 128, // 8-byte aligned (original 125 -> 128)
    SIZE_INDEX_ID: 4,
    SIZE_PARENT_INDEX_ID: 4,
    SIZE_NEXT_INDEX_ID: 4,
    SIZE_PREV_INDEX_ID: 4,
    SIZE_IS_LEAF: 1,
    SIZE_KEYS_COUNT: 4,
    SIZE_VALUES_COUNT: 4,
    SIZE_KEY: 8,   // Updated to 8 bytes for Float64
    SIZE_VALUE: 8, // Updated to 8 bytes for Float64
  } as const

  /**
   * Checks if the page type is `IndexPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `IndexPage`
   */
  static IsIndexPage(page: Uint8Array): page is IndexPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_INDEX
  }

  /**
   * Checks if the page type is `IndexPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `IndexPage`
   */
  isIndexPage(page: Uint8Array): page is IndexPage {
    return IndexPageManager.IsIndexPage(page)
  }

  /**
   * Gets the index ID of the page.
   * @param page Page data
   * @returns Index ID
   */
  getIndexId(page: Uint8Array): number {
    return bytesToNumber(
      page,
      IndexPageManager.CONSTANT.OFFSET_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_INDEX_ID
    )
  }

  /**
   * Gets the parent index ID of the page.
   * @param page Page data
   * @returns Parent index ID
   */
  getParentIndexId(page: Uint8Array): number {
    return bytesToNumber(
      page,
      IndexPageManager.CONSTANT.OFFSET_PARENT_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_PARENT_INDEX_ID
    )
  }

  /**
   * Gets the next index ID of the page.
   * @param page Page data
   * @returns Next index ID
   */
  getNextIndexId(page: Uint8Array): number {
    return bytesToNumber(
      page,
      IndexPageManager.CONSTANT.OFFSET_NEXT_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_NEXT_INDEX_ID
    )
  }

  /**
   * Gets the previous index ID of the page.
   * @param page Page data
   * @returns Previous index ID
   */
  getPrevIndexId(page: Uint8Array): number {
    return bytesToNumber(
      page,
      IndexPageManager.CONSTANT.OFFSET_PREV_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_PREV_INDEX_ID
    )
  }

  /**
   * Gets the is leaf of the page.
   * @param page Page data
   * @returns Is leaf
   */
  getIsLeaf(page: Uint8Array): boolean {
    return bytesToNumber(
      page,
      IndexPageManager.CONSTANT.OFFSET_IS_LEAF,
      IndexPageManager.CONSTANT.SIZE_IS_LEAF
    ) === 1
  }

  /**
   * Gets the keys count of the page.
   * @param page Page data
   * @returns Keys count
   */
  getKeysCount(page: Uint8Array): number {
    return bytesToNumber(
      page,
      IndexPageManager.CONSTANT.OFFSET_KEYS_COUNT,
      IndexPageManager.CONSTANT.SIZE_KEYS_COUNT
    )
  }

  /**
   * Gets the values count of the page.
   * @param page Page data
   * @returns Values count
   */
  getValuesCount(page: Uint8Array): number {
    return bytesToNumber(
      page,
      IndexPageManager.CONSTANT.OFFSET_VALUES_COUNT,
      IndexPageManager.CONSTANT.SIZE_VALUES_COUNT
    )
  }

  /**
   * Sets the index ID of the page.
   * @param page Page data
   * @param indexId Index ID
   */
  setIndexId(page: Uint8Array, indexId: number): void {
    numberToBytes(
      indexId,
      page,
      IndexPageManager.CONSTANT.OFFSET_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_INDEX_ID
    )
  }

  /**
   * Sets the parent index ID of the page.
   * @param page Page data
   * @param parentIndexId Parent index ID
   */
  setParentIndexId(page: Uint8Array, parentIndexId: number): void {
    numberToBytes(
      parentIndexId,
      page,
      IndexPageManager.CONSTANT.OFFSET_PARENT_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_PARENT_INDEX_ID
    )
  }

  /**
   * Sets the next index ID of the page.
   * @param page Page data
   * @param nextIndexId Next index ID
   */
  setNextIndexId(page: Uint8Array, nextIndexId: number): void {
    numberToBytes(
      nextIndexId,
      page,
      IndexPageManager.CONSTANT.OFFSET_NEXT_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_NEXT_INDEX_ID
    )
  }

  /**
   * Sets the previous index ID of the page.
   * @param page Page data
   * @param prevIndexId Previous index ID
   */
  setPrevIndexId(page: Uint8Array, prevIndexId: number): void {
    numberToBytes(
      prevIndexId,
      page,
      IndexPageManager.CONSTANT.OFFSET_PREV_INDEX_ID,
      IndexPageManager.CONSTANT.SIZE_PREV_INDEX_ID
    )
  }

  /**
   * Sets the is leaf of the page.
   * @param page Page data
   * @param isLeaf Is leaf
   */
  setIsLeaf(page: Uint8Array, isLeaf: boolean): void {
    numberToBytes(
      isLeaf ? 1 : 0,
      page,
      IndexPageManager.CONSTANT.OFFSET_IS_LEAF,
      IndexPageManager.CONSTANT.SIZE_IS_LEAF
    )
  }

  /**
   * Sets the keys count of the page.
   * @param page Page data
   * @param keysCount Keys count
   */
  setKeysCount(page: Uint8Array, keysCount: number): void {
    numberToBytes(
      keysCount,
      page,
      IndexPageManager.CONSTANT.OFFSET_KEYS_COUNT,
      IndexPageManager.CONSTANT.SIZE_KEYS_COUNT
    )
  }

  /**
   * Sets the values count of the page.
   * @param page Page data
   * @param valuesCount Values count
   */
  setValuesCount(page: Uint8Array, valuesCount: number): void {
    numberToBytes(
      valuesCount,
      page,
      IndexPageManager.CONSTANT.OFFSET_VALUES_COUNT,
      IndexPageManager.CONSTANT.SIZE_VALUES_COUNT
    )
  }

  /**
   * Gets the keys of the page.
   * @param page Page data
   * @returns Keys
   */
  getKeys(page: Uint8Array): number[] {
    const keysCount = this.getKeysCount(page)
    const byteOffset = page.byteOffset + IndexPageManager.CONSTANT.OFFSET_KEYS_AND_VALUES
    const keys = new Float64Array(page.buffer, byteOffset, keysCount)
    return Array.from(keys)
  }

  /**
   * Gets the values of the page.
   * @param page Page data
   * @returns Values
   */
  getValues(page: Uint8Array): number[] {
    const keysCount = this.getKeysCount(page)
    const valuesCount = this.getValuesCount(page)
    const byteOffset = page.byteOffset + IndexPageManager.CONSTANT.OFFSET_KEYS_AND_VALUES + (keysCount * IndexPageManager.CONSTANT.SIZE_KEY)
    const values = new Float64Array(page.buffer, byteOffset, valuesCount)
    return Array.from(values)
  }

  /**
   * Sets the keys and values of the page.
   * @param page Page data
   * @param keys Keys
   * @param values Values
   */
  setKeysAndValues(page: Uint8Array, keys: number[], values: number[]): void {
    const keysCount = keys.length
    const valuesCount = values.length

    // Set Keys
    const keyByteOffset = page.byteOffset + IndexPageManager.CONSTANT.OFFSET_KEYS_AND_VALUES
    const keyDest = new Float64Array(page.buffer, keyByteOffset, keysCount)
    keyDest.set(keys)

    // Set Values
    const valByteOffset = keyByteOffset + (keysCount * IndexPageManager.CONSTANT.SIZE_KEY)
    const valDest = new Float64Array(page.buffer, valByteOffset, valuesCount)
    valDest.set(values)
  }
}

/**
 * Represents a metadata page.
 * A metadata page stores database metadata.
 */
export class MetadataPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_METADATA
  }

  static readonly CONSTANT = {
    ...PageManager.CONSTANT,
    MAGIC_STRING: 'SHARD',
    OFFSET_MAGIC_STRING: 100,
    OFFSET_PAGE_COUNT: 108,
    OFFSET_PAGE_SIZE: 112,
    OFFSET_ROW_COUNT: 116,
    OFFSET_ROOT_INDEX_PAGE_ID: 122,
    OFFSET_ROOT_INDEX_ORDER: 126,
    OFFSET_LAST_INSERT_PAGE_ID: 130,
    OFFSET_LAST_ROW_PK: 134,
    SIZE_PAGE_COUNT: 4,
    SIZE_PAGE_SIZE: 4,
    SIZE_ROOT_INDEX_PAGE_ID: 4,
    SIZE_ROOT_INDEX_ORDER: 4,
    SIZE_LAST_INSERT_PAGE_ID: 4,
  } as const

  /**
   * Checks if the page type is `MetadataPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `MetadataPage`
   */
  static IsMetadataPage(page: Uint8Array): page is MetadataPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_METADATA
  }

  /**
   * Checks if the page type is `MetadataPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `MetadataPage`
   */
  isMetadataPage(page: Uint8Array): page is MetadataPage {
    return MetadataPageManager.IsMetadataPage(page)
  }

  /**
   * Verifies if the page is a valid metadata page.
   * @param page Page data
   * @returns Whether it is a metadata page
   */
  static Verify(page: MetadataPage): boolean {
    const start = MetadataPageManager.CONSTANT.OFFSET_MAGIC_STRING
    const end = MetadataPageManager.CONSTANT.OFFSET_MAGIC_STRING + MetadataPageManager.CONSTANT.MAGIC_STRING.length
    const magicString = page.subarray(start, end)
    if (!magicString.every((byte, index) => byte === MetadataPageManager.CONSTANT.MAGIC_STRING.charCodeAt(index))) {
      return false
    }
    return true
  }

  /**
   * Returns the number of pages stored in the database.
   * @param page Page data
   * @returns Number of pages
   */
  getPageCount(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_COUNT,
      MetadataPageManager.CONSTANT.SIZE_PAGE_COUNT
    )
  }

  /**
   * Returns the database page size.
   * @param page Page data
   * @returns Page size
   */
  getPageSize(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_SIZE,
      MetadataPageManager.CONSTANT.SIZE_PAGE_SIZE
    )
  }

  /**
   * Returns the Root Index Page ID of the database.
   * @param page Page data
   * @returns Root Index Page ID
   */
  getRootIndexPageId(page: MetadataPage): number {
    const id = bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_PAGE_ID,
      MetadataPageManager.CONSTANT.SIZE_ROOT_INDEX_PAGE_ID
    )
    return id === 0xFFFFFFFF ? -1 : id
  }

  /**
   * Returns the order of the database Root Index Page.
   * @param page Page data
   * @returns Root Index Page order
   */
  getRootIndexOrder(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_ORDER,
      MetadataPageManager.CONSTANT.SIZE_ROOT_INDEX_ORDER
    )
  }

  /**
   * Returns the ID of the last insertion page.
   * @param page Page data
   * @returns Last insert page ID
   */
  getLastInsertPageId(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_INSERT_PAGE_ID,
      MetadataPageManager.CONSTANT.SIZE_LAST_INSERT_PAGE_ID
    )
  }

  /**
   * Returns the PK of the last inserted row in the database.
   * @param page Page data
   * @returns Last inserted row PK
   */
  getLastRowPk(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_ROW_PK,
      Row.CONSTANT.SIZE_PK
    )
  }

  /**
   * Returns the number of rows in the database.
   * @param page Page data
   * @returns Number of rows
   */
  getRowCount(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROW_COUNT,
      Row.CONSTANT.SIZE_PK
    )
  }

  /**
   * Sets the number of pages stored in the database.
   * @param page Page data
   * @param pageCount Number of pages
   */
  setPageCount(page: MetadataPage, pageCount: number): void {
    numberToBytes(
      pageCount,
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_COUNT,
      MetadataPageManager.CONSTANT.SIZE_PAGE_COUNT
    )
  }

  /**
   * Sets the database page size.
   * @param page Page data
   * @param pageSize Page size
   */
  setPageSize(page: MetadataPage, pageSize: number): void {
    numberToBytes(
      pageSize,
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_SIZE,
      MetadataPageManager.CONSTANT.SIZE_PAGE_SIZE
    )
  }

  /**
   * Sets the Root Index Page ID of the database.
   * @param page Page data
   * @param rootIndexPageId Root Index Page ID
   */
  setRootIndexPageId(page: MetadataPage, rootIndexPageId: number): void {
    numberToBytes(
      rootIndexPageId,
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_PAGE_ID,
      MetadataPageManager.CONSTANT.SIZE_ROOT_INDEX_PAGE_ID
    )
  }

  /**
   * Sets the order of the database Root Index Page.
   * @param page Page data
   * @param order Root Index Page order
   */
  setRootIndexOrder(page: MetadataPage, order: number): void {
    numberToBytes(
      order,
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_ORDER,
      MetadataPageManager.CONSTANT.SIZE_ROOT_INDEX_ORDER
    )
  }

  /**
   * Sets the magic string of the page.
   * @param page Page data
   */
  setMagicString(page: MetadataPage): void {
    const encoding = new TextEncoder()
    const buffer = encoding.encode(MetadataPageManager.CONSTANT.MAGIC_STRING)
    page.set(buffer, MetadataPageManager.CONSTANT.OFFSET_MAGIC_STRING)
  }

  /**
   * Sets the ID of the last insertion page.
   * @param page Page data
   * @param lastInsertPageId Last insert page ID
   */
  setLastInsertPageId(page: MetadataPage, lastInsertPageId: number): void {
    numberToBytes(
      lastInsertPageId,
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_INSERT_PAGE_ID,
      MetadataPageManager.CONSTANT.SIZE_LAST_INSERT_PAGE_ID
    )
  }

  /**
   * Sets the PK of the last inserted row in the database.
   * @param page Page data
   * @param lastRowPk Last inserted row PK
   */
  setLastRowPk(page: MetadataPage, lastRowPk: number): void {
    numberToBytes(
      lastRowPk,
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_ROW_PK,
      Row.CONSTANT.SIZE_PK
    )
  }

  /**
   * Sets the number of rows in the database.
   * @param page Page data
   * @param rowCount Number of rows
   */
  setRowCount(page: MetadataPage, rowCount: number): void {
    numberToBytes(
      rowCount,
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROW_COUNT,
      Row.CONSTANT.SIZE_PK
    )
  }
}

/**
 * Represents a bitmap page.
 * A bitmap page stores a bitmap of pages.
 */
export class BitmapPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_BITMAP
  }

  /**
   * Checks if the page type is `BitmapPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `BitmapPage`
   */
  static IsBitmapPage(page: Uint8Array): page is BitmapPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_BITMAP
  }

  /**
   * Checks if the page type is `BitmapPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `BitmapPage`
   */
  isBitmapPage(page: Uint8Array): page is BitmapPage {
    return BitmapPageManager.IsBitmapPage(page)
  }

  /**
   * Checks if a page is empty.
   * @param page Page data
   * @param index Bitmap index
   * @returns boolean indicating if the page is empty
   */
  isEmptyPage(page: BitmapPage, index: number): boolean {
    return bytesToNumber(page, index, 1) === 0
  }
}

/**
 * Represents an overflow page.
 * An overflow page stores overflow data of pages.
 * Overflow pages store data of rows in data pages where the overflow flag is set.
 */
export class OverflowPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_OVERFLOW
  }

  /**
   * Checks if the page type is `OverflowPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `OverflowPage`
   */
  static IsOverflowPage(page: Uint8Array): page is OverflowPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_OVERFLOW
  }

  /**
   * Checks if the page type is `OverflowPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `OverflowPage`
   */
  isOverflowPage(page: Uint8Array): page is OverflowPage {
    return OverflowPageManager.IsOverflowPage(page)
  }
}

export class UnknownPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_UNKNOWN
  }

  /**
   * Checks if the page type is `UnknownPage`.
   * @param page Page data
   * @returns boolean indicating if the page type is `UnknownPage`
   */
  static IsUnknownPage(page: Uint8Array): page is UnknownPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_UNKNOWN
  }

  /**
   * Checks if the page is `UnknownPage`.
   * @param page Page data
   * @returns `true` if the page is `UnknownPage`, otherwise `false`
   */
  isUnknownPage(page: Uint8Array): page is UnknownPage {
    return UnknownPageManager.IsUnknownPage(page)
  }
}

export class PageManagerFactory {
  protected static readonly EmptyPage = new EmptyPageManager()
  protected static readonly DataPage = new DataPageManager()
  protected static readonly IndexPage = new IndexPageManager()
  protected static readonly MetadataPage = new MetadataPageManager()
  protected static readonly BitmapPage = new BitmapPageManager()
  protected static readonly OverflowPage = new OverflowPageManager()
  protected static readonly UnknownPage = new UnknownPageManager()

  /**
   * Returns the page type.
   * @param page Page data
   * @returns Page type
   */
  protected getPageType(page: Uint8Array): number {
    return bytesToNumber(page, PageManager.CONSTANT.OFFSET_PAGE_TYPE, PageManager.CONSTANT.SIZE_PAGE_TYPE)
  }

  /**
   * Checks if the page is `EmptyPage`.
   * @param page Page data
   * @returns `true` if the page is `EmptyPage`, otherwise `false`
   */
  isEmptyPage(page: Uint8Array): page is EmptyPage {
    return EmptyPageManager.IsEmptyPage(page)
  }

  /**
   * Checks if the page is `DataPage`.
   * @param page Page data
   * @returns `true` if the page is `DataPage`, otherwise `false`
   */
  isDataPage(page: Uint8Array): page is DataPage {
    return DataPageManager.IsDataPage(page)
  }

  /**
   * Checks if the page is `IndexPage`.
   * @param page Page data
   * @returns `true` if the page is `IndexPage`, otherwise `false`
   */
  isIndexPage(page: Uint8Array): page is IndexPage {
    return IndexPageManager.IsIndexPage(page)
  }

  /**
   * Checks if the page is `MetadataPage`.
   * @param page Page data
   * @returns `true` if the page is `MetadataPage`, otherwise `false`
   */
  isMetadataPage(page: Uint8Array): page is MetadataPage {
    return MetadataPageManager.IsMetadataPage(page)
  }

  /**
   * Checks if the page is `BitmapPage`.
   * @param page Page data
   * @returns `true` if the page is `BitmapPage`, otherwise `false`
   */
  isBitmapPage(page: Uint8Array): page is BitmapPage {
    return BitmapPageManager.IsBitmapPage(page)
  }

  /**
   * Checks if the page is `OverflowPage`.
   * @param page Page data
   * @returns `true` if the page is `OverflowPage`, otherwise `false`
   */
  isOverflowPage(page: Uint8Array): page is OverflowPage {
    return OverflowPageManager.IsOverflowPage(page)
  }

  /**
   * Checks if the page is `UnknownPage`.
   * @param page Page data
   * @returns `true` if the page is `UnknownPage`, otherwise `false`
   */
  isUnknownPage(page: Uint8Array): page is UnknownPage {
    return UnknownPageManager.IsUnknownPage(page)
  }

  /**
   * Returns a page manager based on the page type.
   * @param page Page data
   * @returns Page manager
   */
  getManager(page: EmptyPage): EmptyPageManager
  getManager(page: MetadataPage): MetadataPageManager
  getManager(page: IndexPage): IndexPageManager
  getManager(page: DataPage): DataPageManager
  getManager(page: BitmapPage): BitmapPageManager
  getManager(page: OverflowPage): OverflowPageManager
  getManager(page: UnknownPage): UnknownPageManager
  getManager(page: Uint8Array): PageManager
  getManager(page: Uint8Array): PageManager {
    switch (this.getPageType(page)) {
      case PageManager.CONSTANT.PAGE_TYPE_EMPTY:
        return PageManagerFactory.EmptyPage
      case PageManager.CONSTANT.PAGE_TYPE_METADATA:
        return PageManagerFactory.MetadataPage
      case PageManager.CONSTANT.PAGE_TYPE_BITMAP:
        return PageManagerFactory.BitmapPage
      case PageManager.CONSTANT.PAGE_TYPE_INDEX:
        return PageManagerFactory.IndexPage
      case PageManager.CONSTANT.PAGE_TYPE_DATA:
        return PageManagerFactory.DataPage
      case PageManager.CONSTANT.PAGE_TYPE_OVERFLOW:
        return PageManagerFactory.OverflowPage
      case PageManager.CONSTANT.PAGE_TYPE_UNKNOWN:
        return PageManagerFactory.UnknownPage
      default:
        throw new Error(`Invalid page type: ${this.getPageType(page)}`)
    }
  }

  getManagerFromType(pageType: number): PageManager {
    switch (pageType) {
      case PageManager.CONSTANT.PAGE_TYPE_EMPTY:
        return PageManagerFactory.EmptyPage
      case PageManager.CONSTANT.PAGE_TYPE_METADATA:
        return PageManagerFactory.MetadataPage
      case PageManager.CONSTANT.PAGE_TYPE_BITMAP:
        return PageManagerFactory.BitmapPage
      case PageManager.CONSTANT.PAGE_TYPE_INDEX:
        return PageManagerFactory.IndexPage
      case PageManager.CONSTANT.PAGE_TYPE_DATA:
        return PageManagerFactory.DataPage
      case PageManager.CONSTANT.PAGE_TYPE_OVERFLOW:
        return PageManagerFactory.OverflowPage
      case PageManager.CONSTANT.PAGE_TYPE_UNKNOWN:
        return PageManagerFactory.UnknownPage
      default:
        throw new Error(`Invalid page type: ${pageType}`)
    }
  }
}