import { DataPage, IndexPage, BitmapPage, OverflowPage, MetadataPage, EmptyPage, UnknownPage } from '../types'
import { bytesToNumber, numberToBytes } from '../utils'
import { Row } from './Row'

/**
 * 페이지의 종류마다 있는 공통적인 메소드를 관리합니다.
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
    OFFSET_PAGE_TYPE: 0,
    OFFSET_PAGE_ID: 1,
    OFFSET_NEXT_PAGE_ID: 5,
    OFFSET_INSERTED_ROW_COUNT: 9,
    OFFSET_REMAINING_CAPACITY: 13,
  } as const

  /**
   * 페이지 타입을 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입
   */
  static GetPageType(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_PAGE_TYPE,
      PageManager.CONSTANT.SIZE_PAGE_TYPE
    )
  }

  /**
   * 페이지 타입을 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입
   */
  getPageType(page: Uint8Array): number {
    return PageManager.GetPageType(page)
  }

  /**
   * 페이지 아이디를 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 아이디
   */
  getPageId(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_PAGE_ID,
      PageManager.CONSTANT.SIZE_PAGE_ID
    )
  }

  /**
   * 페이지 연결된 다음 아이디를 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 연결된 다음 아이디
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
   * 페이지 남은 용량을 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 남은 용량
   */
  getRemainingCapacity(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_REMAINING_CAPACITY,
      PageManager.CONSTANT.SIZE_REMAINING_CAPACITY
    )
  }

  /**
   * 페이지 타입을 설정합니다.
   * @param page 페이지 데이터
   * @param pageType 페이지 타입
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
   * 페이지 아이디를 설정합니다.
   * @param page 페이지 데이터
   * @param pageId 페이지 아이디
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
   * 페이지 연결된 다음 아이디를 설정합니다.
   * @param page 페이지 데이터
   * @param nextPageId 페이지 연결된 다음 아이디
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
   * 페이지 남은 용량을 설정합니다.
   * @param page 페이지 데이터
   * @param remainingCapacity 페이지 남은 용량
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
   * 페이지 헤더를 설정합니다.
   * @param page 페이지 데이터
   * @param header 페이지 헤더
   */
  setHeader(page: Uint8Array, header: Uint8Array): void {
    page.set(header)
  }

  /**
   * 페이지 바디를 설정합니다.
   * @param page 페이지 데이터
   * @param body 페이지 바디
   */
  setBody(page: Uint8Array, body: Uint8Array): void {
    page.set(body, PageManager.CONSTANT.SIZE_PAGE_HEADER)
  }

  /**
   * 페이지를 초기화합니다.
   * @param page 페이지 데이터
   * @param pageType 페이지 타입
   * @param pageId 페이지 아이디
   * @param nextPageId 페이지 연결된 다음 아이디
   * @param remainingCapacity 페이지 남은 용량
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
   * 페이지의 본문을 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지의 본문
   */
  getBody(page: Uint8Array): Uint8Array {
    return page.subarray(PageManager.CONSTANT.SIZE_PAGE_HEADER)
  }

  /**
   * 페이지 타입을 반환합니다.
   */
  abstract get pageType(): number

  /**
   * 새로운 페이지를 생성합니다.
   * @param pageSize 페이지 크기
   * @param pageId 페이지 아이디
   * @returns 생성된 페이지 데이터
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
 * 페이지가 비어있다는 것을 나타냅니다.
 */
export class EmptyPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_EMPTY
  }

  /**
   * 페이지 타입이 `EmptyPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `EmptyPage`인지 나타내는 boolean 값
   */
  static IsEmptyPage(page: Uint8Array): page is EmptyPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_EMPTY
  }

  /**
   * 페이지 타입이 `EmptyPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `EmptyPage`인지 나타내는 boolean 값
   */
  isEmptyPage(page: Uint8Array): page is EmptyPage {
    return EmptyPageManager.IsEmptyPage(page)
  }
}

/**
 * 데이터 페이지를 나타냅니다.
 * 데이터 페이지는 실제 데이터를 저장하는 페이지입니다.
 * 데이터페이지에는 행(Row)이 저장됩니다. 행이 저장되는 위치는 slot offset에 의해 결정됩니다.
 * slot offset은 페이지의 마지막에서부터 2바이트씩 차지합니다.
 * 
 * 행의 크기가 페이지의 남은 용량보다 크다면 새로운 페이지를 생성하여 저장합니다.
 * 만일 행의 크기가 페이지의 크기보다 크다면 오버플로우 페이지에 저장합니다. 이 때 행은 헤더의 오버플로우 플래그를 설정하고, 본문에 오버플로우 페이지의 아이디를 저장합니다.
 */
export class DataPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_DATA
  }

  protected readonly row = new Row()

  /**
   * 페이지 타입이 `DataPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `DataPage`인지 나타내는 boolean 값
   */
  static IsDataPage(page: Uint8Array): page is DataPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_DATA
  }

  /**
   * 페이지 타입이 `DataPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `DataPage`인지 나타내는 boolean 값
   */
  isDataPage(page: Uint8Array): page is DataPage {
    return DataPageManager.IsDataPage(page)
  }

  /**
   * 페이지 내에 행의 위치를 반환합니다.
   * @param page 페이지 데이터
   * @param slotIndex slot 인덱스
   * @returns 페이지 내의 행의 위치
   */
  getRowOffset(page: DataPage, slotIndex: number): number {
    return bytesToNumber(
      page,
      (page.length - PageManager.CONSTANT.SIZE_SLOT_OFFSET) - (slotIndex * PageManager.CONSTANT.SIZE_SLOT_OFFSET),
      PageManager.CONSTANT.SIZE_SLOT_OFFSET
    )
  }

  /**
   * 페이지의 행을 반환합니다.
   * @param page 페이지 데이터
   * @param slotIndex slot 인덱스
   * @returns 페이지 내의 행 데이터
   */
  getRow(page: DataPage, slotIndex: number): Uint8Array {
    const offset = this.getRowOffset(page, slotIndex)
    const headerSize = Row.CONSTANT.SIZE_HEADER
    const bodySize = this.row.getBodySize(page.subarray(offset))
    const row = page.subarray(offset, offset + headerSize + bodySize)
    return row
  }

  /**
   * 페이지 삽입된 행 개수를 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 삽입된 행 개수
   */
  getInsertedRowCount(page: Uint8Array): number {
    return bytesToNumber(
      page,
      PageManager.CONSTANT.OFFSET_INSERTED_ROW_COUNT,
      PageManager.CONSTANT.SIZE_INSERTED_ROW_COUNT
    )
  }

  /**
   * 페이지의 행의 위치를 설정합니다.
   * @param page 페이지 데이터
   * @param slotIndex slot 인덱스
   * @param offset 페이지 내에 행의 위치
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
   * 페이지 삽입된 행 개수를 설정합니다.
   * @param page 페이지 데이터
   * @param insertedRowCount 페이지 삽입된 행 개수
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
   * 페이지에 행이 들어갈 공간이 있는지를 계산하고 삽입 가능 여부를 판단합니다.
   * 삽입 가능하다면 페이지에 삽입될 slot 인덱스를 반환합니다.
   * 삽입 불가능하다면 -1을 반환합니다.
   * @param page 페이지 데이터
   * @param row 행 데이터
   * @returns 페이지에 삽입될 slot 인덱스
   */
  getNextSlotIndex(page: DataPage, row: Uint8Array): number {
    const slotOffsetSize = PageManager.CONSTANT.SIZE_SLOT_OFFSET
    const remainingCapacity = this.getRemainingCapacity(page)
    const totalSize = row.length + slotOffsetSize
    return remainingCapacity >= totalSize ? this.getInsertedRowCount(page) : -1
  }

  /**
   * 페이지에 삽입될 행의 위치를 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지에 삽입될 행의 위치
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
   * 페이지에 행을 삽입합니다. 삽입 전에 `getNextSlotIndex`를 호출하여 삽입 가능 여부를 확인해야 합니다.
   * @param page 페이지 데이터
   * @param row 행 데이터
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
 * 인덱스 페이지를 나타냅니다.
 * 인덱스 페이지는 페이지의 인덱스를 저장하는 페이지입니다.
 */
export class IndexPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_INDEX
  }

  /**
   * 페이지 타입이 `IndexPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `IndexPage`인지 나타내는 boolean 값
   */
  static IsIndexPage(page: Uint8Array): page is IndexPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_INDEX
  }

  /**
   * 페이지 타입이 `IndexPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `IndexPage`인지 나타내는 boolean 값
   */
  isIndexPage(page: Uint8Array): page is IndexPage {
    return IndexPageManager.IsIndexPage(page)
  }
}

/**
 * 메타데이터 페이지를 나타냅니다.
 * 메타데이터 페이지는 데이터베이스의 메타데이터를 저장하는 페이지입니다.
 */
export class MetadataPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_METADATA
  }

  static readonly CONSTANT = {
    ...PageManager.CONSTANT,
    MAGIC_STRING: 'SHARD',
    OFFSET_MAGIC_STRING: PageManager.CONSTANT.SIZE_PAGE_HEADER,
    OFFSET_PAGE_COUNT: PageManager.CONSTANT.SIZE_PAGE_HEADER + 8,
    OFFSET_PAGE_SIZE: PageManager.CONSTANT.SIZE_PAGE_HEADER + 12,
    OFFSET_ROOT_INDEX_PAGE_ID: PageManager.CONSTANT.SIZE_PAGE_HEADER + 16,
    OFFSET_ROOT_INDEX_ORDER: PageManager.CONSTANT.SIZE_PAGE_HEADER + 20,
    OFFSET_LAST_INSERT_PAGE_ID: PageManager.CONSTANT.SIZE_PAGE_HEADER + 24,
    OFFSET_LAST_ROW_PK: PageManager.CONSTANT.SIZE_PAGE_HEADER + 28,
  } as const

  /**
   * 페이지 타입이 `MetadataPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `MetadataPage`인지 나타내는 boolean 값
   */
  static IsMetadataPage(page: Uint8Array): page is MetadataPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_METADATA
  }

  /**
   * 페이지 타입이 `MetadataPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `MetadataPage`인지 나타내는 boolean 값
   */
  isMetadataPage(page: Uint8Array): page is MetadataPage {
    return MetadataPageManager.IsMetadataPage(page)
  }

  /**
   * 페이지가 올바른 메타데이터 페이지인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 메타데이터 페이지 여부
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
   * 데이터베이스에 저장된 페이지 수를 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 수
   */
  getPageCount(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_COUNT,
      4
    )
  }

  /**
   * 데이터베이스의 페이지 크기를 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 크기
   */
  getPageSize(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_SIZE,
      4
    )
  }

  /**
   * 데이터베이스의 루트 인덱스 페이지 ID를 반환합니다.
   * @param page 페이지 데이터
   * @returns 루트 인덱스 페이지 ID
   */
  getRootIndexPageId(page: MetadataPage): number {
    const id = bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_PAGE_ID,
      4
    )
    return id === 0xFFFFFFFF ? -1 : id
  }

  /**
   * 데이터베이스의 루트 인덱스 페이지의 order를 반환합니다.
   * @param page 페이지 데이터
   * @returns 루트 인덱스 페이지의 order
   */
  getRootIndexOrder(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_ORDER,
      4
    )
  }

  /**
   * 데이터베이스의 마지막 삽입 페이지 ID를 반환합니다.
   * @param page 페이지 데이터
   * @returns 마지막 삽입 페이지 ID
   */
  getLastInsertPageId(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_INSERT_PAGE_ID,
      4
    )
  }

  /**
   * 데이터베이스의 마지막 삽입된 행의 PK를 반환합니다.
   * @param page 페이지 데이터
   * @returns 마지막 삽입된 행의 PK
   */
  getLastRowPk(page: MetadataPage): number {
    return bytesToNumber(
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_ROW_PK,
      Row.CONSTANT.SIZE_PK
    )
  }

  /**
   * 데이터베이스에 저장된 페이지 수를 설정합니다.
   * @param page 페이지 데이터
   * @param pageCount 페이지 수
   */
  setPageCount(page: MetadataPage, pageCount: number): void {
    numberToBytes(
      pageCount,
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_COUNT,
      4
    )
  }

  /**
   * 데이터베이스의 페이지 크기를 설정합니다.
   * @param page 페이지 데이터
   * @param pageSize 페이지 크기
   */
  setPageSize(page: MetadataPage, pageSize: number): void {
    numberToBytes(
      pageSize,
      page,
      MetadataPageManager.CONSTANT.OFFSET_PAGE_SIZE,
      4
    )
  }

  /**
   * 데이터베이스의 루트 인덱스 페이지 ID를 설정합니다.
   * @param page 페이지 데이터
   * @param rootIndexPageId 루트 인덱스 페이지 ID
   */
  setRootIndexPageId(page: MetadataPage, rootIndexPageId: number): void {
    numberToBytes(
      rootIndexPageId,
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_PAGE_ID,
      4
    )
  }

  /**
   * 데이터베이스의 루트 인덱스 페이지의 order를 설정합니다.
   * @param page 페이지 데이터
   * @param order 루트 인덱스 페이지의 order
   */
  setRootIndexOrder(page: MetadataPage, order: number): void {
    numberToBytes(
      order,
      page,
      MetadataPageManager.CONSTANT.OFFSET_ROOT_INDEX_ORDER,
      4
    )
  }

  /**
   * 페이지의 매직 문자열을 설정합니다.
   * @param page 페이지 데이터
   */
  setMagicString(page: MetadataPage): void {
    const encoding = new TextEncoder()
    const buffer = encoding.encode(MetadataPageManager.CONSTANT.MAGIC_STRING)
    page.set(buffer, MetadataPageManager.CONSTANT.OFFSET_MAGIC_STRING)
  }

  /**
   * 데이터베이스의 마지막 삽입 페이지 ID를 설정합니다.
   * @param page 페이지 데이터
   * @param lastInsertPageId 마지막 삽입 페이지 ID
   */
  setLastInsertPageId(page: MetadataPage, lastInsertPageId: number): void {
    numberToBytes(
      lastInsertPageId,
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_INSERT_PAGE_ID,
      4
    )
  }

  /**
   * 데이터베이스의 마지막 삽입된 행의 PK를 설정합니다.
   * @param page 페이지 데이터
   * @param lastRowPk 마지막 삽입된 행의 PK
   */
  setLastRowPk(page: MetadataPage, lastRowPk: number): void {
    numberToBytes(
      lastRowPk,
      page,
      MetadataPageManager.CONSTANT.OFFSET_LAST_ROW_PK,
      Row.CONSTANT.SIZE_PK
    )
  }
}

/**
 * 비트맵 페이지를 나타냅니다.
 * 비트맵 페이지는 페이지의 비트맵을 저장하는 페이지입니다.
 */
export class BitmapPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_BITMAP
  }

  /**
   * 페이지 타입이 `BitmapPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `BitmapPage`인지 나타내는 boolean 값
   */
  static IsBitmapPage(page: Uint8Array): page is BitmapPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_BITMAP
  }

  /**
   * 페이지 타입이 `BitmapPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `BitmapPage`인지 나타내는 boolean 값
   */
  isBitmapPage(page: Uint8Array): page is BitmapPage {
    return BitmapPageManager.IsBitmapPage(page)
  }

  /**
   * 페이지가 비어있는지 확인합니다.
   * @param page 페이지 데이터
   * @param index 비트맵 인덱스
   * @returns 페이지가 비어있는지 나타내는 boolean 값
   */
  isEmptyPage(page: BitmapPage, index: number): boolean {
    return bytesToNumber(page, index, 1) === 0
  }
}

/**
 * 오버플로우 페이지를 나타냅니다.
 * 오버플로우 페이지는 페이지의 오버플로우 데이터를 저장하는 페이지입니다.
 * 오버플로우 페이지는 데이터 페이지의 오버플로우 플래그가 설정된 행의 데이터를 저장합니다.
 */
export class OverflowPageManager extends PageManager {
  get pageType() {
    return PageManager.CONSTANT.PAGE_TYPE_OVERFLOW
  }

  /**
   * 페이지 타입이 `OverflowPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `OverflowPage`인지 나타내는 boolean 값
   */
  static IsOverflowPage(page: Uint8Array): page is OverflowPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_OVERFLOW
  }

  /**
   * 페이지 타입이 `OverflowPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `OverflowPage`인지 나타내는 boolean 값
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
   * 페이지 타입이 `UnknownPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입이 `UnknownPage`인지 나타내는 boolean 값
   */
  static IsUnknownPage(page: Uint8Array): page is UnknownPage {
    return PageManager.GetPageType(page) === PageManager.CONSTANT.PAGE_TYPE_UNKNOWN
  }

  /**
   * 페이지가 `UnknownPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `UnknownPage`라면 `true`, 그렇지 않다면 `false`
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
   * 페이지 타입을 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 타입
   */
  protected getPageType(page: Uint8Array): number {
    return bytesToNumber(page, PageManager.CONSTANT.OFFSET_PAGE_TYPE, PageManager.CONSTANT.SIZE_PAGE_TYPE)
  }

  /**
   * 페이지가 `EmptyPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `EmptyPage`라면 `true`, 그렇지 않다면 `false`
   */
  isEmptyPage(page: Uint8Array): page is EmptyPage {
    return EmptyPageManager.IsEmptyPage(page)
  }

  /**
   * 페이지가 `DataPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `DataPage`라면 `true`, 그렇지 않다면 `false`
   */
  isDataPage(page: Uint8Array): page is DataPage {
    return DataPageManager.IsDataPage(page)
  }

  /**
   * 페이지가 `IndexPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `IndexPage`라면 `true`, 그렇지 않다면 `false`
   */
  isIndexPage(page: Uint8Array): page is IndexPage {
    return IndexPageManager.IsIndexPage(page)
  }

  /**
   * 페이지가 `MetadataPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `MetadataPage`라면 `true`, 그렇지 않다면 `false`
   */
  isMetadataPage(page: Uint8Array): page is MetadataPage {
    return MetadataPageManager.IsMetadataPage(page)
  }

  /**
   * 페이지가 `BitmapPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `BitmapPage`라면 `true`, 그렇지 않다면 `false`
   */
  isBitmapPage(page: Uint8Array): page is BitmapPage {
    return BitmapPageManager.IsBitmapPage(page)
  }

  /**
   * 페이지가 `OverflowPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `OverflowPage`라면 `true`, 그렇지 않다면 `false`
   */
  isOverflowPage(page: Uint8Array): page is OverflowPage {
    return OverflowPageManager.IsOverflowPage(page)
  }

  /**
   * 페이지가 `UnknownPage`인지 확인합니다.
   * @param page 페이지 데이터
   * @returns 페이지가 `UnknownPage`라면 `true`, 그렇지 않다면 `false`
   */
  isUnknownPage(page: Uint8Array): page is UnknownPage {
    return UnknownPageManager.IsUnknownPage(page)
  }

  /**
   * 페이지 타입에 따라 페이지 관리자를 반환합니다.
   * @param page 페이지 데이터
   * @returns 페이지 관리자
   */
  getManager(page: EmptyPage): EmptyPageManager
  getManager(page: MetadataPage): MetadataPageManager
  getManager(page: IndexPage): IndexPageManager
  getManager(page: DataPage): DataPageManager
  getManager(page: BitmapPage): BitmapPageManager
  getManager(page: OverflowPage): OverflowPageManager
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
        throw new Error('Invalid page type')
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
        throw new Error('Invalid page type')
    }
  }
}