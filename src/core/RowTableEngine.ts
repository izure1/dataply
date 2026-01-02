import type { DataPage, ShardMetadata } from '../types'
import { NumericComparator, BPTreeAsync } from 'serializable-bptree'
import { RowIdentifierStrategy } from './RowIndexStategy'
import { PageFileSystem } from './PageFileSystem'
import { Row } from './Row'
import { KeyManager } from './KeyManager'
import { DataPageManager, MetadataPageManager, OverflowPageManager, PageManagerFactory } from './Page'
import { numberToBytes, bytesToNumber } from '../utils'
import { Transaction } from './transaction/Transaction'

export class RowTableEngine {
  protected readonly bptree: BPTreeAsync<number, number>
  protected readonly order: number
  private readonly factory: PageManagerFactory
  private readonly metadataPageManager: MetadataPageManager
  private readonly dataPageManager: DataPageManager
  private readonly overflowPageManager: OverflowPageManager
  private readonly keyManager: KeyManager
  private readonly rowManager: Row
  private readonly ridBuffer: Uint8Array
  private readonly pageIdBuffer: Uint8Array
  private readonly maxBodySize: number
  private initialized = false

  constructor(protected readonly pfs: PageFileSystem) {
    this.factory = new PageManagerFactory()
    this.metadataPageManager = this.factory.getManagerFromType(MetadataPageManager.CONSTANT.PAGE_TYPE_METADATA) as MetadataPageManager
    this.dataPageManager = this.factory.getManagerFromType(DataPageManager.CONSTANT.PAGE_TYPE_DATA) as DataPageManager
    this.overflowPageManager = this.factory.getManagerFromType(OverflowPageManager.CONSTANT.PAGE_TYPE_OVERFLOW) as OverflowPageManager
    this.rowManager = new Row()
    this.keyManager = new KeyManager()
    this.ridBuffer = new Uint8Array(Row.CONSTANT.SIZE_RID)
    this.pageIdBuffer = new Uint8Array(DataPageManager.CONSTANT.SIZE_PAGE_ID)
    this.maxBodySize = this.pfs.pageSize - DataPageManager.CONSTANT.SIZE_PAGE_HEADER
    this.order = this.getOptimalOrder(pfs.pageSize, Row.CONSTANT.SIZE_RID, Row.CONSTANT.SIZE_PK)
    this.bptree = new BPTreeAsync(new RowIdentifierStrategy(this.order, pfs), new NumericComparator())
  }

  /**
   * Initializes the B+ Tree.
   */
  async init(): Promise<void> {
    if (!this.initialized) {
      await this.bptree.init()
      this.initialized = true
    }
  }

  /**
   * Calculates the optimized order.
   * @param pageSize Page size
   * @param keySize Key size
   * @param pointerSize Pointer size
   * @returns Optimal order
   */
  private getOptimalOrder(pageSize: number, keySize: number, pointerSize: number): number {
    return Math.floor((pageSize + keySize) / (keySize + pointerSize))
  }

  /**
   * Returns the actual row size generated from the data.
   * @param rowBody Data
   * @returns Actual row size generated
   */
  private getRequiredRowSize(rowBody: Uint8Array): number {
    return Row.CONSTANT.SIZE_HEADER + DataPageManager.CONSTANT.SIZE_SLOT_OFFSET + rowBody.length
  }

  /**
   * Sets the RID in the buffer.
   * @param pageId Page ID
   * @param slotIndex Slot index
   * @returns Buffer
   */
  private setRID(pageId: number, slotIndex: number): Uint8Array {
    this.keyManager.setPageId(this.ridBuffer, pageId)
    this.keyManager.setSlotIndex(this.ridBuffer, slotIndex)
    return this.ridBuffer
  }

  /**
   * Returns the RID from the buffer.
   * @returns RID
   */
  private getRID(): number {
    return this.keyManager.toNumericKey(this.ridBuffer)
  }

  /**
   * Returns the metadata of the shard.
   * @param tx Transaction
   * @returns Metadata
   */
  async getMetadata(tx: Transaction): Promise<ShardMetadata> {
    if (!this.initialized) {
      throw new Error('RowTableEngine instance is not initialized')
    }
    const metadataPage = await this.pfs.getMetadata(tx)
    const manager = this.factory.getManagerFromType(MetadataPageManager.CONSTANT.PAGE_TYPE_METADATA) as MetadataPageManager

    return {
      pageSize: manager.getPageSize(metadataPage),
      pageCount: manager.getPageCount(metadataPage),
      rowCount: manager.getRowCount(metadataPage)
    }
  }

  /**
   * Inserts data.
   * @param data Data
   * @param tx Transaction
   * @returns PK of the inserted data
   */
  async insert(data: Uint8Array, tx: Transaction): Promise<number> {
    // 메타데이터(Page 0) 쓰기 락 획득 (Writers serialize)
    // Select(Readers)는 락을 체크하지 않으므로(Snapshot) 조회가 차단되지 않습니다.
    await tx.__acquireWriteLock(0)

    const metadataPage = await this.pfs.getMetadata(tx)
    const pk = this.metadataPageManager.getLastRowPk(metadataPage) + 1
    let lastInsertDataPageId = this.metadataPageManager.getLastInsertPageId(metadataPage)
    let lastInsertDataPage = await this.pfs.get(lastInsertDataPageId, tx)

    if (!this.factory.isDataPage(lastInsertDataPage)) {
      throw new Error(`Last insert page is not data page`)
    }

    const willRowSize = this.getRequiredRowSize(data)

    // overflow page를 생성해야하는 거대한 데이터라면 overflow page를 생성하고, 해당 페이지에 행을 삽입합니다.
    if (willRowSize > this.maxBodySize) {
      // overflow page를 생성합니다.
      const overflowPageId = await this.pfs.appendNewPage(this.overflowPageManager.pageType, tx)

      // overflow page로 이동하는 포인터 행을 생성합니다.
      const row = new Uint8Array(Row.CONSTANT.SIZE_HEADER + 4)
      this.rowManager.setPK(row, pk)
      this.rowManager.setOverflowFlag(row, true)
      this.rowManager.setBodySize(row, 4)
      this.rowManager.setBody(row, numberToBytes(overflowPageId, this.pageIdBuffer))

      // data page에 포인터 행을 추가합니다.
      const nextSlotIndex = this.dataPageManager.getNextSlotIndex(lastInsertDataPage, row)
      // 페이지에 삽입 가능하다면 페이지에 삽입합니다.
      if (nextSlotIndex !== -1) {
        this.setRID(lastInsertDataPageId, nextSlotIndex)
        this.dataPageManager.insert(lastInsertDataPage, row)
        await this.pfs.setPage(lastInsertDataPageId, lastInsertDataPage, tx)
      }
      // 페이지에 삽입 불가능하다면 새로운 data page를 생성 후 삽입합니다.
      else {
        const newPageId = await this.pfs.appendNewPage(this.dataPageManager.pageType, tx)
        const newPage = await this.pfs.get(newPageId, tx) as DataPage
        this.dataPageManager.insert(newPage, row)
        this.setRID(newPageId, 0)
        lastInsertDataPageId = newPageId
        lastInsertDataPage = newPage
        await this.pfs.setPage(newPageId, newPage, tx)
      }

      // overflow page에 실제 데이터를 삽입합니다
      await this.pfs.writePageContent(overflowPageId, data, 0, tx)
    }
    // 거대 데이터가 아니라면 마지막 데이터 페이지에 삽입합니다.
    else {
      const row = new Uint8Array(Row.CONSTANT.SIZE_HEADER + data.length)
      const slotIndex = this.dataPageManager.getNextSlotIndex(lastInsertDataPage, row)

      this.rowManager.setBodySize(row, data.length)
      this.rowManager.setBody(row, data)

      // 마지막 데이터 페이지의 크기가 부족하다면 새로운 데이터 페이지를 생성합니다.
      if (slotIndex === -1) {
        const newPageId = await this.pfs.appendNewPage(this.dataPageManager.pageType, tx)
        const newPage = await this.pfs.get(newPageId, tx) as DataPage
        this.dataPageManager.insert(newPage, row)
        this.setRID(newPageId, 0)
        lastInsertDataPageId = newPageId
        lastInsertDataPage = newPage
        await this.pfs.setPage(newPageId, newPage, tx)
      }
      // 마지막 데이터 페이지의 크기가 충분하다면 마지막 데이터 페이지에 삽입합니다.
      else {
        this.dataPageManager.insert(lastInsertDataPage, row)
        this.setRID(lastInsertDataPageId, slotIndex)
        await this.pfs.setPage(lastInsertDataPageId, lastInsertDataPage, tx)
      }
    }

    // 메타데이터를 업데이트합니다.
    const freshMetadataPage = await this.pfs.getMetadata(tx)
    this.metadataPageManager.setLastInsertPageId(freshMetadataPage, lastInsertDataPageId)
    this.metadataPageManager.setLastRowPk(freshMetadataPage, pk)

    const currentRowCount = this.metadataPageManager.getRowCount(freshMetadataPage)
    this.metadataPageManager.setRowCount(freshMetadataPage, currentRowCount + 1)

    await this.pfs.setMetadata(freshMetadataPage, tx)

    // B+트리에 삽입합니다.
    await this.bptree.insert(this.getRID(), pk)

    return pk
  }

  /**
   * Looks up the RID by PK.
   * Checks Pending Updates first if a transaction exists.
   * @param pk PK
   * @param tx Transaction
   * @returns RID or null (if not found)
   */
  private async getRidByPK(pk: number, tx: Transaction): Promise<number | null> {
    // 1. 현재 트랜잭션의 Pending Updates 우선 확인
    const pendingUpdate = tx.__getPendingIndexUpdate(pk)
    if (pendingUpdate) {
      return pendingUpdate.newRid
    }

    // 2. B+트리에서 조회
    const keys = await this.bptree.keys({ equal: pk })
    if (keys.size === 0) {
      return null
    }
    return keys.values().next().value!
  }

  /**
   * Looks up the RID corresponding to the PK in the B+ Tree and returns the actual row.
   * @param pk Primary key of the row
   * @param tx Transaction
   * @returns Raw data of the row
   */
  /**
   * Updates data.
   * @param pk Primary key of the row
   * @param data Data to update
   * @param tx Transaction
   */
  async update(pk: number, data: Uint8Array, tx: Transaction): Promise<void> {
    // B+트리에서 pk에 해당하는 rid를 조회합니다.
    const rid = await this.getRidByPK(pk, tx)
    if (rid === null) {
      return
    }
    this.keyManager.setBufferFromKey(rid, this.ridBuffer)

    const pageId = this.keyManager.getPageId(this.ridBuffer)
    const slotIndex = this.keyManager.getSlotIndex(this.ridBuffer)

    const page = await this.pfs.get(pageId, tx)
    if (!this.factory.isDataPage(page)) {
      throw new Error(`RID not found for PK: ${pk}`)
    }

    const row = this.dataPageManager.getRow(page, slotIndex)

    // 오버플로우 행인 경우: 오버플로우 페이지에 직접 데이터를 수정합니다.
    if (this.rowManager.getOverflowFlag(row)) {
      const overflowPageId = bytesToNumber(this.rowManager.getBody(row))
      await this.pfs.writePageContent(overflowPageId, data, 0, tx)
      return
    }

    // 일반 행인 경우
    await this.updateNormalRow(page, pageId, slotIndex, row, pk, data, tx)
  }

  /**
   * Updates a normal row.
   * @param page Page data
   * @param pageId Page ID
   * @param slotIndex Slot index
   * @param row Row data
   * @param pk Primary key of the row
   * @param data Data to update
   * @param tx Transaction
   */
  private async updateNormalRow(
    page: Uint8Array,
    pageId: number,
    slotIndex: number,
    row: Uint8Array,
    pk: number,
    data: Uint8Array,
    tx: Transaction
  ): Promise<void> {
    const oldBodySize = this.rowManager.getBodySize(row)
    const newBodySize = data.length

    // 새 데이터가 기존 데이터보다 짧거나 같은 경우: in-place 수정
    if (newBodySize <= oldBodySize) {
      this.rowManager.setBodySize(row, newBodySize)
      this.rowManager.setBody(row, data)
      await this.pfs.setPage(pageId, page, tx)
      return
    }

    // 일반 데이터인 경우: 새로운 행을 삽입
    const newRow = new Uint8Array(Row.CONSTANT.SIZE_HEADER + newBodySize)
    this.rowManager.setPK(newRow, pk)
    this.rowManager.setBodySize(newRow, newBodySize)
    this.rowManager.setBody(newRow, data)

    // 새 행을 삽입할 위치를 찾습니다.

    // 메타데이터 쓰기 락 획득
    await tx.__acquireWriteLock(0)

    const metadataPage = await this.pfs.getMetadata(tx)
    let lastInsertDataPageId = this.metadataPageManager.getLastInsertPageId(metadataPage)
    let lastInsertDataPage = await this.pfs.get(lastInsertDataPageId, tx) as DataPage

    if (!this.factory.isDataPage(lastInsertDataPage)) {
      throw new Error('Last insert page is not data page')
    }

    let newSlotIndex = this.dataPageManager.getNextSlotIndex(lastInsertDataPage, newRow)
    if (newSlotIndex === -1) {
      const newPageId = await this.pfs.appendNewPage(this.dataPageManager.pageType, tx)
      lastInsertDataPage = await this.pfs.get(newPageId, tx) as DataPage
      lastInsertDataPageId = newPageId
      newSlotIndex = 0
    }

    this.dataPageManager.insert(lastInsertDataPage, newRow)
    await this.pfs.setPage(lastInsertDataPageId, lastInsertDataPage, tx)

    // 페이지 재로딩: insert 과정에서 pageId와 lastInsertDataPageId가 같을 경우,
    // page 변수는 insert 이전의 상태(복사본)를 가지고 있습니다.
    // 따라서 page를 수정해서 저장하면 insert된 내용(슬롯 정보 등)이 유실될 수 있습니다.
    // 안전을 위해 항상 페이지를 다시 로드하여 작업을 수행합니다.
    const targetPage = await this.pfs.get(pageId, tx)
    if (!this.factory.isDataPage(targetPage)) {
      throw new Error('Target page is not data page')
    }
    const targetRow = this.dataPageManager.getRow(targetPage as DataPage, slotIndex)

    // 기존 행을 삭제로 표시합니다.
    this.rowManager.setDeletedFlag(targetRow, true)
    await this.pfs.setPage(pageId, targetPage, tx)

    // B+트리에서 기존 RID를 삭제하고 새 RID를 삽입합니다.
    this.setRID(pageId, slotIndex)
    const oldRidNumeric = this.getRID()
    this.setRID(lastInsertDataPageId, newSlotIndex)
    const newRidNumeric = this.getRID()

    // B+트리 업데이트 (트랜잭션 격리 지원)
    // 즉시 반영하지 않고, 커밋 시점으로 미룹니다.
    // 이는 다른 트랜잭션이 커밋되지 않은 새 RID를 참조하는 것을 방지합니다.
    if (tx.__getPendingIndexUpdates().size === 0) {
      // 커밋 시 일괄 적용 hook 등록 (최초 1회)
      tx.onCommit(async () => {
        const updates = tx.__getPendingIndexUpdates()
        for (const [key, { newRid, oldRid }] of updates) {
          await this.bptree.delete(oldRid, key)
          await this.bptree.insert(newRid, key)
        }
      })
    }
    tx.__addPendingIndexUpdate(pk, newRidNumeric, oldRidNumeric)

    const freshMetadataPage = await this.pfs.getMetadata(tx)
    this.metadataPageManager.setLastInsertPageId(freshMetadataPage, lastInsertDataPageId)
    await this.pfs.setMetadata(freshMetadataPage, tx)
  }

  /**
   * Deletes data.
   * @param pk PK of the data to delete
   * @param tx Transaction
   */
  async delete(pk: number, tx: Transaction): Promise<void> {
    // 메타데이터 쓰기 락 획득
    await tx.__acquireWriteLock(0)

    const rid = await this.getRidByPK(pk, tx)
    if (rid === null) {
      return
    }

    this.keyManager.setBufferFromKey(rid, this.ridBuffer)
    const pageId = this.keyManager.getPageId(this.ridBuffer)
    const slotIndex = this.keyManager.getSlotIndex(this.ridBuffer)

    const page = await this.pfs.get(pageId, tx)
    if (!this.factory.isDataPage(page)) {
      throw new Error(`RID not found for PK: ${pk}`)
    }

    const row = this.dataPageManager.getRow(page as DataPage, slotIndex)

    if (this.rowManager.getDeletedFlag(row)) {
      return
    }

    this.rowManager.setDeletedFlag(row, true)
    await this.pfs.setPage(pageId, page, tx)

    const metadataPage = await this.pfs.getMetadata(tx)
    const currentRowCount = this.metadataPageManager.getRowCount(metadataPage)
    this.metadataPageManager.setRowCount(metadataPage, currentRowCount - 1)
    await this.pfs.setMetadata(metadataPage, tx)
  }

  /**
   * Looks up the RID corresponding to the PK in the B+ Tree and returns the actual row.
   * @param pk Primary key of the row
   * @param tx Transaction
   * @returns Raw data of the row
   */
  async selectByPK(pk: number, tx: Transaction): Promise<Uint8Array | null> {
    const rid = await this.getRidByPK(pk, tx)
    if (rid === null) {
      return null
    }
    return this.fetchRowByRid(pk, rid, tx)
  }

  private async fetchRowByRid(pk: number, rid: number, tx: Transaction): Promise<Uint8Array | null> {
    this.keyManager.setBufferFromKey(rid, this.ridBuffer)

    const pageId = this.keyManager.getPageId(this.ridBuffer)
    const slotIndex = this.keyManager.getSlotIndex(this.ridBuffer)

    const page = await this.pfs.get(pageId, tx)
    if (!this.factory.isDataPage(page)) {
      throw new Error(`RID not found for PK: ${pk}`)
    }

    const manager = this.factory.getManager(page)
    const row = manager.getRow(page, slotIndex)

    if (this.rowManager.getDeletedFlag(row)) {
      return null
    }
    else if (this.rowManager.getOverflowFlag(row)) {
      const overflowPageId = bytesToNumber(this.rowManager.getBody(row))
      const overflowPage = await this.pfs.get(overflowPageId, tx)
      if (!this.factory.isOverflowPage(overflowPage)) {
        throw new Error(`Overflow page not found for RID: ${rid}`)
      }
      return this.pfs.getBody(overflowPageId, true, tx)
    }

    return this.rowManager.getBody(row)
  }

  /**
   * Returns the count of rows.
   * @param tx Transaction
   * @returns Row count
   */
  async getRowCount(tx: Transaction): Promise<number> {
    const metadataPage = await this.pfs.getMetadata(tx)
    return this.metadataPageManager.getRowCount(metadataPage)
  }
}
