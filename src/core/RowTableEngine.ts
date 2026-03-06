import os from 'node:os'
import type { DataPage, DataplyMetadata, DataplyOptions } from '../types'
import { NumericComparator, BPTreeAsync, BPTreeAsyncTransaction } from 'serializable-bptree'
import { RowIdentifierStrategy } from './RowIndexStrategy'
import { PageFileSystem } from './PageFileSystem'
import { Row } from './Row'
import { KeyManager } from './KeyManager'
import { DataPageManager, MetadataPageManager, OverflowPageManager, PageManagerFactory, IndexPageManager } from './Page'
import { numberToBytes, bytesToNumber, clusterNumbers } from '../utils'
import { Transaction } from './transaction/Transaction'
import { TransactionContext } from './transaction/TxContext'

export class RowTableEngine {
  protected readonly bptree: BPTreeAsync<number, number>
  protected readonly strategy: RowIdentifierStrategy
  protected readonly order: number
  protected readonly factory: PageManagerFactory
  private readonly metadataPageManager: MetadataPageManager
  private readonly dataPageManager: DataPageManager
  private readonly overflowPageManager: OverflowPageManager
  protected readonly keyManager: KeyManager
  protected readonly rowManager: Row
  protected readonly maxBodySize: number
  private readonly ridBuffer: Uint8Array
  private readonly pageIdBuffer: Uint8Array
  private initialized = false

  constructor(
    protected readonly pfs: PageFileSystem,
    protected readonly txContext: TransactionContext,
    protected readonly options: Required<DataplyOptions>
  ) {
    this.factory = new PageManagerFactory()
    this.metadataPageManager = this.factory.getManagerFromType(MetadataPageManager.CONSTANT.PAGE_TYPE_METADATA) as MetadataPageManager
    this.dataPageManager = this.factory.getManagerFromType(DataPageManager.CONSTANT.PAGE_TYPE_DATA) as DataPageManager
    this.overflowPageManager = this.factory.getManagerFromType(OverflowPageManager.CONSTANT.PAGE_TYPE_OVERFLOW) as OverflowPageManager
    this.rowManager = new Row()
    this.keyManager = new KeyManager()
    this.ridBuffer = new Uint8Array(Row.CONSTANT.SIZE_RID)
    this.pageIdBuffer = new Uint8Array(DataPageManager.CONSTANT.SIZE_PAGE_ID)
    this.maxBodySize = this.pfs.pageSize - DataPageManager.CONSTANT.SIZE_PAGE_HEADER
    this.order = this.getOptimalOrder(pfs.pageSize, IndexPageManager.CONSTANT.SIZE_KEY, IndexPageManager.CONSTANT.SIZE_VALUE)
    this.strategy = new RowIdentifierStrategy(this.order, pfs, txContext)
    const budget = os.totalmem() * 0.1
    const nodeMemory = (this.order * 24) + 256
    const capacity = Math.max(1000, Math.min(1000000, Math.floor(budget / nodeMemory)))

    this.bptree = new BPTreeAsync(
      this.strategy,
      new NumericComparator(),
      {
        capacity
      }
    )
  }

  /**
   * Retrieves the BPTree transaction associated with the given transaction.
   * If it doesn't exist, it creates a new one and registers commit/rollback hooks.
   * @param tx Dataply transaction
   * @returns BPTree transaction
   */
  private async getBPTreeTransaction(tx: Transaction): Promise<BPTreeAsyncTransaction<number, number>> {
    let btx = tx.__getBPTreeTransaction()
    if (!btx) {
      btx = await this.bptree.createTransaction()
      tx.__setBPTreeTransaction(btx)
      tx.onCommit(async () => {
        if (!tx.__isBPTreeDirty()) {
          return
        }
        if (!btx) return
        const result = await btx.commit()
        if (result.success) {
          // 삭제된 노드들의 ID를 추출하여 Strategy에서 삭제
          for (const entry of result.deleted) {
            await this.strategy.delete(entry.key)
          }
        } else {
          throw new Error(`BPTree transaction commit failed. Current Root: ${this.bptree.getRootId()}`)
        }
      })
    }
    return btx
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
  protected getOptimalOrder(pageSize: number, keySize: number, pointerSize: number): number {
    pageSize -= IndexPageManager.CONSTANT.OFFSET_KEYS_AND_VALUES
    return Math.floor((pageSize + keySize) / (keySize + pointerSize))
  }

  /**
   * Returns the actual row size generated from the data.
   * @param rowBody Data
   * @returns Actual row size generated
   */
  protected getRequiredRowSize(rowBody: Uint8Array): number {
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
   * Returns the metadata of the dataply.
   * @param tx Transaction
   * @returns Metadata
   */
  async getMetadata(tx: Transaction): Promise<DataplyMetadata> {
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
   * Inserts data (batch insert).
   * @param dataList Array of data to insert
   * @param incrementRowCount Whether to increment the row count to metadata
   * @param overflowForcly Force overflow page creation for all data
   * @param tx Transaction
   * @returns Array of PKs of the inserted data
   */
  async insert(
    dataList: Uint8Array[],
    incrementRowCount: boolean,
    overflowForcly: boolean,
    tx: Transaction
  ): Promise<number[]> {
    if (dataList.length === 0) {
      return []
    }

    // 메타데이터(Page 0) 쓰기 락 획득 (한 번만)
    await tx.__acquireWriteLock(0)

    // BPTree 트랜잭션을 미리 획득 (한 번만)
    const btx = await this.getBPTreeTransaction(tx)

    const pks: number[] = []
    const metadataPage = await this.pfs.getMetadata(tx)
    let lastPk = this.metadataPageManager.getLastRowPk(metadataPage)
    let lastInsertDataPageId = this.metadataPageManager.getLastInsertPageId(metadataPage)
    let lastInsertDataPage = await this.pfs.get(lastInsertDataPageId, tx)

    if (!this.factory.isDataPage(lastInsertDataPage)) {
      throw new Error(`Last insert page is not data page`)
    }

    const batchInsertData: [number, number][] = []
    for (const data of dataList) {
      const pk = ++lastPk
      const willRowSize = this.getRequiredRowSize(data)

      // overflow page를 생성해야하는 거대한 데이터라면 overflow page를 생성하고, 해당 페이지에 행을 삽입합니다.
      if ((willRowSize > this.maxBodySize) || overflowForcly) {
        // overflow page를 생성합니다.
        const overflowPageId = await this.pfs.appendNewPage(this.overflowPageManager.pageType, tx)

        // overflow page로 이동하는 포인터 행을 생성합니다.
        const row = new Uint8Array(Row.CONSTANT.SIZE_HEADER + 4)
        this.rowManager.setPK(row, pk)
        this.rowManager.setOverflowFlag(row, true)
        this.rowManager.setBodySize(row, 4)
        this.rowManager.setBody(row, numberToBytes(overflowPageId, this.pageIdBuffer))

        // data page에 포인터 행을 추가합니다.
        const nextSlotIndex = this.dataPageManager.getNextSlotIndex(lastInsertDataPage as DataPage, row)
        // 페이지에 삽입 가능하다면 페이지에 삽입합니다.
        if (nextSlotIndex !== -1) {
          this.setRID(lastInsertDataPageId, nextSlotIndex)
          this.dataPageManager.insert(lastInsertDataPage as DataPage, row)
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
        const slotIndex = this.dataPageManager.getNextSlotIndex(lastInsertDataPage as DataPage, row)

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
          this.dataPageManager.insert(lastInsertDataPage as DataPage, row)
          this.setRID(lastInsertDataPageId, slotIndex)
          await this.pfs.setPage(lastInsertDataPageId, lastInsertDataPage, tx)
        }
      }

      // B+트리에 삽입합니다.
      batchInsertData.push([this.getRID(), pk])
      pks.push(pk)
    }

    await btx.batchInsert(batchInsertData)

    // 메타데이터를 한 번만 업데이트합니다.
    tx.__markBPTreeDirty()
    const freshMetadataPage = await this.pfs.getMetadata(tx)
    this.metadataPageManager.setLastInsertPageId(freshMetadataPage, lastInsertDataPageId)
    this.metadataPageManager.setLastRowPk(freshMetadataPage, lastPk)

    if (incrementRowCount) {
      const currentRowCount = this.metadataPageManager.getRowCount(freshMetadataPage)
      this.metadataPageManager.setRowCount(freshMetadataPage, currentRowCount + pks.length)
    }

    await this.pfs.setMetadata(freshMetadataPage, tx)

    return pks
  }

  /**
   * Looks up the RID by PK.
   * Checks Pending Updates first if a transaction exists.
   * @param pk PK
   * @param tx Transaction
   * @returns RID or null (if not found)
   */
  private async getRidByPK(pk: number, tx: Transaction): Promise<number | null> {
    const btx = await this.getBPTreeTransaction(tx)
    const keys = await btx.keys({ equal: pk })
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
    // 쓰기 작업 전 메타데이터 락을 획득하여 동시성 충돌을 방지합니다.
    await tx.__acquireWriteLock(0)

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

    // B+트리 업데이트
    const btx = await this.getBPTreeTransaction(tx)
    await btx.delete(oldRidNumeric, pk)
    await btx.insert(newRidNumeric, pk)
    tx.__markBPTreeDirty()

    const freshMetadataPage = await this.pfs.getMetadata(tx)
    this.metadataPageManager.setLastInsertPageId(freshMetadataPage, lastInsertDataPageId)
    await this.pfs.setMetadata(freshMetadataPage, tx)
  }

  /**
   * Deletes data.
   * @param pk PK of the data to delete
   * @param decrementRowCount Whether to decrement the row count to metadata
   * @param tx Transaction
   */
  async delete(pk: number, decrementRowCount: boolean, tx: Transaction): Promise<void> {
    // 쓰기 작업 전 메타데이터 락을 획득하여 동시성 충돌을 방지합니다.
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

    // 1. 오버플로우 페이지 해제
    if (this.rowManager.getOverflowFlag(row)) {
      const overflowPageId = bytesToNumber(this.rowManager.getBody(row))
      await this.pfs.freeChain(overflowPageId, tx)
    }

    this.rowManager.setDeletedFlag(row, true)
    await this.pfs.setPage(pageId, page, tx)

    // B+트리에서 삭제합니다.
    const btx = await this.getBPTreeTransaction(tx)
    await btx.delete(rid, pk)
    tx.__markBPTreeDirty()

    if (decrementRowCount) {
      const metadataPage = await this.pfs.getMetadata(tx)
      const currentRowCount = this.metadataPageManager.getRowCount(metadataPage)
      this.metadataPageManager.setRowCount(metadataPage, currentRowCount - 1)
      await this.pfs.setMetadata(metadataPage, tx)
    }

    // 2. 빈 데이터 페이지 확인 및 해제 (새로 추가된 로직)
    const insertedRowCount = this.dataPageManager.getInsertedRowCount(page)
    let allDeleted = true

    // 마지막 삽입 페이지 ID 가져오기 (반복문 밖에서 한 번만 호출)
    const metadataPage = await this.pfs.getMetadata(tx)
    const lastInsertPageId = this.metadataPageManager.getLastInsertPageId(metadataPage)

    // 마지막 삽입 페이지라면 해제하지 않음
    if (pageId === lastInsertPageId) {
      allDeleted = false
    } else {
      let i = 0
      while (i < insertedRowCount) {
        const slotRow = this.dataPageManager.getRow(page, i)
        if (!this.rowManager.getDeletedFlag(slotRow)) {
          allDeleted = false
          break
        }
        i++
      }
    }

    if (allDeleted) {
      // 모든 행이 삭제되었다면 페이지 반환 및 초기화
      await this.pfs.freeChain(pageId, tx)
    }
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

  /**
   * Selects multiple rows by their PKs in a single B+ Tree traversal.
   * Results are returned in no guaranteed order, and PKs not found are excluded from the result.
   * @param pks Array of PKs to look up
   * @param tx Transaction
   * @returns Array of raw data of the rows in the same order as input PKs
   */
  async selectMany(pks: number[] | Float64Array, tx: Transaction): Promise<(Uint8Array | null)[]> {
    if (pks.length === 0) {
      return []
    }

    const pkIndexMap = new Map<number, number>()
    for (let i = 0, len = pks.length; i < len; i++) {
      pkIndexMap.set(pks[i], i)
    }

    const validCount = pks.length
    const pkArray = new Float64Array(validCount).fill(0)
    const ridArray = new Float64Array(validCount).fill(0)
    const indexArray = new Float64Array(validCount).fill(0)

    const btx = await this.getBPTreeTransaction(tx)

    // PK를 클러스터링하여 분산된 범위를 여러 번 조회
    const clusters = clusterNumbers(pks, this.order / 2)

    for (let i = 0, len = clusters.length; i < len; i++) {
      const cluster = clusters[i]
      const minPk = cluster[0]
      const maxPk = cluster[cluster.length - 1]

      // 단일 PK 클러스터는 equal 조회로 최적화
      if (minPk === maxPk) {
        const keys = await btx.keys({ equal: minPk })
        if (keys.size > 0) {
          const rid = keys.values().next().value!
          const index = pkIndexMap.get(minPk)
          if (index !== undefined) {
            pkArray[index] = minPk
            ridArray[index] = rid
            indexArray[index] = index
          }
        }
        continue
      }

      const stream = btx.whereStream({ gte: minPk, lte: maxPk })
      for await (const [rid, pk] of stream) {
        const index = pkIndexMap.get(pk)
        if (index !== undefined) {
          pkArray[index] = pk
          ridArray[index] = rid
          indexArray[index] = index
        }
      }
    }

    return this.fetchRowsByRids(validCount, pkArray, ridArray, indexArray, tx)
  }

  /**
   * Fetches multiple rows by their RID and PK combinations, grouping by page ID to minimize I/O.
   * @param pkRidPairs Array of {pk, rid} pairs
   * @param tx Transaction
   * @returns Array of row data in the same order as input PKs
   */
  private async fetchRowsByRids(
    validCount: number,
    pkArray: Float64Array,
    ridArray: Float64Array,
    indexArray: Float64Array,
    tx: Transaction
  ): Promise<(Uint8Array | null)[]> {
    const result: (Uint8Array | null)[] = new Array(validCount).fill(null)
    if (validCount === 0) return result

    // Group items by pageId using bitwise operations for speed
    const pageGroupMap = new Map<number, { pk: number, slotIndex: number, index: number }[]>()
    for (let i = 0; i < validCount; i++) {
      const pk = pkArray[i]
      const rid = ridArray[i]
      const index = indexArray[i]

      if (pk === 0 && rid === 0 && index === 0) continue

      const slotIndex = rid % 65536
      const pageId = Math.floor(rid / 65536)

      if (!pageGroupMap.has(pageId)) {
        pageGroupMap.set(pageId, [])
      }
      pageGroupMap.get(pageId)!.push({ pk, slotIndex, index })
    }

    // 순차 읽기를 위한 정렬
    const sortedPageIds = Array.from(pageGroupMap.keys()).sort((a, b) => a - b)
    await Promise.all(sortedPageIds.map(async (pageId) => {
      const items = pageGroupMap.get(pageId)!
      const page = await this.pfs.get(pageId, tx)
      if (!this.factory.isDataPage(page)) {
        throw new Error(`Page ${pageId} is not a data page`)
      }

      const manager = this.factory.getManager(page)
      for (let i = 0, len = items.length; i < len; i++) {
        const item = items[i]
        const row = manager.getRow(page, item.slotIndex)

        if (this.rowManager.getDeletedFlag(row)) {
          result[item.index] = null
        }
        else if (this.rowManager.getOverflowFlag(row)) {
          const overflowPageId = bytesToNumber(this.rowManager.getBody(row))
          const body = await this.pfs.getBody(overflowPageId, true, tx)
          result[item.index] = body
        }
        else {
          result[item.index] = this.rowManager.getBody(row)
        }
      }
    }))

    return result
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
