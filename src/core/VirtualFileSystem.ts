import fs from 'node:fs'
import { LRUMap } from 'cache-entanglement'
import { LogManager } from './LogManager'
import type { Transaction } from './transaction/Transaction'
import { PageManagerFactory } from './Page'

/**
 * Virtual File System class that manages and caches files in page units.
 */
export class VirtualFileSystem {
  /** Cache list (Page ID -> Data Buffer) */
  protected cache: LRUMap<number, Uint8Array>
  /** Track logical file size */
  protected fileSize: number
  /** Bit shift value for page size */
  protected pageShift: number
  /** Bit mask value for page size */
  protected pageMask: number

  // 트랜잭션 지원
  protected logManager?: LogManager
  // PageID -> 소유 트랜잭션
  protected dirtyPageOwners: Map<number, Transaction> = new Map()
  // TxID -> Transaction (활성 트랜잭션 목록)
  protected activeTransactions: Map<number, Transaction> = new Map()

  constructor(
    protected fileHandle: number,
    protected pageSize: number,
    protected pageCacheCapacity: number,
    walPath?: string | undefined | null
  ) {
    // 페이지 크기는 비트 연산 최적화를 위해 2의 거듭제곱이어야 함
    if ((pageSize & (pageSize - 1)) !== 0) {
      throw new Error('Page size must be a power of 2')
    }
    this.pageShift = Math.log2(pageSize)
    this.pageMask = pageSize - 1

    this.cache = new LRUMap(pageCacheCapacity)

    // 중요: 초기 파일 크기 로드
    this.fileSize = fs.fstatSync(fileHandle).size

    // WAL 경로가 제공된 경우에만 LogManager 초기화 및 복구 수행
    if (walPath) {
      this.logManager = new LogManager(walPath, pageSize)
      this.recover()
    }
  }

  /**
   * Performs recovery (Redo) using WAL logs.
   * Called in constructor, so it's a synchronous process and data is only reflected in cache.
   * Actual disk sync and log clearing are performed during future transactions or closure.
   */
  private recover() {
    if (!this.logManager) return

    this.logManager!.open()
    const restoredPages = this.logManager!.readAllSync()

    if (restoredPages.size === 0) {
      return
    }

    // 복구된 페이지들을 캐시에 반영하고 즉시 디스크(본 파일)에 기록 (Checkpoint)
    const promises: Promise<number>[] = []
    for (const [pageId, data] of restoredPages) {
      // [안전 장치] 손상된 WAL 데이터로 인해 거대한 스파스 파일이 생성되는 것을 방지
      if (pageId > 1000000) continue

      // Checksum verification
      try {
        const manager = new PageManagerFactory().getManager(data)
        if (!manager.verifyChecksum(data)) {
          console.warn(`[VFS] Checksum verification failed for PageID ${pageId} during recovery. Ignoring changes.`)
          continue
        }
      } catch (e) {
        console.warn(`[VFS] Failed to verify checksum for PageID ${pageId} during recovery: ${e}. Ignoring changes.`)
        continue
      }

      this.cache.set(pageId, data)

      // 즉시 디스크 동기화 (복구 시점)
      promises.push(this._writeAsync(
        this.fileHandle,
        data,
        0,
        this.pageSize,
        pageId * this.pageSize
      ))

      // 복구된 페이지가 기존 파일 범위를 벗어나면 파일 크기 업데이트
      const endPos = (pageId + 1) * this.pageSize
      if (endPos > this.fileSize) {
        this.fileSize = endPos
      }
    }

    // 복구 작업 완료 대기
    // 주의: 생성자가 비동기가 아니므로, 실제로는 초기 트랜잭션 시작 전에 복구가 완료됨을 보장해야 함.
    // 여기서는 동기적으로 처리하거나 혹은 LogManager를 사용하는 외부에서 관리해야 하지만,
    // 현재 구조상 promises를 관리하여 완료를 보장하는 방향으로 구현.
    Promise.all(promises).then(() => {
      if (this.logManager && restoredPages.size > 0) {
        this.logManager.clear().catch(console.error)
      }
    })
  }

  /**
   * Prepares the transaction for commit (Phase 1).
   * Writes dirty pages to WAL but does not update the main database file.
   * @param tx Transaction
   */
  async prepareCommit(tx: Transaction): Promise<void> {
    const dirtyPageMap = tx.__getDirtyPages()
    if (dirtyPageMap.size === 0) {
      return
    }

    if (this.logManager && dirtyPageMap.size > 0) {
      // WAL에 기록 (원자적)
      await this.logManager.append(dirtyPageMap)
    }
  }

  /**
   * Finalizes the transaction commit (Phase 2).
   * Writes commit marker to WAL and updates the main database file (Checkpoint).
   * @param tx Transaction
   */
  async finalizeCommit(tx: Transaction): Promise<void> {
    const dirtyPageMap = tx.__getDirtyPages()
    if (dirtyPageMap.size === 0) {
      this.cleanupTransaction(tx)
      return
    }

    // 1. Commit Marker 기록 (WAL 유효성 보장)
    if (this.logManager) {
      await this.logManager.writeCommitMarker()
    }

    // 2. 디스크 동기화 (Checkpoint) - 해당 트랜잭션의 페이지만 반영
    // 최적화를 위해 정렬 (PageID 기준)
    const sortedPageIds = Array.from(dirtyPageMap.keys()).sort((a, b) => a - b)
    const promises: Promise<number>[] = []

    for (const pageId of sortedPageIds) {
      const page = dirtyPageMap.get(pageId)
      if (page) {
        promises.push(this._writeAsync(
          this.fileHandle,
          page,
          0,
          this.pageSize,
          pageId * this.pageSize
        ).then((v) => {
          this.cache.set(pageId, page)
          return v
        }))
      }
    }
    await Promise.all(promises)

    // 3. 로그 비우기
    // 본인(self)만 남은 경우 (곧 0이 됨)
    if (this.activeTransactions.size <= 1) {
      if (this.logManager) {
        await this.logManager.clear()
      }
    }

    this.cleanupTransaction(tx)
  }

  /**
   * Commits the transaction (Single Phase).
   * Wrapper for prepare + finalize for backward compatibility.
   * @param tx Transaction
   */
  async commit(tx: Transaction): Promise<void> {
    await this.prepareCommit(tx)
    await this.finalizeCommit(tx)
  }

  /**
   * Rolls back the transaction.
   * @param tx Transaction
   */
  async rollback(tx: Transaction): Promise<void> {
    // [보안/정합성] 롤백 시 Dirty Page 맵의 참조를 모두 해제하고 undo 데이터로 복구
    for (const [pageId, undoData] of tx.__getUndoPages()) {
      this.cache.set(pageId, undoData)
    }

    this.cleanupTransaction(tx)
  }

  private cleanupTransaction(tx: Transaction) {
    // Dirty Page 소유권 해제
    const pageIds = tx.__getDirtyPageIds()
    for (const pageId of pageIds) {
      this.dirtyPageOwners.delete(pageId)
    }
    this.activeTransactions.delete(tx.id)
  }

  /**
   * Reads data from a specific position in the file.
   * @param handle File handle
   * @param buffer Data buffer to read into
   * @param offset Start position in buffer
   * @param length Length of data to read
   * @param position Start position in file
   * @returns Length of data read
   */
  protected _readAsync(handle: number, buffer: Uint8Array, offset: number, length: number, position: number): Promise<number> {
    return new Promise((resolve, reject) => {
      fs.read(handle, buffer, offset, length, position, (err, bytesRead) => {
        if (err) return reject(err)
        resolve(bytesRead)
      })
    })
  }

  /**
   * Writes data to a specific position in the file.
   * @param handle File handle
   * @param buffer Data buffer to write
   * @param offset Start position in buffer
   * @param length Length of data to write
   * @param position Start position in file
   */
  protected _writeAsync(handle: number, buffer: Uint8Array, offset: number, length: number, position: number): Promise<number> {
    // [안전 장치] 파일 크기 폭발 방지 (512MB 제한)
    if (position + length > 512 * 1024 * 1024) {
      return Promise.reject(new Error(`[Safety Limit] File write exceeds 512MB limit at position ${position}`))
    }

    return new Promise((resolve, reject) => {
      fs.write(handle, buffer, offset, length, position, (err, bytesWritten) => {
        if (err) return reject(err)
        resolve(bytesWritten)
      })
    })
  }

  /**
   * Appends data to the end of the file.
   * @param buffer Data buffer to append
   */
  protected _appendAsync(handle: number, buffer: Uint8Array): Promise<void> {
    // [안전 장치] 파일 크기 폭발 방지 (100MB 제한)
    if (this.fileSize + buffer.length > 100 * 1024 * 1024) {
      return Promise.reject(new Error(`[Safety Limit] File append exceeds 100MB limit`))
    }

    return new Promise((resolve, reject) => {
      fs.appendFile(handle, buffer, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  protected async _readPage(pageIndex: number, tx: Transaction): Promise<Uint8Array> {
    // 1. 현재 트랜잭션이 수정한 페이지(Dirty)인지 최우선 확인 (캐시 증발 대비)
    const txDirtyPage = tx.__getDirtyPage(pageIndex)
    if (txDirtyPage) {
      return txDirtyPage
    }

    // 2. [MVCC] 다른 트랜잭션이 수정한 페이지(Dirty)인지 확인
    if (this.activeTransactions.size > 0) {
      const ownerTx = this.dirtyPageOwners.get(pageIndex)
      if (ownerTx) {
        // 타인이 소유 중 -> Undo 데이터(Snapshot)를 읽음
        if (tx.id !== ownerTx.id) {
          const undoPage = ownerTx.__getUndoPage(pageIndex)
          if (undoPage) {
            return undoPage
          }
        }
      }
    }

    // 일반 읽기 (캐시 혹은 디스크)
    if (this.cache.has(pageIndex)) {
      return this.cache.get(pageIndex)!
    }

    const buffer = new Uint8Array(this.pageSize)
    const pageStartPos = pageIndex * this.pageSize
    if (pageStartPos >= this.fileSize) {
      return buffer
    }

    await this._readAsync(this.fileHandle, buffer, 0, this.pageSize, pageStartPos)
    this.cache.set(pageIndex, buffer)
    return buffer
  }

  /**
   * Reads data from a specific position in the file.
   * @param offset Start position
   * @param length Length of data to read
   * @param tx Transaction
   * @returns Read data buffer
   */
  async read(offset: number, length: number, tx: Transaction): Promise<Uint8Array> {
    const startPage = offset >> this.pageShift // 최적화: 비트 시프트 사용
    const endPage = (offset + length - 1) >> this.pageShift
    const result = new Uint8Array(length) // 결과 버퍼

    // 읽어야 할 페이지들 식별
    const pagePromises: Promise<Uint8Array>[] = []
    for (let pageIndex = startPage; pageIndex <= endPage; pageIndex++) {
      pagePromises.push(this._readPage(pageIndex, tx))
    }

    // 모든 페이지 로드 대기 (병렬 처리)
    const pages = await Promise.all(pagePromises)

    let infoOffset = 0
    for (let i = 0; i < pages.length; i++) {
      const pageIndex = startPage + i
      const page = pages[i]

      const pageStartOffset = pageIndex * this.pageSize

      const copyStart = Math.max(0, offset - pageStartOffset)
      const copyEnd = Math.min(this.pageSize, offset + length - pageStartOffset)

      // page.copy(result, infoOffset, copyStart, copyEnd) -> result.set
      result.set(page.subarray(copyStart, copyEnd), infoOffset)
      infoOffset += copyEnd - copyStart
    }

    return result
  }

  /**
   * Appends data to the end of the file.
   * @param buffer Data buffer to append
   * @returns Length of appended data
   */
  async append(buffer: Uint8Array, tx: Transaction): Promise<number> {
    // 캐시를 거치도록 fs.appendFile 대신 write 사용
    // 참고: fs.write는 끝을 넘어서 쓰면 자동으로 파일을 확장함. (테스트로 검증됨)
    return await this.write(this.fileSize, buffer, tx)
  }

  /**
   * Writes data to a specific position in the file.
   * @param offset Start position
   * @param buffer Data buffer to write
   * @returns Length of data written
   */
  async write(offset: number, buffer: Uint8Array, tx: Transaction): Promise<number> {
    const startPage = Math.floor(offset / this.pageSize)
    const endPage = Math.floor((offset + buffer.length - 1) / this.pageSize)

    // 필요한 페이지들 병렬 로드
    const pagePromises: Promise<Uint8Array>[] = []
    for (let pageIndex = startPage; pageIndex <= endPage; pageIndex++) {
      pagePromises.push(this._readPage(pageIndex, tx))
    }

    const pages = await Promise.all(pagePromises)

    // 활성 트랜잭션 등록
    if (!this.activeTransactions.has(tx.id)) {
      this.activeTransactions.set(tx.id, tx)
    }

    let bufferOffset = 0
    for (let i = 0; i < pages.length; i++) {
      const pageIndex = startPage + i
      const page = pages[i]

      // 소유자 등록
      this.dirtyPageOwners.set(pageIndex, tx)

      // 스냅샷 저장 (이미 있으면 무시)
      // 현재 페이지의 상태 백업 (Deep Copy)
      if (!tx.__hasUndoPage(pageIndex)) {
        const snapshot = new Uint8Array(this.pageSize)
        snapshot.set(page)
        tx.__setUndoPage(pageIndex, snapshot)
      }

      const pageStartOffset = pageIndex * this.pageSize
      const writeStart = Math.max(0, offset - pageStartOffset)
      const writeEnd = Math.min(this.pageSize, offset + buffer.length - pageStartOffset)

      // 페이지에 버퍼 내용 덮어쓰기
      page.set(buffer.subarray(bufferOffset, bufferOffset + (writeEnd - writeStart)), writeStart)
      bufferOffset += writeEnd - writeStart

      // [중요] _readPage가 복사본을 반환하므로 수정한 페이지를 캐시에 다시 반영
      this.cache.set(pageIndex, page)

      // 트랜잭션에 Dirty Page 등록 (참조 유지하여 캐시 증발 방지)
      tx.__addDirtyPage(pageIndex, page)
    }

    // 파일 크기 업데이트 (확장된 경우에만)
    const endPosition = offset + buffer.length
    if (endPosition > this.fileSize) {
      this.fileSize = endPosition
    }

    return buffer.length
  }

  /**
   * Closes the file.
   */
  async close(): Promise<void> {
    this.cache.clear()
    if (this.logManager) {
      this.logManager.close()
    }
  }
}
