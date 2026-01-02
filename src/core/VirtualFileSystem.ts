import fs from 'node:fs'
import { LogManager } from './LogManager'
import type { Transaction } from './transaction/Transaction'
import { PageManagerFactory } from './Page'

/**
 * Virtual File System class that manages and caches files in page units.
 */
export class VirtualFileSystem {
  /** Cache list (Page ID -> Data Buffer) */
  protected cache: Map<number, Uint8Array> = new Map()
  /** Page IDs that have changes and need disk synchronization */
  protected dirtyPages: Set<number> = new Set()
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

  constructor(protected fileHandle: number, protected pageSize: number, walPath?: string | undefined | null) {
    // 페이지 크기는 비트 연산 최적화를 위해 2의 거듭제곱이어야 함
    if ((pageSize & (pageSize - 1)) !== 0) {
      throw new Error('Page size must be a power of 2')
    }
    this.pageShift = Math.log2(pageSize)
    this.pageMask = pageSize - 1

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

    // 복구된 페이지들을 캐시에 반영하고 dirty로 표시
    for (const [pageId, data] of restoredPages) {
      // [안전 장치] 손상된 WAL 데이터로 인해 거대한 스파스 파일이 생성되는 것을 방지
      // PageID가 비정상적으로 큰 경우(예: 1GB 이상의 오프셋) 무시
      if (pageId > 1000000) {
        console.warn(`[VFS] Ignoring suspicious PageID ${pageId} during recovery.`)
        continue
      }

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
      this.dirtyPages.add(pageId)

      // 복구된 페이지가 기존 파일 범위를 벗어나면 파일 크기 업데이트
      const endPos = (pageId + 1) * this.pageSize
      if (endPos > this.fileSize) {
        this.fileSize = endPos
      }
    }

    // 주의: 생성자 내에서 실행되므로 비동기 sync/clear를 호출하지 않음.
    // WAL 파일은 그대로 유지되며 이후 트랜잭션 커밋이나 종료 시 처리됨.
    // 이는 멱등성(Idempotent)이 보장되므로 안전함.
  }

  /**
   * Commits the transaction.
   * @param tx Transaction
   */
  async commit(tx: Transaction): Promise<void> {
    const dirtyPages = tx.__getDirtyPages()
    if (dirtyPages.size === 0) {
      this.cleanupTransaction(tx)
      return
    }

    // 1. 변경된 페이지들을 WAL에 기록
    const dirtyPageMap = new Map<number, Uint8Array>()
    for (const pageId of dirtyPages) {
      const page = this.cache.get(pageId)
      if (page) {
        dirtyPageMap.set(pageId, page)
      }
    }

    if (this.logManager && dirtyPageMap.size > 0) {
      // WAL에 기록 (원자적)
      await this.logManager.append(dirtyPageMap)
    }

    // 2. 디스크 동기화 (Checkpoint) - 해당 트랜잭션의 페이지만 반영
    // 최적화를 위해 정렬
    const sortedPages = Array.from(dirtyPages).sort((a, b) => a - b)
    const promises: Promise<number>[] = []

    for (const pageId of sortedPages) {
      const page = this.cache.get(pageId)
      if (page) {
        promises.push(this._writeAsync(
          this.fileHandle,
          page,
          0,
          this.pageSize,
          pageId * this.pageSize
        ))
      }
    }

    await Promise.all(promises)

    // 3. 로그 비우기
    // 주의: 다중 트랜잭션 환경에서는 다른 트랜잭션의 로그가 섞여 있을 수 있음.
    // 하지만 현재 구조는 커밋 시 즉시 동기화를 수행하므로 (해당 tx에 대해) WAL은 비워도 되는 상태가 됨.
    // 다른 트랜잭션의 WAL 로그가 섞여 있다면? 
    // LogManager.append는 파일 끝에 추가함.
    // LogManager.clear()는 파일을 비움.
    // 만약 Tx A 커밋 중 Tx B가 로그를 썼다면? -> LockManager가 페이지 단위 락을 걸지만 WAL 파일 자체는?
    // LogManager가 내부적으로 파일 락을 쓰거나 Append 시 순서를 보장해야 함.
    // 일단 현재는 clear()를 함부로 하면 위험할 수 있음 (다른 트랜잭션의 Redo 로그 유실 가능).
    // *해결책*: WAL은 Crash Recovery용이므로, 동기화가 완료된 데이터에 대한 로그는 더 이상 필요 없음.
    // 다만 아직 동기화되지 않은 다른 활성 트랜잭션의 로그는 지우면 안 됨.
    // 따라서 활성 트랜잭션이 하나도 없을 때만 비우거나, 체크포인트 메커니즘이 필요함.
    // 간단한 타협: 활성 트랜잭션 수가 0일 때 비우기.
    // 하지만 파일이 무한히 커지는 것을 막아야 함.
    // -> 커밋 시마다 동기화를 하므로, WAL은 "현재 진행 중인 트랜잭션들"의 로그만 관리하면 됨.
    // Tx A 커밋 -> 동기화 A -> A의 로그 필요 없음.
    // Tx B 활성 -> B의 로그 필요함.
    // B의 로그가 A 뒤에 있다면? A 로그만 지울 수 있나?
    // WAL 파일의 앞부분을 잘라내는 Truncate 기능이 필요함.
    // 현재는 복잡성을 낮추기 위해 '활성 트랜잭션 수 0'일 때 비우는 것으로 타협.

    if (this.activeTransactions.size <= 1) { // 본인(self)만 남은 경우 (곧 0이 됨)
      if (this.logManager) {
        await this.logManager.clear()
      }
    }

    this.cleanupTransaction(tx)
  }

  /**
   * Rolls back the transaction.
   * @param tx Transaction
   */
  async rollback(tx: Transaction): Promise<void> {
    const dirtyPages = tx.__getDirtyPages()

    // Undo 버퍼를 사용하여 이전 상태로 복구
    for (const pageId of dirtyPages) {
      const undoData = tx.__getUndoPage(pageId)
      if (undoData) {
        this.cache.set(pageId, undoData)
      }
    }

    // 새로 생성된 페이지 처리 (dirtyPages에는 있지만 undoBuffer에는 없는 경우 = 신규 할당?)
    // 신규 할당된 페이지는 undoBuffer에 '빈 페이지' 혹은 '이전 상태 없음'을 기록해야 함.
    // VFS 레벨에서 파일 크기를 줄이기는 까다로움. 
    // Undo로 페이지 내용은 복구되지만, 파일 크기가 늘어난 것은 그대로 유지될 수 있음 (Sparse).
    // 파일 크기 축소: ftruncate? 
    // 트랜잭션이 파일 크기를 늘렸는지 여부를 추적해야 함.
    // 여기서는 단순성을 위해 파일 크기 축소는 생략함 (내용은 복구됨).

    this.cleanupTransaction(tx)
  }

  private cleanupTransaction(tx: Transaction) {
    // Dirty Page 소유권 해제
    const pages = tx.__getDirtyPages()
    for (const pageId of pages) {
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
    // [안전 장치] 파일 크기 폭발 방지 (100MB 제한)
    if (position + length > 100 * 1024 * 1024) {
      return Promise.reject(new Error(`[Safety Limit] File write exceeds 100MB limit at position ${position}`))
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
    // [MVCC] 다른 트랜잭션이 수정한 페이지(Dirty)인지 확인
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

      tx.__addDirtyPage(pageIndex)
    }

    // 파일 크기 업데이트 (확장된 경우에만)
    const endPosition = offset + buffer.length
    if (endPosition > this.fileSize) {
      this.fileSize = endPosition
    }

    return buffer.length
  }

  /**
   * Synchronizes dirty pages to disk.
   */
  async sync(): Promise<void> {
    const promises: Promise<number>[] = []

    // 디스크 액세스 최적화를 위해 페이지 인덱스 정렬
    const sortedPages = Array.from(this.dirtyPages).sort((a, b) => a - b)

    for (const pageIndex of sortedPages) {
      const page = this.cache.get(pageIndex)
      if (page) {
        promises.push(this._writeAsync(
          this.fileHandle,
          page,
          0,
          this.pageSize,
          pageIndex * this.pageSize
        ))
      }
    }

    await Promise.all(promises)
    this.dirtyPages.clear()
  }

  /**
   * Closes the file.
   */
  async close(): Promise<void> {
    await this.sync()
    this.dirtyPages.clear()
    this.cache.clear()
    if (this.logManager) {
      this.logManager.close()
    }
  }
}
