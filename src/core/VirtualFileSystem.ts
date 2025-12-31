import fs from 'node:fs'
import { LogManager } from './LogManager'
import type { Transaction } from './transaction/Transaction'

/**
 * 페이지 단위로 파일을 관리하고 캐싱하는 가상 파일 시스템 클래스
 */
export class VirtualFileSystem {
  /** 캐시 리스트 (페이지 번호 -> 데이터 버퍼) */
  protected cache: Map<number, Uint8Array> = new Map()
  /** 변경사항이 있어 디스크 동기화가 필요한 페이지 번호들 */
  protected dirtyPages: Set<number> = new Set()
  /** 논리적 파일 크기 추적 */
  protected fileSize: number
  /** 페이지 크기의 비트 시프트 값 */
  protected pageShift: number
  /** 페이지 크기의 비트 마스크 값 */
  protected pageMask: number

  // Transaction Support
  protected logManager?: LogManager
  // PageID -> Owner Transaction
  protected dirtyPageOwners: Map<number, Transaction> = new Map()
  // TxID -> Transaction (Active Transactions)
  protected activeTransactions: Map<number, Transaction> = new Map()

  constructor(protected fileHandle: number, protected pageSize: number, walPath?: string | undefined | null) {
    // 페이지 크기는 2의 제곱수여야 비트 연산 최적화가 가능함
    if ((pageSize & (pageSize - 1)) !== 0) {
      throw new Error('Page size must be a power of 2')
    }
    this.pageShift = Math.log2(pageSize)
    this.pageMask = pageSize - 1

    // 중요: 초기 파일 크기 로드
    this.fileSize = fs.fstatSync(fileHandle).size

    // WAL 파일 경로가 제공된 경우에만 LogManager 초기화 및 복구 수행
    if (walPath) {
      this.logManager = new LogManager(walPath, pageSize)
      this.recover()
    }
  }

  /**
   * WAL 로그를 이용하여 복구(Redo)를 수행합니다.
   * 생성자에서 호출되므로 동기적으로 처리하며, 데이터는 캐시에만 반영합니다.
   * 실제 디스크 반영(Sync)과 로그 비우기는 이후 트랜잭션이나 종료 시 수행됩니다.
   */
  private recover() {
    if (!this.logManager) return

    this.logManager!.open()
    const restoredPages = this.logManager!.readAllSync()

    if (restoredPages.size === 0) {
      return
    }

    // 복구된 페이지들을 캐시에 적용하고 dirty로 표시
    for (const [pageId, data] of restoredPages) {
      // [Safety Fix] WAL 데이터 오염으로 인한 초대형 파일(Sparse File) 생성 방지
      // PageID가 비정상적으로 크다면 무시 (예: 1GB 이상 오프셋)
      if (pageId > 1000000) {
        console.warn(`[VFS] Ignoring suspicious PageID ${pageId} during recovery.`)
        continue
      }

      this.cache.set(pageId, data)
      this.dirtyPages.add(pageId)

      // 파일 사이즈 갱신 (복구된 페이지가 기존 파일 범위를 넘어서면)
      const endPos = (pageId + 1) * this.pageSize
      if (endPos > this.fileSize) {
        this.fileSize = endPos
      }
    }

    // 주의: 생성자이므로 비동기 sync/clear를 호출하지 않음.
    // WAL 파일은 그대로 유지되며, 나중에 commit이나 close 호출 시 처리됨.
    // 이는 Idempotent하므로 안전함.
  }

  /**
   * 트랜잭션을 커밋합니다.
   * @param tx 트랜잭션
   */
  async commit(tx: Transaction): Promise<void> {
    const dirtyPages = tx.getDirtyPages()
    if (!dirtyPages || dirtyPages.size === 0) {
      this.cleanupTransaction(tx)
      return
    }

    // 1. 변경된 페이지(Dirty Pages)를 WAL에 기록
    const dirtyPageMap = new Map<number, Uint8Array>()
    for (const pageId of dirtyPages) {
      const page = this.cache.get(pageId)
      if (page) {
        dirtyPageMap.set(pageId, page)
      }
    }

    if (this.logManager && dirtyPageMap.size > 0) {
      // WAL에 기록 (Atomic하게)
      await this.logManager.append(dirtyPageMap)
    }

    // 2. 디스크 동기화 (Checkpoint) - 해당 트랜잭션의 페이지만
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
    // 주의: 멀티 트랜잭션 환경에서는 다른 트랜잭션의 로그도 있을 수 있으므로 신중해야 함.
    // 하지만 현재 구조에서는 commit 시 바로 sync하므로 WAL은 항상 비워도 되는 상태가 됨 (해당 tx에 한해).
    // 만약 다른 트랜잭션의 WAL 로그가 섞여있다면?
    // LogManager.append는 파일 끝에 추가함.
    // LogManager.clear()는 파일을 비움.
    // 만약 Tx A 커밋 중에 Tx B가 로그를 썼다면? -> LockManager가 페이지 단위 락을 걸고 있지만, WAL 파일 자체에 대한 락은?
    // LogManager가 내부적으로 append 시 파일 락을 쓰거나, append 순서가 보장되어야 함.
    // 일단 여기서는 clear()를 호출하면 안될 수도 있음 (다른 tx의 redo log가 날아감).
    // *해결책*: WAL은 Crash Recovery용이므로, Sync가 완료된 데이터에 대한 로그는 필요 없음.
    // 하지만 Sync되지 않은 다른 Tx의 로그가 있다면 지우면 안됨.
    // 따라서, 모든 Active Transaction이 없을 때만 Clear 하거나, Checkpoint 매커니즘 필요.
    // 간단하게: 여기서 clear() 하지 않음. (나중에 vacuum 하거나, 시작 시 recover 후 clear 하므로).
    // 하지만 파일이 무한정 커지는 것 방지 필요.
    // -> 일단은 Commit 시 Sync하므로, WAL은 "현재 진행중인 트랜잭션"들의 로그만 중요함.
    // Tx A Commit -> Sync A -> A의 로그 필요 없음.
    // Tx B Active -> B의 로그 필요함.
    // B의 로그가 A보다 뒤에 있다면? A 로그 지워도 됨?
    // WAL 파일 앞부분을 잘라내는 Truncate 기능이 필요.
    // 여기서는 복잡도를 낮추기 위해 'active transaction count === 0' 일 때 clear 하는 식으로 타협 가능.

    if (this.activeTransactions.size <= 1) { // 나 자신만 남았을 때 (이제 0이 될 예정)
      if (this.logManager) {
        await this.logManager.clear()
      }
    }

    this.cleanupTransaction(tx)
  }

  /**
   * 트랜잭션을 롤백합니다.
   * @param tx 트랜잭션
   */
  async rollback(tx: Transaction): Promise<void> {
    const dirtyPages = tx.getDirtyPages()

    if (dirtyPages) {
      // Undo Buffer를 통해 이전 상태로 복구
      for (const pageId of dirtyPages) {
        const undoData = tx.getUndoPage(pageId)
        if (undoData) {
          const restored = new Uint8Array(this.pageSize)
          restored.set(undoData)
          this.cache.set(pageId, restored)
        }
      }
    }

    // 새로 생성된 페이지 처리 (dirtyPages 에는 있지만 undoBuffer에는 없는 페이지 = 신규 할당?)
    // 신규 할당된 페이지는 undoBuffer에 '빈 페이지' 혹은 '이전 상태 없음'으로 기록되어야 할까?
    // VFS 레벨에서는 '파일 크기'를 줄이는 것이 까다로움.
    // 페이지 내용은 Undo로 복구되지만, 파일 크기가 늘어난 것은 줄어들지 않을 수 있음 (Sparse).
    // 파일 크기 줄이기: ftruncate?
    // 트랜잭션이 파일 크기를 늘렸는지 추적 필요. 
    // 여기서는 복잡도상 파일 크기 축소는 생략 (내용은 복구됨).

    this.cleanupTransaction(tx)
  }

  private cleanupTransaction(tx: Transaction) {
    // Dirty Page Owner 정리
    const pages = tx.getDirtyPages()
    if (pages) {
      for (const pageId of pages) {
        this.dirtyPageOwners.delete(pageId)
      }
    }
    this.activeTransactions.delete(tx.id)
  }

  /**
   * 파일의 특정 위치에서 데이터를 읽어옵니다.
   * @param handle 파일 핸들
   * @param buffer 읽을 데이터 버퍼
   * @param offset 읽기 시작 위치
   * @param length 읽을 데이터의 길이
   * @param position 읽기 시작 위치
   * @returns 읽은 데이터의 길이
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
   * 파일의 특정 위치에 데이터를 씁니다.
   * @param handle 파일 핸들
   * @param buffer 쓰일 데이터 버퍼
   * @param offset 쓰기 시작 위치
   * @param length 쓰일 데이터의 길이
   * @param position 쓰기 시작 위치
   */
  protected _writeAsync(handle: number, buffer: Uint8Array, offset: number, length: number, position: number): Promise<number> {
    // [Safety Fix] 파일 크기 폭증 방지 (100MB 제한)
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
   * 파일의 끝에 데이터를 추가합니다.
   * @param buffer 추가할 데이터 버퍼
   */
  protected _appendAsync(handle: number, buffer: Uint8Array): Promise<void> {
    // [Safety Fix] 파일 크기 폭증 방지 (100MB 제한)
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

  protected async _readPage(pageIndex: number, tx?: Transaction): Promise<Uint8Array> {
    // [MVCC] Check if this page is dirty by another transaction
    if (this.activeTransactions.size > 0) {
      const ownerTx = this.dirtyPageOwners.get(pageIndex)
      if (ownerTx) {
        // If I am the owner, I see the dirty version (Cache)
        if (tx && tx.id === ownerTx.id) {
          // pass (read cache)
        } else {
          // Someone else owns it -> read Undo (Snapshot)
          const undoPage = ownerTx.getUndoPage(pageIndex)
          if (undoPage) {
            const snapshot = new Uint8Array(this.pageSize)
            snapshot.set(undoPage)
            return snapshot
          }
        }
      }
    }

    // Normal Read (Cache or Disk)
    if (this.cache.has(pageIndex)) {
      return this.cache.get(pageIndex)!.slice()
    }

    const buffer = new Uint8Array(this.pageSize)
    const pageStartPos = pageIndex * this.pageSize
    if (pageStartPos >= this.fileSize) {
      return buffer
    }

    await this._readAsync(this.fileHandle, buffer, 0, this.pageSize, pageStartPos)
    this.cache.set(pageIndex, buffer)
    return buffer.slice()
  }

  /**
   * 파일의 특정 위치에서 데이터를 읽어옵니다.
   * @param offset 읽기 시작 위치
   * @param length 읽을 데이터의 길이
   * @param tx 트랜잭션 (MVCC용)
   * @returns 읽은 데이터 버퍼
   */
  async read(offset: number, length: number, tx?: Transaction): Promise<Uint8Array> {
    const startPage = offset >> this.pageShift // 최적화: 비트 시프트 사용
    const endPage = (offset + length - 1) >> this.pageShift
    const result = new Uint8Array(length) // 결과 버퍼

    // 읽어야 할 페이지들을 파악
    const pagePromises: Promise<Uint8Array>[] = []
    for (let pageIndex = startPage; pageIndex <= endPage; pageIndex++) {
      pagePromises.push(this._readPage(pageIndex, tx))
    }

    // 모든 페이지 로딩까지 대기 (병렬 처리)
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
   * 파일의 끝에 데이터를 추가합니다.
   * @param buffer 추가할 데이터 버퍼
   * @returns 추가된 데이터의 길이
   */
  async append(buffer: Uint8Array, tx?: Transaction): Promise<number> {
    // fs.appendFile 대신 write를 사용하여 캐시를 경유하도록 함
    // 주의: fs.write는 파일 끝을 넘어서 쓸 경우 자동으로 파일을 확장해줍니다. (테스트 검증 완료)
    return await this.write(this.fileSize, buffer, tx)
  }

  /**
   * 파일의 특정 위치에 데이터를 씁니다.
   * @param offset 쓰기 시작 위치
   * @param buffer 쓰일 데이터 버퍼
   * @returns 쓰인 데이터의 길이
   */
  async write(offset: number, buffer: Uint8Array, tx?: Transaction): Promise<number> {
    const startPage = Math.floor(offset / this.pageSize)
    const endPage = Math.floor((offset + buffer.length - 1) / this.pageSize)

    // 필요한 페이지들을 병렬로 로드
    const pagePromises: Promise<Uint8Array>[] = []
    for (let pageIndex = startPage; pageIndex <= endPage; pageIndex++) {
      pagePromises.push(this._readPage(pageIndex, tx))
    }

    const pages = await Promise.all(pagePromises)

    let bufferOffset = 0
    for (let i = 0; i < pages.length; i++) {
      const pageIndex = startPage + i
      const page = pages[i]

      // [UndoLog] 트랜잭션 중이라면 변경 전 상태 백업
      // Auto-commit(tx 없음)일 경우 백업 불필요
      // [UndoLog] 트랜잭션 중이라면 변경 전 상태 백업
      // Auto-commit(tx 없음)일 경우 백업 불필요
      if (tx) {
        // Active Transaction 등록
        if (!this.activeTransactions.has(tx.id)) {
          this.activeTransactions.set(tx.id, tx)
        }

        // Owner 등록
        this.dirtyPageOwners.set(pageIndex, tx)

        // Snapshot 저장 (이미 있으면 덮어쓰지 않음 - Transaction 객체가 처리)
        // 현재 페이지 상태 백업 (Deep Copy)
        const snapshot = new Uint8Array(this.pageSize)
        snapshot.set(page)
        tx.addUndoPage(pageIndex, snapshot)
      }

      const pageStartOffset = pageIndex * this.pageSize
      const writeStart = Math.max(0, offset - pageStartOffset)
      const writeEnd = Math.min(this.pageSize, offset + buffer.length - pageStartOffset)

      // 버퍼 내용을 페이지에 덮어씀
      page.set(buffer.subarray(bufferOffset, bufferOffset + (writeEnd - writeStart)), writeStart)
      bufferOffset += writeEnd - writeStart

      // [Important] _readPage가 복사본을 반환하므로, 수정된 페이지를 캐시에 다시 반영해야 함
      this.cache.set(pageIndex, page)

      if (tx) {
        tx.addDirtyPage(pageIndex)
      } else {
        // Auto-commit: 즉시 디스크 반영
        // 단순히 dirtyPages에 넣고 sync 호출
        this.dirtyPages.add(pageIndex)
        // 성능상 비효율적일 수 있으나 Auto-commit 안전성 보장
        await this._writeAsync(this.fileHandle, page, 0, this.pageSize, pageIndex * this.pageSize)
        this.dirtyPages.delete(pageIndex)
      }
    }

    // 파일 크기 갱신 (더 큰 경우에만 늘어남)
    const endPosition = offset + buffer.length
    if (endPosition > this.fileSize) {
      this.fileSize = endPosition
    }

    return buffer.length
  }

  /**
   * 변경된 페이지를 디스크에 동기화합니다.
   */
  async sync(): Promise<void> {
    const promises: Promise<number>[] = []

    // 디스크 접근 최적화를 위해 페이지 인덱스 순서대로 정렬하여 쓰기 수행
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
   * 파일을 닫습니다.
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
