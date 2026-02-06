import fs from 'node:fs'
import { PageManagerFactory } from './Page'

/**
 * WAL (Write-Ahead Logging) Manager class.
 * Records changes to a log file and manages them to ensure atomicity of the database.
 * Handles commit phases and crash recovery.
 */
export class WALManager {
  private fd: number | null = null
  private readonly walFilePath: string
  private readonly pageSize: number
  private readonly entrySize: number
  private buffer: Uint8Array
  private view: DataView
  private totalWrittenPages: number = 0

  /**
   * Constructor
   * @param walFilePath WAL file path
   * @param pageSize Page size
   */
  constructor(walFilePath: string, pageSize: number) {
    // 페이지 크기는 비트 연산 최적화를 위해 2의 거듭제곱이어야 함
    if ((pageSize & (pageSize - 1)) !== 0) {
      throw new Error('Page size must be a power of 2')
    }

    this.walFilePath = walFilePath
    this.pageSize = pageSize
    this.entrySize = 4 + pageSize
    // 페이지 크기는 고정이므로 항상 동일한 크기의 버퍼를 재사용
    this.buffer = new Uint8Array(this.entrySize)
    this.view = new DataView(this.buffer.buffer)
  }

  /**
   * Opens the log file.
   */
  open(): void {
    // 'a+': 읽기 및 추가 모드로 열기. 파일이 없으면 생성.
    // 쓰기는 항상 파일 끝에 추가됨 (atomic append).
    this.fd = fs.openSync(this.walFilePath, 'a+')
  }

  // ─────────────────────────────────────────────────────────────
  // High-level WAL operations (2-Phase Commit)
  // ─────────────────────────────────────────────────────────────

  /**
   * Performs recovery (Redo) using WAL logs.
   * Called during initialization, ensuring data is fully restored before operations start.
   * @param writePage Callback to write recovered pages to disk
   */
  async recover(writePage: (pageId: number, data: Uint8Array) => Promise<void>): Promise<void> {
    this.open()
    const restoredPages = this.readAllSync()

    if (restoredPages.size === 0) {
      return
    }

    // 복구된 페이지들을 즉시 디스크(본 파일)에 기록 (Checkpoint)
    const promises: Promise<void>[] = []
    for (const [pageId, data] of restoredPages) {
      // [안전 장치] 손상된 WAL 데이터로 인해 거대한 스파스 파일이 생성되는 것을 방지
      if (pageId > 1000000) continue

      // Checksum verification
      try {
        const manager = new PageManagerFactory().getManager(data)
        if (!manager.verifyChecksum(data)) {
          console.warn(`[WALManager] Checksum verification failed for PageID ${pageId} during recovery. Ignoring changes.`)
          continue
        }
      } catch (e) {
        console.warn(`[WALManager] Failed to verify checksum for PageID ${pageId} during recovery: ${e}. Ignoring changes.`)
        continue
      }

      // 디스크에 기록
      promises.push(writePage(pageId, data))
    }

    // 복구 작업 완료 대기 및 로그 비우기
    await Promise.all(promises)
    if (restoredPages.size > 0) {
      await this.clear()
    }
  }

  /**
   * WAL에 페이지 데이터를 기록합니다 (Phase 1: Prepare).
   * @param dirtyPages 변경된 페이지들 (pageId -> data)
   */
  async prepareCommit(dirtyPages: Map<number, Uint8Array>): Promise<void> {
    if (dirtyPages.size === 0) {
      return
    }
    await this.append(dirtyPages)
  }

  /**
   * WAL에 커밋 마커를 기록하고 로그를 정리합니다 (Phase 2: Finalize).
   * @param hasActiveTransactions 아직 활성 트랜잭션이 있는지 여부
   */
  async finalizeCommit(hasActiveTransactions: boolean): Promise<void> {
    // Commit Marker 기록
    await this.writeCommitMarker()

    // 활성 트랜잭션이 없으면 로그 비우기
    if (!hasActiveTransactions) {
      await this.clear()
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Low-level WAL operations
  // ─────────────────────────────────────────────────────────────

  /**
   * Appends changed pages to the log file.
   * Records them sorted by page ID.
   * @param pages Map of changed pages (Page ID -> Data)
   */
  async append(pages: Map<number, Uint8Array>): Promise<void> {
    if (this.fd === null) {
      this.open()
    }

    // 페이지 아이디 순으로 정렬 (순차적 기록을 위함)
    const sortedPageIds = Array.from(pages.keys()).sort((a, b) => a - b)

    for (const pageId of sortedPageIds) {
      const data = pages.get(pageId)!

      // 1. 버퍼에 데이터 채우기 (PageID + Data)
      this.view.setUint32(0, pageId, true)
      // 데이터 복사 (offset 4부터)
      this.buffer.set(data, 4)

      // 2. 파일에 쓰기 (OS 캐시)
      // a+ 모드이므로 position에 null을 전달하여 자동으로 끝에 추가되도록 함
      await new Promise<void>((resolve, reject) => {
        fs.write(this.fd!, this.buffer, 0, this.entrySize, null, (err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }

    // 3. 디스크 동기화 (배치 작업 당 1회)
    await new Promise<void>((resolve, reject) => {
      fs.fsync(this.fd!, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  /**
   * Writes a commit marker to the log file.
   * This indicates that the preceding logs are part of a committed transaction.
   */
  async writeCommitMarker(): Promise<void> {
    if (this.fd === null) {
      this.open()
    }

    // Commit Marker: PageID = 0xFFFFFFFF (4294967295)
    // Data = 0 (Empty) or Checksum/TxID
    this.view.setUint32(0, 0xFFFFFFFF, true)

    // 마커 뒤에 더미 데이터나 메타데이터를 채울 수 있음. 
    // 여기서는 0으로 채움.
    this.buffer.fill(0, 4)

    await new Promise<void>((resolve, reject) => {
      fs.write(this.fd!, this.buffer, 0, this.entrySize, null, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })

    await new Promise<void>((resolve, reject) => {
      fs.fsync(this.fd!, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  /**
   * Reads the log file to recover the page map.
   * Runs synchronously as it is called during initialization.
   * Only returns pages from committed transactions (ended with a commit marker).
   * @returns Recovered page map
   */
  readAllSync(): Map<number, Uint8Array> {
    if (this.fd === null) {
      this.open()
    }

    const restoredPages = new Map<number, Uint8Array>()
    // 현재 파일 크기 확인
    const currentFileSize = fs.fstatSync(this.fd!).size
    let offset = 0

    // 트랜잭션 단위로 버퍼링
    // 커밋 마커를 만나면 pendingPages를 restoredPages에 반영
    let pendingPages = new Map<number, Uint8Array>()

    // 엔트리 단위로 반복해서 읽기
    while (offset + this.entrySize <= currentFileSize) {
      fs.readSync(this.fd!, this.buffer, 0, this.entrySize, offset)

      const pageId = this.view.getUint32(0, true)

      if (pageId === 0xFFFFFFFF) {
        // 커밋 마커 발견: 펜딩된 페이지들을 확정
        for (const [pId, pData] of pendingPages) {
          restoredPages.set(pId, pData)
        }
        pendingPages.clear()
      } else {
        // 일반 페이지 로그: 펜딩 버퍼에 추가
        const pageData = this.buffer.slice(4, 4 + this.pageSize)
        // 같은 페이지가 여러 번 수정되었을 수 있으므로 덮어씀 (최신본 유지)
        // 하지만 Uint8Array.slice()는 새로운 버퍼를 생성하므로 안전함.
        pendingPages.set(pageId, pageData)
      }

      offset += this.entrySize
    }

    // 루프가 끝난 후 pendingPages에 남아있는 데이터는 커밋 마커가 없는(쓰다 만) 트랜잭션이므로 버림.

    return restoredPages
  }

  /**
   * Increments the total written pages count.
   * @param count Number of pages written
   */
  incrementWrittenPages(count: number): void {
    this.totalWrittenPages += count
  }

  /**
   * Returns whether a checkpoint should be performed.
   * @param threshold Threshold (number of pages)
   * @returns Whether a checkpoint should be performed
   */
  shouldCheckpoint(threshold: number): boolean {
    return this.totalWrittenPages >= threshold
  }

  /**
   * Initializes (clears) the log file.
   * Should be called after a checkpoint.
   */
  async clear(): Promise<void> {
    // Windows에서 a+ 모드로 열린 파일에 대해 ftruncate가 EPERM 에러를 발생시킬 수 있음.
    // 안전하게 파일을 닫고 경로 기반으로 Truncate 수행.
    if (this.fd !== null) {
      fs.closeSync(this.fd)
      this.fd = null
    }

    return new Promise((resolve, reject) => {
      // 파일 크기를 0으로 만듦
      fs.truncate(this.walFilePath, 0, (err) => {
        if (err) return reject(err)
        this.totalWrittenPages = 0
        resolve()
      })
    })
  }

  /**
   * Cleans up resources.
   */
  close(): void {
    if (this.fd !== null) {
      fs.closeSync(this.fd)
      this.fd = null
    }
  }
}

