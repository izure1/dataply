import fs from 'node:fs'
import { LogManager } from './LogManager'
import { PageManagerFactory } from './Page'

/**
 * Virtual File System class.
 * WAL(Write-Ahead Logging)과 복구를 담당합니다.
 * 
 * mvcc-api 기반 리팩토링 후:
 * - MVCC 스냅샷 로직은 PageMVCCStrategy와 mvcc-api가 담당
 * - VFS는 WAL 관리와 복구에만 집중
 */
export class VirtualFileSystem {
  /** Track logical file size */
  protected fileSize: number
  /** Page size */
  protected pageSize: number
  /** File handle */
  protected fileHandle: number
  /** WAL Log Manager */
  protected logManager?: LogManager

  constructor(
    fileHandle: number,
    pageSize: number,
    pageCacheCapacity: number,
    walPath?: string | undefined | null
  ) {
    // 페이지 크기는 비트 연산 최적화를 위해 2의 거듭제곱이어야 함
    if ((pageSize & (pageSize - 1)) !== 0) {
      throw new Error('Page size must be a power of 2')
    }

    this.fileHandle = fileHandle
    this.pageSize = pageSize
    this.fileSize = fs.fstatSync(fileHandle).size

    // WAL 경로가 제공된 경우에만 LogManager 초기화
    if (walPath) {
      this.logManager = new LogManager(walPath, pageSize)
    }
  }

  /**
   * Performs recovery (Redo) using WAL logs.
   * Called during initialization (DataplyAPI.init), ensuring data is fully restored before operations start.
   */
  async recover(): Promise<void> {
    if (!this.logManager) return

    this.logManager!.open()
    const restoredPages = this.logManager!.readAllSync()

    if (restoredPages.size === 0) {
      return
    }

    // 복구된 페이지들을 즉시 디스크(본 파일)에 기록 (Checkpoint)
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

    // 복구 작업 완료 대기 및 로그 비우기
    await Promise.all(promises)
    if (this.logManager && restoredPages.size > 0) {
      await this.logManager.clear()
    }
  }

  /**
   * WAL에 페이지 데이터를 기록합니다 (Phase 1).
   * @param dirtyPages 변경된 페이지들 (pageId -> data)
   */
  async prepareCommitWAL(dirtyPages: Map<number, Uint8Array>): Promise<void> {
    if (!this.logManager || dirtyPages.size === 0) {
      return
    }
    await this.logManager.append(dirtyPages)
  }

  /**
   * WAL에 커밋 마커를 기록하고 로그를 정리합니다 (Phase 2).
   * @param hasActiveTransactions 아직 활성 트랜잭션이 있는지 여부
   */
  async finalizeCommitWAL(hasActiveTransactions: boolean): Promise<void> {
    if (!this.logManager) {
      return
    }

    // Commit Marker 기록
    await this.logManager.writeCommitMarker()

    // 활성 트랜잭션이 없으면 로그 비우기
    if (!hasActiveTransactions) {
      await this.logManager.clear()
    }
  }

  /**
   * 디스크에서 페이지를 읽습니다.
   * @param pageId Page ID
   * @returns Page data
   */
  async readPage(pageId: number): Promise<Uint8Array> {
    const buffer = new Uint8Array(this.pageSize)
    const pageStartPos = pageId * this.pageSize

    if (pageStartPos >= this.fileSize) {
      return buffer
    }

    await this._readAsync(this.fileHandle, buffer, 0, this.pageSize, pageStartPos)
    return buffer
  }

  /**
   * 디스크에 페이지를 씁니다.
   * @param pageId Page ID
   * @param data Page data
   */
  async writePage(pageId: number, data: Uint8Array): Promise<void> {
    const pageStartPos = pageId * this.pageSize

    // 안전 장치: 512MB 제한
    if (pageStartPos + this.pageSize > 512 * 1024 * 1024) {
      throw new Error(`[Safety Limit] File write exceeds 512MB limit at position ${pageStartPos}`)
    }

    await this._writeAsync(this.fileHandle, data, 0, this.pageSize, pageStartPos)

    // 파일 크기 업데이트
    const endPosition = pageStartPos + this.pageSize
    if (endPosition > this.fileSize) {
      this.fileSize = endPosition
    }
  }

  /**
   * 현재 파일 크기 반환
   */
  getFileSize(): number {
    return this.fileSize
  }

  /**
   * Closes the file.
   */
  async close(): Promise<void> {
    if (this.logManager) {
      this.logManager.close()
    }
  }

  protected _readAsync(handle: number, buffer: Uint8Array, offset: number, length: number, position: number): Promise<number> {
    return new Promise((resolve, reject) => {
      fs.read(handle, buffer, offset, length, position, (err, bytesRead) => {
        if (err) return reject(err)
        resolve(bytesRead)
      })
    })
  }

  protected _writeAsync(handle: number, buffer: Uint8Array, offset: number, length: number, position: number): Promise<number> {
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
}
