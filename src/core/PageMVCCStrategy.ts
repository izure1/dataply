import fs from 'node:fs'
import { AsyncMVCCStrategy } from 'mvcc-api'

/**
 * 페이지 수준 MVCC Strategy.
 * mvcc-api의 AsyncMVCCStrategy를 상속하여 디스크 I/O를 담당합니다.
 * 캐시 및 버퍼 관리는 mvcc-api의 AsyncMVCCTransaction이 담당합니다.
 * 
 * 키: 페이지 ID (number)
 * 값: 페이지 데이터 (Uint8Array)
 */
export class PageMVCCStrategy extends AsyncMVCCStrategy<number, Uint8Array> {
  /** 파일 크기 (논리적) */
  private fileSize: number

  constructor(
    private readonly fileHandle: number,
    private readonly pageSize: number,
  ) {
    super()
    this.fileSize = fs.fstatSync(fileHandle).size
  }

  /**
   * 디스크에서 페이지를 읽습니다.
   * @param pageId 페이지 ID
   * @returns 페이지 데이터
   */
  async read(pageId: number): Promise<Uint8Array> {
    const buffer = new Uint8Array(this.pageSize)
    const pageStartPos = pageId * this.pageSize

    // 파일 범위를 벗어나면 빈 버퍼 반환
    if (pageStartPos >= this.fileSize) {
      return buffer
    }

    await this._readFromDisk(buffer, pageStartPos)
    return buffer
  }

  /**
   * 디스크에 페이지를 씁니다.
   * @param pageId 페이지 ID
   * @param data 페이지 데이터
   */
  async write(pageId: number, data: Uint8Array): Promise<void> {
    const pageStartPos = pageId * this.pageSize
    await this._writeToDisk(data, pageStartPos)

    // 파일 크기 업데이트 (논리적 크기)
    const endPosition = pageStartPos + this.pageSize
    if (endPosition > this.fileSize) {
      this.fileSize = endPosition
    }
  }

  /**
   * 페이지 삭제.
   * 실제 페이지 해제는 상위 레이어(FreeList)에서 관리합니다.
   * @param pageId 페이지 ID
   */
  async delete(pageId: number): Promise<void> {
    // No-op: 실제 페이지 해제는 FreeList에서 관리
  }

  /**
   * 페이지 존재 여부 확인
   * @param pageId 페이지 ID
   * @returns 존재하면 true
   */
  async exists(pageId: number): Promise<boolean> {
    const pageStartPos = pageId * this.pageSize
    return pageStartPos < this.fileSize
  }

  /**
   * 메인 DB 파일의 물리적 동기화를 수행합니다 (fsync).
   */
  async sync(): Promise<void> {
    return new Promise((resolve, reject) => {
      fs.fsync(this.fileHandle, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  /**
   * 현재 파일 크기 반환
   */
  getFileSize(): number {
    return this.fileSize
  }

  // ─────────────────────────────────────────────────────────────
  // Private helpers
  // ─────────────────────────────────────────────────────────────

  private _readFromDisk(buffer: Uint8Array, position: number): Promise<number> {
    return new Promise((resolve, reject) => {
      fs.read(this.fileHandle, buffer, 0, this.pageSize, position, (err, bytesRead) => {
        if (err) return reject(err)
        resolve(bytesRead)
      })
    })
  }

  private _writeToDisk(buffer: Uint8Array, position: number): Promise<number> {
    return new Promise((resolve, reject) => {
      fs.write(this.fileHandle, buffer, 0, this.pageSize, position, (err, bytesWritten) => {
        if (err) return reject(err)
        resolve(bytesWritten)
      })
    })
  }
}
