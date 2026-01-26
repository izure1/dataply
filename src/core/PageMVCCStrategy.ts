import fs from 'node:fs'
import { AsyncMVCCStrategy } from 'mvcc-api'
import { LRUMap } from 'cache-entanglement'

/**
 * 페이지 수준 MVCC Strategy.
 * mvcc-api의 AsyncMVCCStrategy를 상속하여 디스크 I/O를 담당합니다.
 * 
 * 키: 페이지 ID (number)
 * 값: 페이지 데이터 (Uint8Array)
 */
export class PageMVCCStrategy extends AsyncMVCCStrategy<number, Uint8Array> {
  /** LRU 캐시 (페이지 ID -> 페이지 데이터) */
  private readonly cache: LRUMap<number, Uint8Array>
  /** 파일 크기 (논리적) */
  private fileSize: number

  constructor(
    private readonly fileHandle: number,
    private readonly pageSize: number,
    cacheCapacity: number
  ) {
    super()
    this.cache = new LRUMap(cacheCapacity)
    this.fileSize = fs.fstatSync(fileHandle).size
  }

  /**
   * 디스크에서 페이지를 읽습니다.
   * 캐시에 있으면 캐시에서 반환합니다.
   * @param pageId 페이지 ID
   * @returns 페이지 데이터
   */
  async read(pageId: number): Promise<Uint8Array> {
    // 캐시 확인
    const cached = this.cache.get(pageId)
    if (cached) {
      // 캐시된 데이터의 복사본 반환 (불변성 보장)
      const copy = new Uint8Array(this.pageSize)
      copy.set(cached)
      return copy
    }

    // 디스크에서 읽기
    const buffer = new Uint8Array(this.pageSize)
    const pageStartPos = pageId * this.pageSize

    // 파일 범위를 벗어나면 빈 버퍼 반환
    if (pageStartPos >= this.fileSize) {
      return buffer
    }

    await this._readFromDisk(buffer, pageStartPos)

    // 캐시에 저장 (복사본)
    const cacheCopy = new Uint8Array(this.pageSize)
    cacheCopy.set(buffer)
    this.cache.set(pageId, cacheCopy)

    return buffer
  }

  /**
   * 디스크에 페이지를 씁니다.
   * @param pageId 페이지 ID
   * @param data 페이지 데이터
   */
  async write(pageId: number, data: Uint8Array): Promise<void> {
    const pageStartPos = pageId * this.pageSize

    // 안전 장치: 512MB 제한
    if (pageStartPos + this.pageSize > 512 * 1024 * 1024) {
      throw new Error(`[Safety Limit] File write exceeds 512MB limit at position ${pageStartPos}`)
    }

    await this._writeToDisk(data, pageStartPos)

    // 캐시 업데이트 (복사본)
    const cacheCopy = new Uint8Array(this.pageSize)
    cacheCopy.set(data)
    this.cache.set(pageId, cacheCopy)

    // 파일 크기 업데이트
    const endPosition = pageStartPos + this.pageSize
    if (endPosition > this.fileSize) {
      this.fileSize = endPosition
    }
  }

  /**
   * 페이지 삭제 (실제로는 캐시에서만 제거)
   * 실제 페이지 해제는 상위 레이어(FreeList)에서 관리합니다.
   * @param pageId 페이지 ID
   */
  async delete(pageId: number): Promise<void> {
    this.cache.delete(pageId)
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
   * 현재 파일 크기 반환
   */
  getFileSize(): number {
    return this.fileSize
  }

  /**
   * 캐시 초기화
   */
  clearCache(): void {
    this.cache.clear()
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
