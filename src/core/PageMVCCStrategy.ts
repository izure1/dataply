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
  /** 디스크에 기록되지 않은 변경된 페이지들 (페이지 ID -> 페이지 데이터) */
  private readonly dirtyPages: Map<number, Uint8Array> = new Map()
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
    // 1. 더티 페이지(아직 디스크에 안 써진 최신본) 확인
    const dirty = this.dirtyPages.get(pageId)
    if (dirty) {
      const copy = new Uint8Array(this.pageSize)
      copy.set(dirty)
      return copy
    }

    // 2. 캐시 확인
    const cached = this.cache.get(pageId)
    if (cached) {
      return cached
    }

    // 3. 디스크에서 읽기
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

    // 데이터 복사본 생성 (원본 수정을 방지하기 위함)
    const dataCopy = new Uint8Array(this.pageSize)
    dataCopy.set(data)

    // 더티 페이지 및 캐시 업데이트 (디스크 쓰기는 지연됨)
    this.dirtyPages.set(pageId, dataCopy)
    this.cache.set(pageId, dataCopy)

    // 파일 크기 업데이트 (논리적 크기)
    const endPosition = pageStartPos + this.pageSize
    if (endPosition > this.fileSize) {
      this.fileSize = endPosition
    }
  }

  /**
   * 더티 페이지들을 메인 디스크 파일에 일괄 기록합니다.
   * WAL 체크포인트 시점에 호출되어야 합니다.
   */
  async flush(): Promise<void> {
    if (this.dirtyPages.size === 0) {
      return
    }

    // 1. 현재 시점의 더티 페이지들을 스냅샷으로 캡처
    const snapshot = new Map(this.dirtyPages)

    // 페이지 번호 순으로 정렬하여 순차 I/O 유도
    const sortedPageIds = Array.from(snapshot.keys()).sort((a, b) => a - b)

    for (const pageId of sortedPageIds) {
      const data = snapshot.get(pageId)!
      const position = pageId * this.pageSize
      await this._writeToDisk(data, position)

      // 2. 기록에 성공한 것만 dirtyPages에서 제거 (그 사이 새로 들어온 데이터는 보존됨)
      // 주의: 만약 flush 도중 새로운 데이터가 써졌다면 데이터의 '내용'이 다를 수 있음.
      // 하지만 Dataply의 경우 글로벌 락으로 보호되므로 flush 도중 write가 발생하지 않음을 전제함.
      this.dirtyPages.delete(pageId)
    }
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
   * 페이지 삭제 (실제로는 캐시에서만 제거)
   * 실제 페이지 해제는 상위 레이어(FreeList)에서 관리합니다.
   * @param pageId 페이지 ID
   */
  async delete(pageId: number): Promise<void> {
    this.dirtyPages.delete(pageId)
    this.cache.delete(pageId)
  }

  /**
   * 페이지 존재 여부 확인
   * @param pageId 페이지 ID
   * @returns 존재하면 true
   */
  async exists(pageId: number): Promise<boolean> {
    if (this.dirtyPages.has(pageId)) {
      return true
    }
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
