import fs from 'node:fs'

/**
 * Log Manager class.
 * Records changes to a log file (WAL) and manages them to ensure atomicity of the database.
 */
export class LogManager {
  private fd: number | null = null
  private readonly walFilePath: string
  private readonly pageSize: number
  private readonly entrySize: number
  private buffer: Uint8Array
  private view: DataView

  /**
   * Constructor
   * @param walFilePath WAL file path
   * @param pageSize Page size
   */
  constructor(walFilePath: string, pageSize: number) {
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
   * Reads the log file to recover the page map.
   * Runs synchronously as it is called by the VFS constructor.
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

    // 엔트리 단위로 반복해서 읽기
    // 무한 루프 방지를 위해 entrySize가 0보다 커야 함 (생성자에서 보장됨)
    while (offset + this.entrySize <= currentFileSize) {
      fs.readSync(this.fd!, this.buffer, 0, this.entrySize, offset)

      const pageId = this.view.getUint32(0, true)
      // Restore 시에는 데이터를 복사해서 맵에 저장해야 함 (버퍼가 재사용되므로)
      const pageData = this.buffer.slice(4, 4 + this.pageSize)

      restoredPages.set(pageId, pageData)
      offset += this.entrySize
    }

    return restoredPages
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
