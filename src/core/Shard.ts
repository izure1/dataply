import fs from 'node:fs'
import type { ShardOptions, MetadataPage, DataPage, IndexPage } from '../types'
import { PageFileSystem } from './PageFileSystem'
import { MetadataPageManager, DataPageManager, IndexPageManager } from './Page'
import { RowTableEngine } from './RowTableEngine'
import { TextCodec } from '../utils/TextCodec'
import { LockManager } from './transaction/LockManager'
import { Transaction } from './transaction/Transaction'

/**
 * Shard 파일을 관리하는 클래스
 */
export class Shard {
  protected readonly options: Required<ShardOptions>
  protected readonly pfs: PageFileSystem
  protected readonly rowTableEngine: RowTableEngine
  protected readonly lockManager: LockManager
  private readonly textCodec: TextCodec
  private initialized: boolean
  private txIdCounter = 0

  protected constructor(
    protected file: string,
    protected fileHandle: number,
    options: Required<ShardOptions>
  ) {
    this.options = options
    this.pfs = new PageFileSystem(fileHandle, this.options.pageSize, this.options.wal)
    this.textCodec = new TextCodec()
    this.lockManager = new LockManager()
    this.rowTableEngine = new RowTableEngine(this.pfs)
    this.initialized = false
  }

  /**
   * 페이지 파일이 올바른 Shard 파일인지 확인합니다.
   * 메타데이터 페이지는 Shard 파일의 시작 위치에 위치해야 합니다.
   * @param fileHandle 파일 핸들
   * @returns 페이지 파일이 올바른 Shard 파일인지
   */
  static Verify(fileHandle: number): boolean {
    const size = MetadataPageManager.CONSTANT.OFFSET_MAGIC_STRING + MetadataPageManager.CONSTANT.MAGIC_STRING.length
    const metadataPage = new Uint8Array(size)
    fs.readSync(fileHandle, metadataPage, 0, size, 0)
    if (!MetadataPageManager.IsMetadataPage(metadataPage)) {
      return false
    }
    return MetadataPageManager.Verify(metadataPage)
  }

  /**
   * 누락된 옵션을 기본값으로 채웁니다.
   * @param options 옵션
   * @returns 누락없이 채워진 옵션
   */
  protected static VerboseOptions(options?: ShardOptions): Required<ShardOptions> {
    return Object.assign({ pageSize: 8192, wal: null }, options)
  }

  /**
   * 데이터베이스 파일을 초기화합니다.
   * 맨 처음 페이지는 메타데이터 페이지로 초기화됩니다.
   * 두 번째 페이지는 최초의 데이터 페이지로 초기화됩니다.
   * @param fileHandle 파일 핸들
   */
  protected static Initialize(fileHandle: number, options: Required<ShardOptions>): void {
    const metadataPageManager = new MetadataPageManager()
    const dataPageManager = new DataPageManager()
    const metadataPage = new Uint8Array(options.pageSize) as MetadataPage
    const dataPage = new Uint8Array(options.pageSize) as DataPage

    // 첫 번째 메타데이터 페이지 초기화
    metadataPageManager.initial(
      metadataPage,
      MetadataPageManager.CONSTANT.PAGE_TYPE_METADATA,
      0,
      0,
      options.pageSize - MetadataPageManager.CONSTANT.SIZE_PAGE_HEADER
    )
    metadataPageManager.setMagicString(metadataPage)
    metadataPageManager.setPageCount(metadataPage, 2)
    metadataPageManager.setRootIndexPageId(metadataPage, -1)
    metadataPageManager.setLastInsertPageId(metadataPage, 1)

    // 두 번째 데이터 페이지 초기화
    dataPageManager.initial(
      dataPage,
      DataPageManager.CONSTANT.PAGE_TYPE_DATA,
      1,
      -1,
      options.pageSize - DataPageManager.CONSTANT.SIZE_PAGE_HEADER
    )

    fs.appendFileSync(fileHandle, metadataPage)
    fs.appendFileSync(fileHandle, dataPage)
  }

  /**
   * 데이터베이스 파일을 엽니다. 파일이 존재하지 않는다면 초기화합니다.
   * @param file 데이터베이스 파일
   * @param options 옵션
   * @returns 데이터베이스 파일
   */
  static Open(file: string, options?: ShardOptions): Shard {
    const verboseOption = this.VerboseOptions(options)
    let fileHandle: number
    if (!fs.existsSync(file)) {
      fileHandle = fs.openSync(file, 'w+')
      // 파일이 존재하지 않는다면 생성하고 메타데이터 페이지를 추가합니다.
      this.Initialize(fileHandle, verboseOption)
    } else {
      fileHandle = fs.openSync(file, 'r+')
    }
    // 메타데이터를 확인하여 shard 파일인지 확인합니다.
    if (!this.Verify(fileHandle)) {
      throw new Error('Invalid shard file')
    }
    return new this(file, fileHandle, verboseOption)
  }

  /**
   * shard 인스턴스를 초기화합니다.
   * 반드시 shard 인스턴스를 사용하기 전에 호출해야 합니다.
   * 호출하지 않는다면 shard 인스턴스를 사용할 수 없습니다.
   */
  async init(): Promise<void> {
    if (this.initialized) {
      return
    }
    await this.rowTableEngine.init()
    this.initialized = true
  }

  /**
   * 트랜잭션을 시작합니다.
   * @returns 트랜잭션 객체
   */
  async beginTransaction(): Promise<Transaction> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }
    return new Transaction(++this.txIdCounter, this.pfs.vfsInstance, this.lockManager)
  }

  /**
   * 데이터를 추가합니다. 추가된 행의 PK를 반환합니다.
   * @param data 추가할 데이터
   * @param tx 트랜잭션
   * @returns 추가된 데이터의 PK
   */
  async insert(data: string | Uint8Array, tx?: Transaction): Promise<number> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }
    if (!tx) {
      tx = await this.beginTransaction()
    }
    if (typeof data === 'string') {
      data = this.textCodec.encode(data)
    }
    return this.rowTableEngine.insert(data, tx)
  }

  /**
   * 여러 데이터를 배치로 추가합니다.
   * 트랜잭션이 전달되지 않으면 내부적으로 단일 트랜잭션을 생성하여 처리합니다.
   * @param dataList 추가할 데이터 배열
   * @param tx 트랜잭션
   * @returns 추가된 데이터들의 PK 배열
   */
  async insertBatch(dataList: (string | Uint8Array)[], tx?: Transaction): Promise<number[]> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }

    const isInternalTx = !tx
    if (!tx) {
      tx = await this.beginTransaction()
    }

    const pks: number[] = []

    try {
      for (const data of dataList) {
        const encoded = typeof data === 'string' ? this.textCodec.encode(data) : data
        const pk = await this.rowTableEngine.insert(encoded, tx)
        pks.push(pk)
      }
      // 내부 트랜잭션인 경우에만 commit
      if (isInternalTx) {
        await tx.commit()
      }
      return pks
    } catch (error) {
      // 내부 트랜잭션인 경우에만 rollback
      if (isInternalTx) {
        await tx.rollback()
      }
      throw error
    }
  }

  /**
   * 데이터를 수정합니다.
   * @param pk 수정할 데이터의 PK
   * @param data 수정할 데이터
   * @param tx 트랜잭션
   */
  async update(pk: number, data: string | Uint8Array, tx?: Transaction): Promise<void> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }
    if (!tx) {
      tx = await this.beginTransaction()
    }
    const encoded = typeof data === 'string' ? this.textCodec.encode(data) : data
    await this.rowTableEngine.update(pk, encoded, tx)
  }

  /**
   * 데이터를 조회합니다.
   * @param pk 조회할 데이터의 PK
   * @param asRaw 조회할 데이터를 원본으로 반환할지
   * @param tx 트랜잭션
   * @returns 조회된 데이터
   */
  async select(pk: number, asRaw: true, tx?: Transaction): Promise<Uint8Array | null>
  async select(pk: number, asRaw: false, tx?: Transaction): Promise<string | null>
  async select(pk: number, asRaw?: boolean, tx?: Transaction): Promise<string | null>
  async select(pk: number, asRaw: boolean = false, tx?: Transaction): Promise<Uint8Array | string | null> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }
    const data = await this.rowTableEngine.selectByPK(pk, tx)
    if (data === null) {
      return null
    }
    if (asRaw) {
      return data
    }
    return this.textCodec.decode(data)
  }

  /**
   * shard 파일을 닫습니다.
   */
  async close(): Promise<void> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }
    await this.pfs.close()
    fs.closeSync(this.fileHandle)
  }
}
