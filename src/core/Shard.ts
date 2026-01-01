import fs from 'node:fs'
import type { ShardOptions, MetadataPage, DataPage } from '../types'
import { PageFileSystem } from './PageFileSystem'
import { MetadataPageManager, DataPageManager } from './Page'
import { RowTableEngine } from './RowTableEngine'
import { TextCodec } from '../utils/TextCodec'
import { catchPromise } from '../utils/catchPromise'
import { LockManager } from './transaction/LockManager'
import { Transaction } from './transaction/Transaction'

/**
 * Class for managing Shard files.
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
   * Verifies if the page file is a valid Shard file.
   * The metadata page must be located at the beginning of the Shard file.
   * @param fileHandle File handle
   * @returns Whether the page file is a valid Shard file
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
   * Fills missing options with default values.
   * @param options Options
   * @returns Options filled without omissions
   */
  protected static VerboseOptions(options?: ShardOptions): Required<ShardOptions> {
    return Object.assign({ pageSize: 8192, wal: null }, options)
  }

  /**
   * Initializes the database file.
   * The first page is initialized as the metadata page.
   * The second page is initialized as the first data page.
   * @param fileHandle File handle
   */
  protected static Initialize(fileHandle: number, options: Required<ShardOptions>): void {
    const metadataPageManager = new MetadataPageManager()
    const dataPageManager = new DataPageManager()
    const metadataPage = new Uint8Array(options.pageSize) as MetadataPage
    const dataPage = new Uint8Array(options.pageSize) as DataPage

    // Initialize the first metadata page
    metadataPageManager.initial(
      metadataPage,
      MetadataPageManager.CONSTANT.PAGE_TYPE_METADATA,
      0,
      0,
      options.pageSize - MetadataPageManager.CONSTANT.SIZE_PAGE_HEADER
    )
    metadataPageManager.setMagicString(metadataPage)
    metadataPageManager.setPageCount(metadataPage, 2)
    metadataPageManager.setPageSize(metadataPage, options.pageSize)
    metadataPageManager.setRootIndexPageId(metadataPage, -1)
    metadataPageManager.setLastInsertPageId(metadataPage, 1)

    // Initialize the second data page
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
   * Opens the database file. If the file does not exist, it initializes it.
   * @param file Database file path
   * @param options Options
   * @returns Shard instance
   */
  static Open(file: string, options?: ShardOptions): Shard {
    const verboseOption = this.VerboseOptions(options)
    let fileHandle: number
    if (!fs.existsSync(file)) {
      if (verboseOption.pageSize < 4096) {
        throw new Error('Page size must be at least 4096 bytes')
      }
      fileHandle = fs.openSync(file, 'w+')
      // 파일이 없으면 생성하고 메타데이터 페이지를 추가합니다.
      this.Initialize(fileHandle, verboseOption)
    } else {
      fileHandle = fs.openSync(file, 'r+')
      // 메타데이터 페이지에서 페이지 크기를 읽어옵니다.
      // 메타데이터 헤더 + 페이지 크기 필드(4바이트)까지 읽기 위해 충분한 크기를 읽습니다.
      const buffer = new Uint8Array(128)
      fs.readSync(fileHandle, buffer, 0, 128, 0)
      const metadataManager = new MetadataPageManager()
      if (metadataManager.isMetadataPage(buffer)) {
        const storedPageSize = metadataManager.getPageSize(buffer)
        if (storedPageSize > 0) {
          verboseOption.pageSize = storedPageSize
        }
      }
    }
    // 메타데이터 확인을 통해 Shard 파일인지 체크합니다.
    if (!this.Verify(fileHandle)) {
      throw new Error('Invalid shard file')
    }
    return new this(file, fileHandle, verboseOption)
  }

  /**
   * Initializes the shard instance.
   * Must be called before using the shard instance.
   * If not called, the shard instance cannot be used.
   */
  async init(): Promise<void> {
    if (this.initialized) {
      return
    }
    await this.rowTableEngine.init()
    this.initialized = true
  }

  /**
   * Creates a transaction.
   * The created transaction object can be used to add or modify data.
   * A transaction must be terminated by calling either `commit` or `rollback`.
   * @returns Transaction object
   */
  createTransaction(): Transaction {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }
    return new Transaction(++this.txIdCounter, this.pfs.vfsInstance, this.lockManager)
  }

  private async runWithDefault<T>(callback: (tx: Transaction) => Promise<T>, tx?: Transaction): Promise<T> {
    const isInternalTx = !tx
    if (!tx) {
      tx = this.createTransaction()
    }
    const [error, result] = await catchPromise(callback(tx))
    if (error) {
      if (isInternalTx) {
        await tx.rollback()
      }
      throw error
    }
    if (isInternalTx) {
      await tx.commit()
    }
    return result
  }

  /**
   * Inserts data. Returns the PK of the added row.
   * @param data Data to add
   * @param tx Transaction
   * @returns PK of the added data
   */
  async insert(data: string | Uint8Array, tx?: Transaction): Promise<number> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }

    if (typeof data === 'string') {
      data = this.textCodec.encode(data)
    }

    return this.runWithDefault(async (tx) => {
      const pk = await this.rowTableEngine.insert(data, tx)
      return pk
    }, tx)
  }

  /**
   * Inserts multiple data in batch.
   * If a transaction is not provided, it internally creates a single transaction to process.
   * @param dataList Array of data to add
   * @param tx Transaction
   * @returns Array of PKs of the added data
   */
  async insertBatch(dataList: (string | Uint8Array)[], tx?: Transaction): Promise<number[]> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }

    return this.runWithDefault(async (tx) => {
      const pks: number[] = []
      for (const data of dataList) {
        const encoded = typeof data === 'string' ? this.textCodec.encode(data) : data
        const pk = await this.rowTableEngine.insert(encoded, tx)
        pks.push(pk)
      }
      return pks
    }, tx)
  }

  /**
   * Updates data.
   * @param pk PK of the data to update
   * @param data Data to update
   * @param tx Transaction
   */
  async update(pk: number, data: string | Uint8Array, tx?: Transaction): Promise<void> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }

    if (typeof data === 'string') {
      data = this.textCodec.encode(data)
    }

    return this.runWithDefault(async (tx) => {
      await this.rowTableEngine.update(pk, data, tx)
    }, tx)
  }

  /**
   * Deletes data.
   * @param pk PK of the data to delete
   * @param tx Transaction
   */
  async delete(pk: number, tx?: Transaction): Promise<void> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }

    return this.runWithDefault(async (tx) => {
      await this.rowTableEngine.delete(pk, tx)
    }, tx)
  }

  /**
   * Selects data.
   * @param pk PK of the data to select
   * @param asRaw Whether to return the selected data as raw
   * @param tx Transaction
   * @returns Selected data
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
   * Closes the shard file.
   */
  async close(): Promise<void> {
    if (!this.initialized) {
      throw new Error('Shard instance is not initialized')
    }
    await this.pfs.close()
    fs.closeSync(this.fileHandle)
  }
}
