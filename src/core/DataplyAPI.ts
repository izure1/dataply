import fs from 'node:fs'
import type { DataplyOptions, MetadataPage, BitmapPage, DataPage, DataplyMetadata } from '../types'
import { type IHookall, useHookall } from 'hookall'
import { PageFileSystem } from './PageFileSystem'
import { MetadataPageManager, DataPageManager, BitmapPageManager, IndexPageManager } from './Page'
import { RowTableEngine } from './RowTableEngine'
import { TextCodec } from '../utils/TextCodec'
import { catchPromise } from '../utils/catchPromise'
import { LockManager } from './transaction/LockManager'
import { Transaction } from './transaction/Transaction'
import { TransactionContext } from './transaction/TxContext'

interface DataplyAPIAsyncHook {
  init: (tx: Transaction, isNewlyCreated: boolean) => Promise<Transaction>
  close: () => Promise<void>
}

/**
 * Class for managing Dataply files.
 */
export class DataplyAPI {
  /**
   * These are not the same options that were used when the database was created.
   * They are simply the options received when the instance was created.
   * If you want to retrieve the options used during database creation, use `getMetadata()` instead.
   */
  readonly options: Required<DataplyOptions>
  /** File handle. Database file descriptor */
  protected readonly fileHandle: number
  /** Page file system. Used for managing pages. If you know what it is, you can skip this. */
  protected readonly pfs: PageFileSystem
  /** Row table engine. Used for managing rows. If you know what it is, you can skip this. */
  protected readonly rowTableEngine: RowTableEngine
  /** Lock manager. Used for managing transactions */
  protected readonly lockManager: LockManager
  /** Text codec. Used for encoding and decoding text data */
  protected readonly textCodec: TextCodec
  /** Transaction context */
  protected readonly txContext: TransactionContext
  /** Hook */
  protected readonly hook: IHookall<DataplyAPIAsyncHook>
  /** Whether the database was initialized via `init()` */
  protected initialized: boolean
  /** Whether the database was created this time. */
  private readonly isNewlyCreated: boolean
  private txIdCounter: number

  constructor(
    protected readonly file: string,
    options: DataplyOptions
  ) {
    this.hook = useHookall(this)
    this.options = this.verboseOptions(options)
    this.isNewlyCreated = !fs.existsSync(file)
    this.fileHandle = this.createOrOpen(file, this.options)
    this.pfs = new PageFileSystem(
      this.fileHandle,
      this.options.pageSize,
      this.options.pageCacheCapacity,
      this.options
    )
    this.textCodec = new TextCodec()
    this.txContext = new TransactionContext()
    this.lockManager = new LockManager()
    this.rowTableEngine = new RowTableEngine(this.pfs, this.txContext, this.options)
    this.initialized = false
    this.txIdCounter = 0
  }

  /**
   * Verifies if the page file is a valid Dataply file.
   * The metadata page must be located at the beginning of the Dataply file.
   * @param fileHandle File handle
   * @returns Whether the page file is a valid Dataply file
   */
  protected verifyFormat(fileHandle: number): boolean {
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
  protected verboseOptions(options?: DataplyOptions): Required<DataplyOptions> {
    return Object.assign({
      pageSize: 8192,
      pageCacheCapacity: 10000,
      wal: null,
      walCheckpointThreshold: 1000,
    }, options)
  }

  /**
   * Initializes the database file.
   * The first page is initialized as the metadata page.
   * The second page is initialized as the first data page.
   * @param file Database file path
   * @param fileHandle File handle
   */
  protected initializeFile(file: string, fileHandle: number, options: Required<DataplyOptions>): void {
    const metadataPageManager = new MetadataPageManager()
    const bitmapPageManager = new BitmapPageManager()
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
    metadataPageManager.setPageSize(metadataPage, options.pageSize)

    metadataPageManager.setRootIndexPageId(metadataPage, -1)
    const order = Math.floor(((options.pageSize - IndexPageManager.CONSTANT.OFFSET_KEYS_AND_VALUES) + IndexPageManager.CONSTANT.SIZE_KEY) / (IndexPageManager.CONSTANT.SIZE_KEY + IndexPageManager.CONSTANT.SIZE_VALUE))
    metadataPageManager.setRootIndexOrder(metadataPage, order)
    metadataPageManager.setBitmapPageId(metadataPage, 1)
    metadataPageManager.setLastInsertPageId(metadataPage, 2)

    metadataPageManager.setPageCount(metadataPage, 3)
    metadataPageManager.setFreePageId(metadataPage, -1)

    // Initialize the second bitmap page
    const bitmapPage = new Uint8Array(options.pageSize) as BitmapPage
    bitmapPageManager.initial(
      bitmapPage,
      BitmapPageManager.CONSTANT.PAGE_TYPE_BITMAP,
      1,
      -1,
      options.pageSize - BitmapPageManager.CONSTANT.SIZE_PAGE_HEADER
    )

    // Initialize the third data page
    dataPageManager.initial(
      dataPage,
      DataPageManager.CONSTANT.PAGE_TYPE_DATA,
      2,
      -1,
      options.pageSize - DataPageManager.CONSTANT.SIZE_PAGE_HEADER
    )

    fs.appendFileSync(fileHandle, new Uint8Array([
      ...metadataPage,
      ...bitmapPage,
      ...dataPage,
    ]))
  }

  /**
   * Opens the database file. If the file does not exist, it initializes it.
   * @param file Database file path
   * @param options Options
   * @returns File handle
   */
  protected createOrOpen(file: string, options: Required<DataplyOptions>): number {
    let fileHandle: number
    if (options.pageCacheCapacity < 100) {
      throw new Error('Page cache capacity must be at least 100')
    }
    if (!fs.existsSync(file)) {
      if (options.pageSize < 4096) {
        throw new Error('Page size must be at least 4096 bytes')
      }
      fileHandle = fs.openSync(file, 'w+')
      // 파일이 없으면 생성하고 메타데이터 페이지를 추가합니다.
      this.initializeFile(file, fileHandle, options)
    } else {
      fileHandle = fs.openSync(file, 'r+')
      // 메타데이터 페이지에서 페이지 크기를 읽어옵니다.
      // 메타데이터 헤더 + 페이지 크기 필드(4바이트)까지 읽기 위해 충분한 크기를 읽습니다.
      const buffer = new Uint8Array(
        MetadataPageManager.CONSTANT.OFFSET_PAGE_SIZE +
        MetadataPageManager.CONSTANT.SIZE_PAGE_SIZE
      )
      fs.readSync(fileHandle, buffer)
      const metadataManager = new MetadataPageManager()
      if (metadataManager.isMetadataPage(buffer)) {
        const storedPageSize = metadataManager.getPageSize(buffer)
        if (storedPageSize > 0) {
          options.pageSize = storedPageSize
        }
      }
    }
    // 메타데이터 확인을 통해 Dataply 파일인지 체크합니다.
    if (!this.verifyFormat(fileHandle)) {
      throw new Error('Invalid dataply file')
    }
    return fileHandle
  }

  /**
   * Initializes the dataply instance.
   * Must be called before using the dataply instance.
   * If not called, the dataply instance cannot be used.
   */
  async init(): Promise<void> {
    if (this.initialized) {
      return
    }
    await this.runWithDefault(async (tx) => {
      await this.hook.trigger('init', tx, async (tx) => {
        // VFS/PFS 초기화 (복구 로직 포함)
        await this.pfs.init()
        await this.rowTableEngine.init()
        this.initialized = true
        return tx
      }, this.isNewlyCreated)
    })
  }

  /**
   * Creates a transaction.
   * The created transaction object can be used to add or modify data.
   * A transaction must be terminated by calling either `commit` or `rollback`.
   * @returns Transaction object
   */
  createTransaction(): Transaction {
    return new Transaction(
      ++this.txIdCounter,
      this.txContext,
      this.pfs.getPageStrategy(),
      this.lockManager,
      this.pfs
    )
  }

  /**
   * Runs a callback function within a transaction context.
   * If no transaction is provided, a new transaction is created.
   * The transaction is committed if the callback completes successfully,
   * or rolled back if an error occurs.
   * @param callback The callback function to run within the transaction context.
   * @param tx The transaction to use. If not provided, a new transaction is created.
   * @returns The result of the callback function.
   */
  protected async runWithDefault<T>(callback: (tx: Transaction) => Promise<T>, tx?: Transaction): Promise<T> {
    const isInternalTx = !tx
    if (!tx) {
      tx = this.createTransaction()
    }
    const [error, result] = await catchPromise(this.txContext.run(tx, () => callback(tx)))
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
   * Runs a generator callback function within a transaction context.
   * Similar to runWithDefault but allows yielding values from an AsyncGenerator.
   * If no transaction is provided, a new transaction is created.
   * The transaction is committed if the generator completes successfully,
   * or rolled back if an error occurs.
   * @param callback The generator callback function to run within the transaction context.
   * @param tx The transaction to use. If not provided, a new transaction is created.
   * @returns An AsyncGenerator that yields values from the callback.
   */
  protected async *streamWithDefault<T>(
    callback: (tx: Transaction) => AsyncGenerator<T>,
    tx?: Transaction
  ): AsyncGenerator<T> {
    const isInternalTx = !tx
    if (!tx) {
      tx = this.createTransaction()
    }
    let hasError = false
    try {
      const generator = this.txContext.stream(tx, () => callback(tx!))
      for await (const value of generator) {
        yield value
      }
    }
    catch (error) {
      hasError = true
      if (isInternalTx) {
        await tx.rollback()
      }
      throw error
    }
    finally {
      if (!hasError && isInternalTx) {
        await tx.commit()
      }
    }
  }

  /**
   * Retrieves metadata from the dataply.
   * @returns Metadata of the dataply.
   */
  async getMetadata(tx?: Transaction): Promise<DataplyMetadata> {
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault((tx) => this.rowTableEngine.getMetadata(tx), tx)
  }

  /**
   * Inserts data. Returns the PK of the added row.
   * @param data Data to add
   * @param incrementRowCount Whether to increment the row count to metadata
   * @param tx Transaction
   * @returns PK of the added data
   */
  async insert(data: string | Uint8Array, incrementRowCount?: boolean, tx?: Transaction): Promise<number> {
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault(async (tx) => {
      incrementRowCount = incrementRowCount ?? true
      if (typeof data === 'string') {
        data = this.textCodec.encode(data)
      }
      const pks = await this.rowTableEngine.insert([data], incrementRowCount, false, tx)
      return pks[0]
    }, tx)
  }

  /**
   * Inserts overflow data forcly. Returns the PK of the added row.
   * @param data Data to add
   * @param incrementRowCount Whether to increment the row count to metadata
   * @param tx Transaction
   * @returns PK of the added data
   */
  async insertAsOverflow(data: string | Uint8Array, incrementRowCount?: boolean, tx?: Transaction): Promise<number> {
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault(async (tx) => {
      incrementRowCount = incrementRowCount ?? true
      if (typeof data === 'string') {
        data = this.textCodec.encode(data)
      }
      const pks = await this.rowTableEngine.insert([data], incrementRowCount, true, tx)
      return pks[0]
    }, tx)
  }

  /**
   * Inserts multiple data in batch.
   * If a transaction is not provided, it internally creates a single transaction to process.
   * @param dataList Array of data to add
   * @param incrementRowCount Whether to increment the row count to metadata
   * @param tx Transaction
   * @returns Array of PKs of the added data
   */
  async insertBatch(dataList: (string | Uint8Array)[], incrementRowCount?: boolean, tx?: Transaction): Promise<number[]> {
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault(async (tx) => {
      incrementRowCount = incrementRowCount ?? true
      const encodedList = dataList.map(data =>
        typeof data === 'string' ? this.textCodec.encode(data) : data
      )
      return this.rowTableEngine.insert(encodedList, incrementRowCount, false, tx)
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
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault(async (tx) => {
      if (typeof data === 'string') {
        data = this.textCodec.encode(data)
      }
      await this.rowTableEngine.update(pk, data, tx)
    }, tx)
  }

  /**
   * Deletes data.
   * @param pk PK of the data to delete
   * @param decrementRowCount Whether to decrement the row count to metadata
   * @param tx Transaction
   */
  async delete(pk: number, decrementRowCount?: boolean, tx?: Transaction): Promise<void> {
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault(async (tx) => {
      decrementRowCount = decrementRowCount ?? true
      await this.rowTableEngine.delete(pk, decrementRowCount, tx)
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
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault(async (tx) => {
      const data = await this.rowTableEngine.selectByPK(pk, tx)
      if (data === null) return null
      if (asRaw) return data
      return this.textCodec.decode(data)
    }, tx)
  }

  /**
   * Closes the dataply file.
   */
  async close(): Promise<void> {
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.hook.trigger('close', void 0, async () => {
      await this.pfs.close()
      fs.closeSync(this.fileHandle)
    })
  }
}
