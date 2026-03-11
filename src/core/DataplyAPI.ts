import fs from 'node:fs'
import {
  type DataplyOptions,
  type MetadataPage,
  type BitmapPage,
  type DataPage,
  type DataplyMetadata,
  LogLevel
} from '../types'
import { type IHookall, useHookall } from 'hookall'
import { PageFileSystem } from './PageFileSystem'
import { MetadataPageManager, DataPageManager, BitmapPageManager, IndexPageManager } from './Page'
import { RowTableEngine } from './RowTableEngine'
import { TextCodec } from '../utils/TextCodec'
import { catchPromise } from '../utils/catchPromise'
import { LockManager } from './transaction/LockManager'
import { Transaction } from './transaction/Transaction'
import { TransactionContext } from './transaction/TxContext'
import { LoggerManager, Logger } from './Logger'

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
  /** Base Logger */
  protected readonly loggerManager: LoggerManager
  /** Logger module for DataplyAPI */
  protected readonly logger: Logger
  /** Whether the database was initialized via `init()` */
  protected initialized: boolean
  /** Whether the database was created this time. */
  private readonly isNewlyCreated: boolean
  private txIdCounter: number
  /** Promise-chain mutex for serializing write operations */
  private writeQueue: Promise<void> = Promise.resolve()

  constructor(
    protected readonly file: string,
    options: DataplyOptions
  ) {
    this.hook = useHookall(this)
    this.options = this.verboseOptions(options)
    this.loggerManager = new LoggerManager(this.options.logLevel)
    this.logger = this.loggerManager.create('DataplyAPI')
    this.isNewlyCreated = !fs.existsSync(file)
    this.fileHandle = this.createOrOpen(file, this.options)
    this.pfs = new PageFileSystem(
      this.fileHandle,
      this.options.pageSize,
      this.options.pageCacheCapacity,
      this.options,
      this.loggerManager.create('PageFileSystem'),
      options.wal ? this.loggerManager.create('WALManager') : undefined
    )
    this.textCodec = new TextCodec()
    this.txContext = new TransactionContext()
    this.lockManager = new LockManager()
    this.rowTableEngine = new RowTableEngine(this.pfs, this.txContext, this.options, this.loggerManager.create('RowTableEngine'))
    this.initialized = false
    this.txIdCounter = 0
    this.logger.debug(`DataplyAPI instance created with file: ${file}`)
  }

  /**
   * Verifies if the page file is a valid Dataply file.
   * The metadata page must be located at the beginning of the Dataply file.
   * @param fileHandle File handle
   * @returns Whether the page file is a valid Dataply file
   */
  protected verifyFormat(fileHandle: number): boolean {
    this.logger.debug(`Verifying format for file handle: ${fileHandle}`)
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
      pagePreallocationCount: 1000,
      wal: null,
      walCheckpointThreshold: 1000,
      logLevel: LogLevel.Info,
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
    this.logger.info(`Initializing new dataply file: ${file}`)
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
    this.logger.info(`Opening dataply file: ${file}`)
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
    this.logger.info('Initializing DataplyAPI')
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
    this.logger.debug(`Creating transaction: ${this.txIdCounter + 1}`)
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
  /**
   * Acquires the global write lock.
   * Returns a release function that MUST be called to unlock.
   * Used internally by runWithDefaultWrite.
   * @returns A release function
   */
  protected acquireWriteLock(): Promise<() => void> {
    this.logger.debug('Acquiring write lock')
    const previous = this.writeQueue
    let release: () => void
    this.writeQueue = new Promise<void>((resolve) => {
      release = resolve
    })
    return previous.then(() => release!)
  }

  /**
   * Runs a write callback within a transaction context with global write serialization.
   * If no transaction is provided, a new transaction is created, committed on success, rolled back on error.
   * If a transaction is provided (external), the write lock is acquired on first call and held until commit/rollback.
   * Subclasses MUST use this method for all write operations instead of runWithDefault.
   * @param callback The callback function to run.
   * @param tx Optional external transaction.
   * @returns The result of the callback.
   */
  protected async runWithDefaultWrite<T>(callback: (tx: Transaction) => Promise<T>, tx?: Transaction): Promise<T> {
    this.logger.debug('Running with default write transaction')
    if (!tx) {
      // Internal transaction: acquire lock, create tx, run, commit, release
      const release = await this.acquireWriteLock()
      const internalTx = this.createTransaction()
      internalTx.__setWriteLockRelease(release)
      const [error, result] = await catchPromise(this.txContext.run(internalTx, () => callback(internalTx)))
      if (error) {
        await internalTx.rollback()
        throw error
      }
      await internalTx.commit()
      return result
    }
    // External transaction: acquire lock on first write, hold until commit/rollback
    if (!tx.__hasWriteLockRelease()) {
      const release = await this.acquireWriteLock()
      tx.__setWriteLockRelease(release)
    }
    const [error, result] = await catchPromise(this.txContext.run(tx, () => callback(tx)))
    if (error) {
      throw error
    }
    return result
  }

  protected async runWithDefault<T>(callback: (tx: Transaction) => Promise<T>, tx?: Transaction): Promise<T> {
    this.logger.debug('Running with default transaction')
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
    this.logger.debug('Streaming with default transaction')
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
    this.logger.debug('Getting metadata')
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
    this.logger.debug('Inserting data')
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefaultWrite(async (tx) => {
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
    this.logger.debug('Inserting data as overflow')
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefaultWrite(async (tx) => {
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
    this.logger.debug(`Inserting batch data: ${dataList.length} items`)
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefaultWrite(async (tx) => {
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
    this.logger.debug(`Updating data for PK: ${pk}`)
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefaultWrite(async (tx) => {
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
    this.logger.debug(`Deleting data for PK: ${pk}`)
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefaultWrite(async (tx) => {
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
    this.logger.debug(`Selecting data for PK: ${pk}`)
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
   * Selects multiple data by their PKs.
   * @param pks Array of PKs to select
   * @param asRaw Whether to return the selected data as raw
   * @param tx Transaction
   * @returns Array of selected data in the same order as input PKs
   */
  async selectMany(pks: number[] | Float64Array, asRaw: true, tx?: Transaction): Promise<(Uint8Array | null)[]>
  async selectMany(pks: number[] | Float64Array, asRaw: false, tx?: Transaction): Promise<(string | null)[]>
  async selectMany(pks: number[] | Float64Array, asRaw?: boolean, tx?: Transaction): Promise<(string | null)[]>
  async selectMany(pks: number[] | Float64Array, asRaw: boolean = false, tx?: Transaction): Promise<(Uint8Array | string | null)[]> {
    this.logger.debug(`Selecting many data: ${pks.length} keys`)
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefault(async (tx) => {
      const results = await this.rowTableEngine.selectMany(pks, tx)
      return results.map(data => {
        if (data === null) return null
        if (asRaw) return data
        return this.textCodec.decode(data)
      })
    }, tx)
  }

  /**
   * Closes the dataply file.
   */
  async close(): Promise<void> {
    this.logger.info('Closing DataplyAPI')
    if (!this.initialized) {
      throw new Error('Dataply instance is not initialized')
    }
    return this.runWithDefaultWrite(() => {
      return this.hook.trigger('close', void 0, async () => {
        await this.pfs.close()
        fs.closeSync(this.fileHandle)
      })
    })
  }
}
