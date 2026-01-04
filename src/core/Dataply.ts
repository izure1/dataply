import type { DataplyOptions, DataplyMetadata } from '../types'
import { DataplyAPI } from './DataplyAPI'
import { Transaction } from './transaction/Transaction'

/**
 * Class for managing Dataply files.
 */
export class Dataply {
  protected readonly api: DataplyAPI

  constructor(file: string, options?: DataplyOptions) {
    this.api = DataplyAPI.Use(file, options)
  }

  /**
   * Gets the options used to open the dataply.
   * @returns Options used to open the dataply.
   */
  get options(): Required<DataplyOptions> {
    return this.api.options
  }

  /**
   * Creates a transaction.
   * The created transaction object can be used to add or modify data.
   * A transaction must be terminated by calling either `commit` or `rollback`.
   * @returns Transaction object
   */
  createTransaction(): Transaction {
    return this.api.createTransaction()
  }

  /**
   * Initializes the dataply instance.
   * Must be called before using the dataply instance.
   * If not called, the dataply instance cannot be used.
   */
  async init(): Promise<void> {
    return this.api.init()
  }

  /**
   * Retrieves metadata from the dataply.
   * @returns Metadata of the dataply.
   */
  async getMetadata(): Promise<DataplyMetadata> {
    return this.api.getMetadata()
  }

  /**
   * Inserts data. Returns the PK of the added row.
   * @param data Data to add
   * @param tx Transaction
   * @returns PK of the added data
   */
  async insert(data: string | Uint8Array, tx?: Transaction): Promise<number> {
    return this.api.insert(data, true, tx)
  }

  /**
   * Inserts multiple data in batch.
   * If a transaction is not provided, it internally creates a single transaction to process.
   * @param dataList Array of data to add
   * @param tx Transaction
   * @returns Array of PKs of the added data
   */
  async insertBatch(dataList: (string | Uint8Array)[], tx?: Transaction): Promise<number[]> {
    return this.api.insertBatch(dataList, true, tx)
  }

  /**
   * Updates data.
   * @param pk PK of the data to update
   * @param data Data to update
   * @param tx Transaction
   */
  async update(pk: number, data: string | Uint8Array, tx?: Transaction): Promise<void> {
    return this.api.update(pk, data, tx)
  }

  /**
   * Deletes data.
   * @param pk PK of the data to delete
   * @param tx Transaction
   */
  async delete(pk: number, tx?: Transaction): Promise<void> {
    return this.api.delete(pk, true, tx)
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
    return this.api.select(pk, asRaw as any, tx)
  }

  /**
   * Closes the dataply file.
   */
  async close(): Promise<void> {
    return this.api.close()
  }
}
