import type { ShardOptions, ShardMetadata } from '../types'
import { ShardAPI } from './ShareAPI'
import { Transaction } from './transaction/Transaction'

/**
 * Class for managing Shard files.
 */
export class Shard {
  protected readonly api: ShardAPI

  constructor(file: string, options?: ShardOptions) {
    this.api = ShardAPI.Use(file, options)
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
   * Initializes the shard instance.
   * Must be called before using the shard instance.
   * If not called, the shard instance cannot be used.
   */
  async init(): Promise<void> {
    return this.api.init()
  }

  /**
   * Retrieves metadata from the shard.
   * @returns Metadata of the shard.
   */
  async getMetadata(): Promise<ShardMetadata> {
    return this.api.getMetadata()
  }

  /**
   * Inserts data. Returns the PK of the added row.
   * @param data Data to add
   * @param tx Transaction
   * @returns PK of the added data
   */
  async insert(data: string | Uint8Array, tx?: Transaction): Promise<number> {
    return this.api.insert(data, tx)
  }

  /**
   * Inserts multiple data in batch.
   * If a transaction is not provided, it internally creates a single transaction to process.
   * @param dataList Array of data to add
   * @param tx Transaction
   * @returns Array of PKs of the added data
   */
  async insertBatch(dataList: (string | Uint8Array)[], tx?: Transaction): Promise<number[]> {
    return this.api.insertBatch(dataList, tx)
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
    return this.api.delete(pk, tx)
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
   * Closes the shard file.
   */
  async close(): Promise<void> {
    return this.api.close()
  }
}
