export enum LogLevel {
  /**
   * No logging.
   */
  None = 0,
  /**
   * Debug logging.
   */
  Debug = 1,
  /**
   * Info logging.
  */
  Info = 2,
  /**
    * Warning logging.
  */
  Warning = 3,
  /**
   * Error logging.
   */
  Error = 4,
}

export interface DataplyOptions {
  /**
   * The size of a page in bytes.
   * Default is 8192.
   */
  pageSize?: number
  /**
   * Write-Ahead Logging file path
   */
  wal?: string | undefined | null
  /**
   * The maximum number of pages to cache in memory.
   * This value **DOES NOT** guarantee that the application's memory usage will be exactly `pageSize * pageCacheCapacity`.
   * This value is only a recommendation for optimizing memory usage.
   * Default is 10000.
   */
  pageCacheCapacity?: number
  /**
   * The number of pages to preallocate when creating a new page.
   * Default is 1000.
   */
  pagePreallocationCount?: number
  /**
   * The total number of pages written to the WAL before automatically clearing it.
   * Default is 1000.
   */
  walCheckpointThreshold?: number
  /**
   * The log level.
   * Default is `LogLevel.Info`.
   */
  logLevel?: LogLevel
}

export interface DataplyMetadata {
  /**
   * The size of a page in bytes.
   */
  pageSize: number
  /**
   * The total number of pages in the dataply.
   */
  pageCount: number
  /**
   * The total number of data rows in the dataply.
   */
  rowCount: number
  /**
   * The usage of the dataply. It is calculated based on the remaining page capacity.
   * The value is between 0 and 1.
   */
  usage: number
}

export type DataPage = Uint8Array & { __pageType: 'data' }
export type IndexPage = Uint8Array & { __pageType: 'index' }
export type BitmapPage = Uint8Array & { __pageType: 'bitmap' }
export type OverflowPage = Uint8Array & { __pageType: 'overflow' }
export type MetadataPage = Uint8Array & { __pageType: 'metadata' }
export type EmptyPage = Uint8Array & { __pageType: 'empty' }
export type UnknownPage = Uint8Array & { __pageType: 'unknown' }
