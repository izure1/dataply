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
   * Default is 10000.
   */
  pageCacheCapacity?: number
  /**
   * The total number of pages written to the WAL before automatically clearing it.
   * Default is 1000.
   */
  walCheckpointThreshold?: number
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
}

export type DataPage = Uint8Array & { __pageType: 'data' }
export type IndexPage = Uint8Array & { __pageType: 'index' }
export type BitmapPage = Uint8Array & { __pageType: 'bitmap' }
export type OverflowPage = Uint8Array & { __pageType: 'overflow' }
export type MetadataPage = Uint8Array & { __pageType: 'metadata' }
export type EmptyPage = Uint8Array & { __pageType: 'empty' }
export type UnknownPage = Uint8Array & { __pageType: 'unknown' }
