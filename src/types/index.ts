export interface ShardOptions {
  /**
   * 페이지 크기
   */
  pageSize?: number
  /**
   * Write-Ahead Logging 파일 경로
   */
  wal?: string | undefined | null
}

export type DataPage = Uint8Array & { __pageType: 'data' }
export type IndexPage = Uint8Array & { __pageType: 'index' }
export type BitmapPage = Uint8Array & { __pageType: 'bitmap' }
export type OverflowPage = Uint8Array & { __pageType: 'overflow' }
export type MetadataPage = Uint8Array & { __pageType: 'metadata' }
export type EmptyPage = Uint8Array & { __pageType: 'empty' }
export type UnknownPage = Uint8Array & { __pageType: 'unknown' }
