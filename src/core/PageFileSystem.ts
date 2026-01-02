import { IndexPage, type MetadataPage } from '../types'
import { IndexPageManager, MetadataPageManager, PageManager, PageManagerFactory } from './Page'
import { VirtualFileSystem } from './VirtualFileSystem'
import type { Transaction } from './transaction/Transaction'

/**
 * Page File System class.
 * Manages pages using VFS and page factory.
 */
export class PageFileSystem {
  protected readonly pageFactory = new PageManagerFactory()
  protected readonly vfs: VirtualFileSystem
  protected readonly pageManagerFactory: PageManagerFactory

  /**
   * @param fileHandle 파일 핸들 (fs.open으로 얻은 핸들)
   * @param pageSize 페이지 크기 (기본값: 4096)
   * @param walPath WAL 파일 경로 (기본값: null)
   */
  constructor(
    protected readonly fileHandle: number,
    readonly pageSize: number = 4096,
    walPath?: string | undefined | null
  ) {
    this.vfs = new VirtualFileSystem(fileHandle, pageSize, walPath)
    this.pageManagerFactory = new PageManagerFactory()
  }

  /**
   * VFS 인스턴스를 반환합니다.
   * Transaction 생성 시 사용됩니다.
   */
  get vfsInstance(): VirtualFileSystem {
    return this.vfs
  }

  /**
   * @param pageIndex 페이지 인덱스
   * @param tx 트랜잭션
   * @returns 페이지 버퍼
   */
  async get(pageIndex: number, tx: Transaction): Promise<Uint8Array> {
    return await this.vfs.read(pageIndex * this.pageSize, this.pageSize, tx)
  }

  /**
   * Reads the page header.
   * @param pageIndex Page index
   * @param tx Transaction
   * @returns Page header buffer
   */
  async getHeader(pageIndex: number, tx: Transaction): Promise<Uint8Array> {
    const page = await this.get(pageIndex, tx)
    return page.subarray(0, PageManager.CONSTANT.SIZE_PAGE_HEADER)
  }

  /**
   * Reads the page body.
   * @param pageIndex Page index
   * @param recursive Whether to read pages recursively
   * @param tx Transaction
   * @returns Page body buffer
   */
  async getBody(pageIndex: number, recursive = false, tx: Transaction): Promise<Uint8Array> {
    const page = await this.get(pageIndex, tx)
    const manager = this.pageFactory.getManager(page)
    const fullBody = manager.getBody(page)

    if (!recursive) {
      return fullBody
    }

    const remainingCapacity = manager.getRemainingCapacity(page)
    const usedSize = fullBody.length - remainingCapacity
    const usedBody = fullBody.subarray(0, usedSize)
    const nextIndex = manager.getNextPageId(page)

    if (nextIndex !== -1) {
      const nextBody = await this.getBody(nextIndex, recursive, tx)
      return Buffer.concat([usedBody, nextBody])
    }
    return usedBody
  }

  /**
   * Returns the metadata page.
   * @param tx Transaction
   * @returns Metadata page
   */
  async getMetadata(tx: Transaction): Promise<MetadataPage> {
    const page = await this.get(0, tx)
    if (!MetadataPageManager.IsMetadataPage(page)) {
      throw new Error('Invalid metadata page')
    }
    return page
  }

  /**
   * Returns the number of pages stored in the database.
   * @param tx Transaction
   * @returns Number of pages
   */
  async getPageCount(tx: Transaction): Promise<number> {
    const metadata = await this.getMetadata(tx)
    const manager = this.pageFactory.getManager(metadata)
    return manager.getPageCount(metadata)
  }

  /**
   * Returns the root index page.
   * @param tx Transaction
   * @returns Root index page
   */
  async getRootIndex(tx: Transaction): Promise<IndexPage> {
    const metadata = await this.getMetadata(tx)
    const manager = this.pageFactory.getManager(metadata)
    const rootIndexPageId = manager.getRootIndexPageId(metadata)
    const rootIndexPage = await this.get(rootIndexPageId, tx)
    if (!IndexPageManager.IsIndexPage(rootIndexPage)) {
      throw new Error('Invalid root index page')
    }
    return rootIndexPage
  }

  /**
   * Sets the metadata page.
   * @param metadataPage Metadata page
   * @param tx Transaction
   */
  async setMetadata(metadataPage: MetadataPage, tx: Transaction): Promise<void> {
    await this.setPage(0, metadataPage, tx)
  }

  /**
   * Sets the page.
   * @param pageIndex Page index
   * @param page Page data
   * @param tx Transaction
   */
  async setPage(pageIndex: number, page: Uint8Array, tx: Transaction): Promise<void> {
    const manager = this.pageFactory.getManager(page)
    manager.updateChecksum(page)

    await tx.__acquireWriteLock(pageIndex)
    await this.vfs.write(pageIndex * this.pageSize, page, tx)
  }

  /**
   * Appends and inserts a new page.
   * @returns Created page ID
   */
  async appendNewPage(pageType: number = PageManager.CONSTANT.PAGE_TYPE_EMPTY, tx: Transaction): Promise<number> {
    await tx.__acquireWriteLock(0)
    const metadata = await this.getMetadata(tx)
    const metadataManager = this.pageFactory.getManager(metadata)
    const pageCount = metadataManager.getPageCount(metadata)
    const newPageIndex = pageCount
    const newTotalCount = pageCount + 1

    const manager = this.pageFactory.getManagerFromType(pageType)
    const newPage = manager.create(this.pageSize, newPageIndex)

    await this.setPage(newPageIndex, newPage, tx)

    metadataManager.setPageCount(metadata, newTotalCount)
    await this.setPage(0, metadata, tx)

    return newPageIndex
  }

  /**
   * Writes data to a page. If it overflows, creates the next page and continues writing.
   * @param pageId Page ID
   * @param data Data to write
   * @param offset Position to write (default: 0)
   * @param tx Transaction
   */
  async writePageContent(pageId: number, data: Uint8Array, offset: number = 0, tx: Transaction): Promise<void> {
    let currentPageId = pageId
    let currentOffset = offset
    let dataOffset = 0

    while (dataOffset < data.length) {
      const page = await this.get(currentPageId, tx)
      const manager = this.pageFactory.getManager(page)
      const bodyStart = PageManager.CONSTANT.SIZE_PAGE_HEADER
      const bodySize = this.pageSize - bodyStart

      // 오프셋이 현재 페이지 범위를 넘어가면 다음 페이지로 이동
      if (currentOffset >= bodySize) {
        currentOffset -= bodySize
        const nextPageId = manager.getNextPageId(page)

        if (nextPageId === -1) {
          // 다음 페이지가 없으면 생성
          const newPageId = await this.appendNewPage(manager.pageType, tx)
          manager.setNextPageId(page, newPageId)
          await this.setPage(currentPageId, page, tx)
          currentPageId = newPageId
        } else {
          currentPageId = nextPageId
        }
        continue
      }

      // 현재 페이지에 쓸 수 있는 크기 계산
      const writeSize = Math.min(data.length - dataOffset, bodySize - currentOffset)
      const chunk = data.subarray(dataOffset, dataOffset + writeSize)

      // 데이터 쓰기
      page.set(chunk, bodyStart + currentOffset)

      // 남은 용량 업데이트 (더 많이 썼을 경우에만 줄어듦)
      const currentUsedSize = currentOffset + writeSize
      const currentRemaining = manager.getRemainingCapacity(page)
      const newRemaining = bodySize - currentUsedSize

      if (newRemaining < currentRemaining) {
        manager.setRemainingCapacity(page, newRemaining)
      }

      await this.setPage(currentPageId, page, tx)

      dataOffset += writeSize
      currentOffset = 0 // 다음 페이지부터는 앞에서부터 채움

      // 데이터가 남았는데 현재 페이지가 꽉 찼다면 다음 페이지 연결 준비
      if (dataOffset < data.length) {
        let nextPageId = manager.getNextPageId(page)
        if (nextPageId === -1) {
          const newPageId = await this.appendNewPage(manager.pageType, tx)
          manager.setNextPageId(page, newPageId)
          await this.setPage(currentPageId, page, tx)
          currentPageId = newPageId
        } else {
          currentPageId = nextPageId
        }
      }
    }
  }

  /**
   * Closes the page file system.
   */
  async close(): Promise<void> {
    await this.vfs.close()
  }
}
