import type { BitmapPage, IndexPage, MetadataPage } from '../types'
import type { Transaction } from './transaction/Transaction'
import { IndexPageManager, MetadataPageManager, PageManager, PageManagerFactory, BitmapPageManager } from './Page'
import { VirtualFileSystem } from './VirtualFileSystem'
import { PageMVCCStrategy } from './PageMVCCStrategy'

/**
 * Page File System class.
 * mvcc-api 기반으로 페이지 관리를 수행합니다.
 */
export class PageFileSystem {
  protected readonly pageFactory = new PageManagerFactory()
  protected readonly vfs: VirtualFileSystem
  protected readonly pageManagerFactory: PageManagerFactory
  protected readonly pageStrategy: PageMVCCStrategy

  /**
   * @param fileHandle 파일 핸들 (fs.open으로 얻은 핸들)
   * @param pageSize 페이지 크기
   * @param pageCacheCapacity 페이지 캐시 크기
   * @param walPath WAL 파일 경로 (기본값: null)
   */
  constructor(
    readonly fileHandle: number,
    readonly pageSize: number,
    readonly pageCacheCapacity: number,
    readonly walPath?: string | undefined | null
  ) {
    this.vfs = new VirtualFileSystem(fileHandle, pageSize, pageCacheCapacity, walPath)
    this.pageManagerFactory = new PageManagerFactory()
    this.pageStrategy = new PageMVCCStrategy(fileHandle, pageSize, pageCacheCapacity)
  }

  /**
   * Initializes the page file system.
   * Performs VFS recovery if necessary.
   */
  async init(): Promise<void> {
    await this.vfs.recover()
  }

  /**
   * Returns the page strategy for transaction use.
   */
  getPageStrategy(): PageMVCCStrategy {
    return this.pageStrategy
  }

  /**
   * Updates the bitmap status for a specific page.
   * @param pageId The ID of the page to update
   * @param isFree True to mark as free, false to mark as used
   * @param tx Transaction
   */
  private async updateBitmap(pageId: number, isFree: boolean, tx: Transaction): Promise<void> {
    const metadata = await this.getMetadata(tx)
    const metadataManager = this.pageFactory.getManager(metadata) as MetadataPageManager
    const bitmapPageId = metadataManager.getBitmapPageId(metadata)

    // 비트맵 페이지 용량 계산
    const headerSize = PageManager.CONSTANT.SIZE_PAGE_HEADER
    const capacityPerBitmapPage = (this.pageSize - headerSize) * 8

    let currentBitmapPageId = bitmapPageId
    let targetBitIndex = pageId

    // 타겟 비트 인덱스가 현재 페이지 용량을 초과하는 경우 다음 비트맵 페이지로 이동
    while (targetBitIndex >= capacityPerBitmapPage) {
      // 현재 비트맵 페이지 로드
      const currentBitmapPage = await this.get(currentBitmapPageId, tx)
      const manager = this.pageFactory.getManager(currentBitmapPage)

      targetBitIndex -= capacityPerBitmapPage

      const nextPageId = manager.getNextPageId(currentBitmapPage)

      if (nextPageId === -1) {
        if (!isFree) {
          throw new Error('Bitmap page not found for reused page')
        }
        const newBitmapPageId = await this.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_BITMAP, tx)

        // 링크 연결
        manager.setNextPageId(currentBitmapPage, newBitmapPageId)
        await this.setPage(currentBitmapPageId, currentBitmapPage, tx)

        currentBitmapPageId = newBitmapPageId
      } else {
        currentBitmapPageId = nextPageId
      }
    }

    // 최종 타겟 비트맵 페이지 로드 및 업데이트
    await tx.__acquireWriteLock(currentBitmapPageId)
    const targetBitmapPage = await this.get(currentBitmapPageId, tx)
    const bitmapManager = this.pageFactory.getManager(targetBitmapPage) as BitmapPageManager

    bitmapManager.setBit(targetBitmapPage as BitmapPage, targetBitIndex, isFree)
    await this.setPage(currentBitmapPageId, targetBitmapPage, tx)
  }

  /**
   * VFS 인스턴스를 반환합니다.
   */
  get vfsInstance(): VirtualFileSystem {
    return this.vfs
  }

  /**
   * 페이지 Strategy를 반환합니다.
   */
  get strategy(): PageMVCCStrategy {
    return this.pageStrategy
  }

  /**
   * @param pageIndex 페이지 인덱스
   * @param tx 트랜잭션
   * @returns 페이지 버퍼
   */
  async get(pageIndex: number, tx: Transaction): Promise<Uint8Array> {
    const page = await tx.readPage(pageIndex)
    if (page === null) {
      // 페이지가 없으면 빈 페이지 반환
      return new Uint8Array(this.pageSize)
    }
    return page
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
    await tx.writePage(pageIndex, page)
  }

  /**
   * Appends and inserts a new page.
   * If a free page is available in the free list, it reuses it.
   * Otherwise, it appends a new page to the end of the file.
   * @returns Created or reused page ID
   */
  async appendNewPage(pageType: number = PageManager.CONSTANT.PAGE_TYPE_EMPTY, tx: Transaction): Promise<number> {
    await tx.__acquireWriteLock(0)
    const metadata = await this.getMetadata(tx)
    const metadataManager = this.pageFactory.getManager(metadata) as MetadataPageManager

    // 1. 재사용 가능한 페이지 확인
    const freePageId = metadataManager.getFreePageId(metadata)

    if (freePageId !== -1) {
      const reusedPageId = freePageId

      const reusedPage = await this.get(reusedPageId, tx)
      const reusedPageManager = this.pageFactory.getManager(reusedPage)

      const nextFreePageId = reusedPageManager.getNextPageId(reusedPage)

      metadataManager.setFreePageId(metadata, nextFreePageId)
      await this.setPage(0, metadata, tx)

      await this.updateBitmap(reusedPageId, false, tx)

      const manager = this.pageFactory.getManagerFromType(pageType)
      const newPage = manager.create(this.pageSize, reusedPageId)
      await this.setPage(reusedPageId, newPage, tx)

      return reusedPageId
    }

    // 2. 새 페이지 추가 로직
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

      page.set(chunk, bodyStart + currentOffset)

      // 남은 용량 업데이트
      // 기존 데이터보다 짧아져서 남은 용량이 늘어나는 경우도 반영해야 하므로 조건문 제거
      const currentUsedSize = currentOffset + writeSize
      const newRemaining = bodySize - currentUsedSize
      manager.setRemainingCapacity(page, newRemaining)

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
      } else {
        // 데이터 쓰기가 완료되었는데 다음 페이지가 남아있다면 잘라내기(Truncate)
        let nextPageId = manager.getNextPageId(page)
        if (nextPageId !== -1) {
          let pendingFreePageId = nextPageId
          while (pendingFreePageId !== -1) {
            const pendingPage = await this.get(pendingFreePageId, tx)
            const pendingManager = this.pageFactory.getManager(pendingPage)
            const next = pendingManager.getNextPageId(pendingPage)

            await this.setFreePage(pendingFreePageId, tx)
            pendingFreePageId = next
          }
          manager.setNextPageId(page, -1)
          await this.setPage(currentPageId, page, tx)
        }
      }
    }
  }

  /**
   * Frees the page and marks it as available in the bitmap.
   * It also adds the page to the linked list of free pages in metadata.
   * @param pageId Page ID
   * @param tx Transaction
   */
  async setFreePage(pageId: number, tx: Transaction): Promise<void> {
    // 1. 메타데이터 조회 및 락 획득
    await tx.__acquireWriteLock(0)
    await tx.__acquireWriteLock(pageId)

    const metadata = await this.getMetadata(tx)
    const metadataManager = this.pageFactory.getManager(metadata) as MetadataPageManager

    // 현재 freePageId 가져오기 (Linked List의 Head)
    const currentHeadFreePageId = metadataManager.getFreePageId(metadata)

    // 2. 페이지 초기화 (EmptyPage) 및 링크 연결
    const emptyPageManager = this.pageFactory.getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_EMPTY)
    const emptyPage = emptyPageManager.create(this.pageSize, pageId)

    // 다음 페이지를 이전 Head로 설정 (Stack Push)
    emptyPageManager.setNextPageId(emptyPage, currentHeadFreePageId)

    await this.setPage(pageId, emptyPage, tx)

    // 3. 비트맵 업데이트 (Free로 표시 -> true)
    await this.updateBitmap(pageId, true, tx)

    // 4. 메타데이터 업데이트 (Head를 현재 페이지로 변경)
    metadataManager.setFreePageId(metadata, pageId)
    await this.setPage(0, metadata, tx)
  }

  /**
   * WAL에 커밋합니다.
   * @param dirtyPages 변경된 페이지들
   */
  async commitToWAL(dirtyPages: Map<number, Uint8Array>): Promise<void> {
    await this.vfs.prepareCommitWAL(dirtyPages)
    await this.vfs.finalizeCommitWAL(false)
  }

  /**
   * Closes the page file system.
   */
  async close(): Promise<void> {
    await this.vfs.close()
  }
}
