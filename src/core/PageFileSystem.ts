import { IndexPage, type MetadataPage } from '../types'
import { IndexPageManager, MetadataPageManager, PageManager, PageManagerFactory } from './Page'
import { VirtualFileSystem } from './VirtualFileSystem'
import type { Transaction } from './transaction/Transaction'

/**
 * 페이지 파일 시스템 클래스
 * vps와 page factory를 사용하여 페이지를 관리합니다.
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
   * Transaction 생성을 위해 사용됩니다.
   */
  get vfsInstance(): VirtualFileSystem {
    return this.vfs
  }

  /**
   * @param pageIndex 페이지 인덱스
   * @param tx 트랜잭션 (MVCC용: 스냅샷 격리)
   * @returns 페이지 버퍼
   */
  async get(pageIndex: number, tx?: Transaction): Promise<Uint8Array> {
    // [MVCC] Reads don't acquire locks - snapshot isolation via UndoBuffer
    return await this.vfs.read(pageIndex * this.pageSize, this.pageSize, tx)
  }

  /**
   * 페이지 헤더를 읽어옵니다.
   * @param pageIndex 페이지 인덱스
   * @param tx 트랜잭션
   * @returns 페이지 헤더 버퍼
   */
  async getHeader(pageIndex: number, tx?: Transaction): Promise<Uint8Array> {
    const page = await this.get(pageIndex, tx)
    return page.subarray(0, PageManager.CONSTANT.SIZE_PAGE_HEADER)
  }

  /**
   * 페이지 바디를 읽어옵니다.
   * @param pageIndex 페이지 인덱스
   * @param recursive 재귀적으로 페이지를 읽을지 여부
   * @param tx 트랜잭션
   * @returns 페이지 바디 버퍼
   */
  async getBody(pageIndex: number, recursive = false, tx?: Transaction): Promise<Uint8Array> {
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
   * 메타데이터 페이지를 반환합니다.
   * @returns 메타데이터 페이지
   */
  /**
   * 메타데이터 페이지를 반환합니다.
   * @param tx 트랜잭션
   * @returns 메타데이터 페이지
   */
  async getMetadata(tx?: Transaction): Promise<MetadataPage> {
    const page = await this.get(0, tx)
    if (!MetadataPageManager.IsMetadataPage(page)) {
      throw new Error('Invalid metadata page')
    }
    return page
  }

  /**
   * 데이터베이스에 저장된 페이지 개수를 반환합니다.
   * @param tx 트랜잭션
   * @returns 페이지 개수
   */
  async getPageCount(tx?: Transaction): Promise<number> {
    const metadata = await this.getMetadata(tx)
    const manager = this.pageFactory.getManager(metadata)
    return manager.getPageCount(metadata)
  }

  /**
   * 루트 인덱스 페이지를 반환합니다.
   * @param tx 트랜잭션
   * @returns 루트 인덱스 페이지
   */
  async getRootIndex(tx?: Transaction): Promise<IndexPage> {
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
   * 메타데이터 페이지를 설정합니다.
   * @param metadataPage 메타데이터 페이지
   * @param tx 트랜잭션
   */
  async setMetadata(metadataPage: MetadataPage, tx?: Transaction): Promise<void> {
    await this.setPage(0, metadataPage, tx)
  }

  /**
   * 페이지를 설정합니다.
   * @param pageIndex 페이지 인덱스
   * @param page 페이지 데이터
   * @param tx 트랜잭션
   */
  async setPage(pageIndex: number, page: Uint8Array, tx?: Transaction): Promise<void> {
    if (tx) {
      await tx.acquireWriteLock(pageIndex)
    }
    await this.vfs.write(pageIndex * this.pageSize, page, tx)
  }

  /**
   * 페이지 헤더를 설정합니다.
   * @param pageIndex 페이지 인덱스
   * @param header 페이지 헤더
   * @param tx 트랜잭션
   */
  async setPageHeader(pageIndex: number, header: Uint8Array, tx?: Transaction): Promise<void> {
    const page = await this.get(pageIndex, tx)
    const manager = this.pageFactory.getManager(page)
    manager.setHeader(page, header)
    await this.setPage(pageIndex, page, tx)
  }

  /**
   * 페이지 바디를 설정합니다.
   * @param pageIndex 페이지 인덱스
   * @param body 페이지 바디
   * @param tx 트랜잭션
   */
  async setPageBody(pageIndex: number, body: Uint8Array, tx?: Transaction): Promise<void> {
    const page = await this.get(pageIndex, tx)
    const manager = this.pageFactory.getManager(page)
    manager.setBody(page, body)
    await this.setPage(pageIndex, page, tx)
  }

  /**
   * 새로운 페이지를 생성하고 삽입합니다.
   * @returns 생성된 페이지 아이디
   */
  async appendNewPage(pageType: number = PageManager.CONSTANT.PAGE_TYPE_EMPTY, tx?: Transaction): Promise<number> {
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
   * 페이지에 데이터를 작성합니다. 만일 오버플로우된다면 다음 페이지를 생성하고, 이어서 작성합니다.
   * @param pageId 페이지 아이디
   * @param data 작성할 데이터
   * @param offset 작성할 위치 (기본값: 0)
   * @param tx 트랜잭션
   */
  async writePageContent(pageId: number, data: Uint8Array, offset: number = 0, tx?: Transaction): Promise<void> {
    let currentPageId = pageId
    let currentOffset = offset
    let dataOffset = 0

    while (dataOffset < data.length) {
      const page = await this.get(currentPageId, tx)
      const manager = this.pageFactory.getManager(page)
      const bodyStart = PageManager.CONSTANT.SIZE_PAGE_HEADER
      const bodySize = this.pageSize - bodyStart

      // 오프셋이 현재 페이지 범위를 벗어나는 경우 다음 페이지로 이동
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

      // 현재 페이지에 작성할 수 있는 크기 계산
      const writeSize = Math.min(data.length - dataOffset, bodySize - currentOffset)
      const chunk = data.subarray(dataOffset, dataOffset + writeSize)

      // 페이지에 데이터 쓰기
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
      currentOffset = 0 // 다음 페이지부터는 처음부터 작성

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
   * 페이지 파일 시스템을 닫습니다.
   */
  async close(): Promise<void> {
    await this.vfs.close()
  }
}
