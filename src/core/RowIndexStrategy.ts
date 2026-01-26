import type { BPTreeInternalNode, BPTreeLeafNode, BPTreeNode, SerializeStrategyHead } from 'serializable-bptree'
import { SerializeStrategyAsync } from 'serializable-bptree'
import { PageFileSystem } from './PageFileSystem'
import { IndexPageManager, PageManager, PageManagerFactory } from './Page'
import { TextCodec } from '../utils/TextCodec'
import { TransactionContext } from './transaction/TxContext'

export class RowIdentifierStrategy extends SerializeStrategyAsync<number, number> {
  protected rootPageId = 0
  protected factory: PageManagerFactory
  protected indexPageManger: IndexPageManager
  protected codec: TextCodec

  constructor(
    readonly order: number,
    protected readonly pfs: PageFileSystem,
    protected readonly txContext: TransactionContext
  ) {
    super(order)
    this.factory = new PageManagerFactory()
    this.indexPageManger = this.factory.getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_INDEX) as IndexPageManager
    this.codec = new TextCodec()
  }

  async id(isLeaf: boolean): Promise<string> {
    const tx = this.txContext.get()!

    // Only reserve the page ID - don't create the page yet
    // The page will be created when BPTree calls write()
    await tx.__acquireWriteLock(0)
    const metadata = await this.pfs.getMetadata(tx)
    const metadataManager = this.factory.getManager(metadata)

    // Check free list first
    const freePageId = metadataManager.getFreePageId(metadata)
    let pageId: number

    if (freePageId !== -1) {
      // Reuse a free page - just update the free list pointer
      const freePage = await this.pfs.get(freePageId, tx)
      const freePageManager = this.factory.getManager(freePage)
      const nextFreePageId = freePageManager.getNextPageId(freePage)
      metadataManager.setFreePageId(metadata, nextFreePageId)
      pageId = freePageId
    } else {
      // Allocate a new page ID (just increment counter, don't create page)
      const pageCount = metadataManager.getPageCount(metadata)
      pageId = pageCount
      metadataManager.setPageCount(metadata, pageCount + 1)
    }

    await this.pfs.setMetadata(metadata, tx)
    return pageId.toString()
  }

  async read(id: string): Promise<BPTreeNode<number, number>> {
    const tx = this.txContext.get()!
    const pageId = +(id)
    const page = await this.pfs.get(pageId, tx)

    // Check if this is a valid index page - if not, throw to signal non-existence
    if (!IndexPageManager.IsIndexPage(page)) {
      throw new Error(`Node ${id} does not exist - not a valid index page`)
    }

    const indexId = this.indexPageManger.getIndexId(page)
    const parentIndexId = this.indexPageManger.getParentIndexId(page)
    const nextIndexId = this.indexPageManger.getNextIndexId(page)
    const prevIndexId = this.indexPageManger.getPrevIndexId(page)
    const isLeaf = this.indexPageManger.getIsLeaf(page)
    const keys = this.indexPageManger.getKeys(page)
    const values = this.indexPageManger.getValues(page)

    let res: BPTreeLeafNode<number, number> | BPTreeInternalNode<number, number>
    if (isLeaf) res = {
      leaf: true,
      id: indexId + '',
      parent: parentIndexId ? parentIndexId + '' : null,
      next: nextIndexId ? nextIndexId + '' : null,
      prev: prevIndexId ? prevIndexId + '' : null,
      keys: keys.map((key) => [key]),
      values
    }
    else res = {
      leaf: false,
      id: indexId + '',
      parent: parentIndexId ? parentIndexId + '' : null,
      next: nextIndexId ? nextIndexId + '' : null,
      prev: prevIndexId ? prevIndexId + '' : null,
      keys: keys.map((key) => key + ''),
      values
    }
    return res
  }

  async write(id: string, node: BPTreeNode<number, number>): Promise<void> {
    const tx = this.txContext.get()!
    const pageId = +(id)

    // Get existing page or create new index page structure
    let page = await this.pfs.get(pageId, tx)
    if (!IndexPageManager.IsIndexPage(page)) {
      // Create a new index page structure for this pageId
      page = this.indexPageManger.create(this.pfs.pageSize, pageId)
    }

    if (node.leaf) {
      const n = node as BPTreeLeafNode<number, number>
      const keys = new Array(n.keys.length)
      let i = 0
      const len = keys.length
      while (i < len) {
        keys[i] = +(n.keys[i][0])
        i++
      }
      this.indexPageManger.setIndexId(page, +(n.id))
      this.indexPageManger.setParentIndexId(page, +(n.parent as any))
      this.indexPageManger.setNextIndexId(page, +(n.next as any))
      this.indexPageManger.setPrevIndexId(page, +(n.prev as any))
      this.indexPageManger.setIsLeaf(page, n.leaf)
      this.indexPageManger.setKeysCount(page, n.keys.length)
      this.indexPageManger.setValuesCount(page, n.values.length)
      this.indexPageManger.setKeysAndValues(page, keys, n.values)
    }
    else {
      const n = node as BPTreeInternalNode<number, number>
      const keys: number[] = new Array(n.keys.length)
      let i = 0
      const len = keys.length
      while (i < len) {
        keys[i] = +(n.keys[i])
        i++
      }
      this.indexPageManger.setIndexId(page, +(n.id))
      this.indexPageManger.setParentIndexId(page, +(n.parent as any))
      this.indexPageManger.setNextIndexId(page, +(n.next as any))
      this.indexPageManger.setPrevIndexId(page, +(n.prev as any))
      this.indexPageManger.setIsLeaf(page, n.leaf)
      this.indexPageManger.setKeysCount(page, n.keys.length)
      this.indexPageManger.setValuesCount(page, n.values.length)
      this.indexPageManger.setKeysAndValues(page, keys, n.values)
    }
    await this.pfs.setPage(pageId, page, tx)
  }

  async delete(id: string): Promise<void> {
    const tx = this.txContext.get()!
    const manager = this.factory.getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_INDEX)
    let pageId = +(id)
    while (true) {
      const page = await this.pfs.get(pageId, tx)
      const nextPageId = manager.getNextPageId(page)

      // 인덱스 페이지 반환 및 초기화 (기존 로직 대체)
      await this.pfs.setFreePage(pageId, tx)

      if (nextPageId === -1) {
        break
      }
      pageId = nextPageId
    }
  }

  async readHead(): Promise<SerializeStrategyHead | null> {
    const tx = this.txContext.get()!
    const metadataPage = await this.pfs.getMetadata(tx)
    const manager = this.factory.getManager(metadataPage)
    const rootIndexPageId = manager.getRootIndexPageId(metadataPage)
    if (rootIndexPageId === -1) {
      return null
    }
    const metaOrder = manager.getRootIndexOrder(metadataPage)
    const order = metaOrder || this.order

    return {
      root: rootIndexPageId.toString(),
      order,
      data: {}
    }
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    const tx = this.txContext.get()!
    const { root, order } = head
    if (root === null) {
      throw new Error('')
    }
    const metadataPage = await this.pfs.getMetadata(tx)
    const manager = this.factory.getManager(metadataPage)
    manager.setRootIndexPageId(metadataPage, +(root))
    manager.setRootIndexOrder(metadataPage, order)

    await this.pfs.setPage(0, metadataPage, tx)
  }
}
