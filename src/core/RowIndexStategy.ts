import { type BPTreeNode, type SerializeStrategyHead, SerializeStrategyAsync } from 'serializable-bptree'
import { PageFileSystem } from './PageFileSystem'
import { IndexPageManager, PageManager, PageManagerFactory } from './Page'
import { TextCodec } from '../utils/TextCodec'
import { TxContext } from './transaction/TxContext'

export class RowIdentifierStrategy extends SerializeStrategyAsync<number, number> {
  protected rootPageId = 0
  protected factory: PageManagerFactory
  protected indexPageManger: IndexPageManager
  protected codec: TextCodec

  constructor(readonly order: number, protected readonly pfs: PageFileSystem) {
    super(order)
    this.factory = new PageManagerFactory()
    this.indexPageManger = this.factory.getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_INDEX) as IndexPageManager
    this.codec = new TextCodec()
  }

  async id(isLeaf: boolean): Promise<string> {
    const tx = TxContext.get()!
    const pageId = await this.pfs.appendNewPage(PageManager.CONSTANT.PAGE_TYPE_INDEX, tx)
    return pageId.toString()
  }

  async read(id: string): Promise<BPTreeNode<number, number>> {
    const tx = TxContext.get()!
    const pageId = parseInt(id)
    const page = await this.pfs.getBody(pageId, true, tx)
    const text = this.codec.decode(page)
    return JSON.parse(text)
  }

  async write(id: string, node: BPTreeNode<number, number>): Promise<void> {
    const tx = TxContext.get()!
    const pageId = parseInt(id)
    const text = JSON.stringify(node)
    const source = this.codec.encode(text)
    await this.pfs.writePageContent(pageId, source, 0, tx)
  }

  async delete(id: string): Promise<void> {
    const tx = TxContext.get()!
    const manager = this.factory.getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_EMPTY)
    let pageId = parseInt(id)
    while (true) {
      const page = await this.pfs.get(pageId, tx)
      const nextPageId = manager.getNextPageId(page)
      manager.setNextPageId(page, -1)
      manager.setRemainingCapacity(page, this.pfs.pageSize - PageManager.CONSTANT.SIZE_PAGE_HEADER)
      if (nextPageId === -1) {
        break
      }
      pageId = nextPageId
    }
  }

  async readHead(): Promise<SerializeStrategyHead | null> {
    const tx = TxContext.get()!
    const metadataPage = await this.pfs.getMetadata(tx)
    const manager = this.factory.getManager(metadataPage)
    const rootIndexPageId = manager.getRootIndexPageId(metadataPage)
    if (rootIndexPageId === -1) {
      return null
    }
    return {
      root: manager.getRootIndexPageId(metadataPage).toString(),
      order: manager.getRootIndexOrder(metadataPage),
      data: {}
    }
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    const tx = TxContext.get()!
    const { root, order } = head
    if (root === null) {
      throw new Error('')
    }

    const metadataPage = await this.pfs.getMetadata(tx)
    const manager = this.factory.getManager(metadataPage)
    manager.setRootIndexPageId(metadataPage, parseInt(root))
    manager.setRootIndexOrder(metadataPage, order)

    await this.pfs.setPage(0, metadataPage, tx)
  }
}
