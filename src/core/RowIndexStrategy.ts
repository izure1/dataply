import type { BPTreeInternalNode, BPTreeLeafNode, BPTreeNode, SerializeStrategyHead } from 'serializable-bptree'
import { SerializeStrategyAsync } from 'serializable-bptree'
import { PageFileSystem } from './PageFileSystem'
import { IndexPageManager, PageManager, PageManagerFactory } from './Page'
import { TextCodec } from '../utils/TextCodec'
import { TxContext } from './transaction/TxContext'
import fs from 'node:fs'

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
    const pageId = +(id)
    const raw = await this.pfs.getBody(pageId, true, tx)
    const stringify = this.codec.decode(raw)
    return JSON.parse(stringify)
  }
  // async read(id: string): Promise<BPTreeNode<number, number>> {
  //   const tx = TxContext.get()!
  //   const pageId = +(id)
  //   const page = await this.pfs.get(pageId, tx)

  //   const indexId = this.indexPageManger.getIndexId(page)
  //   const parentIndexId = this.indexPageManger.getParentIndexId(page)
  //   const nextIndexId = this.indexPageManger.getNextIndexId(page)
  //   const prevIndexId = this.indexPageManger.getPrevIndexId(page)
  //   const isLeaf = this.indexPageManger.getIsLeaf(page)
  //   const keys = this.indexPageManger.getKeys(page)
  //   const values = this.indexPageManger.getValues(page)

  //   let res: BPTreeLeafNode<number, number> | BPTreeInternalNode<number, number>
  //   if (isLeaf) res = {
  //     leaf: true,
  //     id: indexId + '',
  //     parent: parentIndexId ? parentIndexId + '' : null,
  //     next: nextIndexId ? nextIndexId + '' : null,
  //     prev: prevIndexId ? prevIndexId + '' : null,
  //     keys: keys.map((key) => [key]),
  //     values
  //   }
  //   else res = {
  //     leaf: false,
  //     id: indexId + '',
  //     parent: parentIndexId ? parentIndexId + '' : null,
  //     next: nextIndexId ? nextIndexId + '' : null,
  //     prev: prevIndexId ? prevIndexId + '' : null,
  //     keys: keys.map((key) => key + ''),
  //     values
  //   }
  //   return res
  // }

  async write(id: string, node: BPTreeNode<number, number>): Promise<void> {
    const tx = TxContext.get()!
    const pageId = +(id)
    const raw = this.codec.encode(JSON.stringify(node))
    await this.pfs.writePageContent(pageId, raw, 0, tx)
    await fs.promises.writeFile(`node_${pageId}.json`, JSON.stringify(node, null, 2))
  }
  // async write(id: string, node: BPTreeNode<number, number>): Promise<void> {
  //   const tx = TxContext.get()!
  //   const pageId = +(id)
  //   const page = await this.pfs.get(pageId, tx)
  //   if (node.leaf) {
  //     const n = node as BPTreeLeafNode<number, number>
  //     const keys = new Array(n.keys.length)
  //     for (let i = 0, len = keys.length; i < len; i++) {
  //       keys[i] = +(n.keys[i][0])
  //     }
  //     this.indexPageManger.setIndexId(page, +(n.id))
  //     this.indexPageManger.setParentIndexId(page, +(n.parent as any))
  //     this.indexPageManger.setNextIndexId(page, +(n.next as any))
  //     this.indexPageManger.setPrevIndexId(page, +(n.prev as any))
  //     this.indexPageManger.setIsLeaf(page, n.leaf)
  //     this.indexPageManger.setKeysCount(page, n.keys.length)
  //     this.indexPageManger.setValuesCount(page, n.values.length)
  //     this.indexPageManger.setKeysAndValues(page, keys, n.values)
  //   }
  //   else {
  //     const n = node as BPTreeInternalNode<number, number>
  //     const keys: number[] = new Array(n.keys.length)
  //     for (let i = 0, len = keys.length; i < len; i++) {
  //       keys[i] = +(n.keys[i])
  //     }
  //     this.indexPageManger.setIndexId(page, +(n.id))
  //     this.indexPageManger.setParentIndexId(page, +(n.parent as any))
  //     this.indexPageManger.setNextIndexId(page, +(n.next as any))
  //     this.indexPageManger.setPrevIndexId(page, +(n.prev as any))
  //     this.indexPageManger.setIsLeaf(page, n.leaf)
  //     this.indexPageManger.setKeysCount(page, n.keys.length)
  //     this.indexPageManger.setValuesCount(page, n.values.length)
  //     this.indexPageManger.setKeysAndValues(page, keys, n.values)
  //   }
  //   // await fs.promises.writeFile(`node_${pageId}.json`, JSON.stringify(node, null, 2))
  //   await this.pfs.setPage(pageId, page, tx)
  // }

  async delete(id: string): Promise<void> {
    const tx = TxContext.get()!
    const manager = this.factory.getManagerFromType(PageManager.CONSTANT.PAGE_TYPE_EMPTY)
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
    manager.setRootIndexPageId(metadataPage, +(root))
    manager.setRootIndexOrder(metadataPage, order)

    await this.pfs.setPage(0, metadataPage, tx)
  }
}
