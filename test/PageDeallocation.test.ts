
import fs from 'node:fs'
import path from 'node:path'
import { DataplyAPI } from '../src/core/DataplyAPI'
import { PageManagerFactory, MetadataPageManager, BitmapPageManager, PageManager } from '../src/core/Page'
import { PageFileSystem } from '../src/core/PageFileSystem'
import type { BitmapPage } from '../src/types'

const TEST_DB = path.join(__dirname, 'page_deallocation_test.db')

describe('Page Deallocation', () => {
  // Individual cleanup ensures isolation
  const cleanup = async () => {
    if (fs.existsSync(TEST_DB)) await fs.promises.unlink(TEST_DB)
  }

  beforeEach(cleanup)
  afterEach(cleanup)

  test('should free data page when it becomes empty', async () => {
    const db = DataplyAPI.Use(TEST_DB, { pageSize: 4096, pageCacheCapacity: 100 })
    await db.init()

    // 1. Insert rows to fill a page partially
    const pks: number[] = []
    // Page size 4096. Insert enough data to fill the first page and move to second.
    // Insert 50 rows of 100 bytes -> 5000 bytes. > 4096. 
    // This should create a second page.
    for (let i = 0; i < 50; i++) {
      pks.push(await db.insert(new Uint8Array(100)))
    }

    const pageId = 2

    // 2. Delete all rows
    for (const pk of pks) {
      await db.delete(pk)
    }

    const pfs = (db as any).pfs as PageFileSystem
    const tx = db.createTransaction()

    // Verify page is now EMPTY
    const page = await pfs.get(pageId, tx)
    const pageType = PageManager.GetPageType(page)

    await tx.commit()

    expect(pageType).toBe(PageManager.CONSTANT.PAGE_TYPE_EMPTY)
    await db.close()
  })

  test('should free overflow pages when overflow row is deleted', async () => {
    const db = DataplyAPI.Use(TEST_DB, { pageSize: 4096, pageCacheCapacity: 100 })
    await db.init()

    // 1. Insert a large row that creates overflow pages
    // Page size 4096, Header 100. Body ~4000. 
    // Insert 10000 bytes -> 1 Data Page + 2~3 Overflow Pages.
    const largeData = new Uint8Array(10000)
    const pk = await db.insert(largeData)

    const pfs = (db as any).pfs as PageFileSystem
    let tx = db.createTransaction()

    // Find the overflow page dynamically starting from page 3
    let overflowPageId = -1
    for (let i = 3; i < 20; i++) {
      const p = await pfs.get(i, tx)
      if (new PageManagerFactory().isOverflowPage(p)) {
        overflowPageId = i
        break
      }
    }

    if (overflowPageId === -1) {
      throw new Error('Could not find any overflow page')
    }

    const overflowPage = await pfs.get(overflowPageId, tx)
    const isOverflow = (new PageManagerFactory().isOverflowPage(overflowPage))
    expect(isOverflow).toBe(true)

    await tx.commit()

    // 2. Delete the row
    await db.delete(pk)

    // 3. Verify overflow page is now EMPTY
    tx = db.createTransaction()
    const freePage = await pfs.get(overflowPageId, tx)
    const finalPageType = PageManager.GetPageType(freePage)

    await tx.commit()

    expect(finalPageType).toBe(PageManager.CONSTANT.PAGE_TYPE_EMPTY)
    await db.close()
  })

  test('should handle bitmap overflow', async () => {
    // PageSize 4096 bytes (Minimum allowed)
    const db = DataplyAPI.Use(TEST_DB, { pageSize: 4096, pageCacheCapacity: 100 })
    await db.init()

    const pfs = (db as any).pfs as PageFileSystem
    const tx = db.createTransaction()

    const highPageId = 33000 // > 31968 capacity ((4096-100)*8)
    await pfs.setFreePage(highPageId, tx)

    // ... verification logic ...
    const factory = new PageManagerFactory()
    const metadata = await pfs.getMetadata(tx)
    const metadataManager = factory.getManager(metadata) as MetadataPageManager
    const firstBitmapPageId = metadataManager.getBitmapPageId(metadata)

    const firstBitmapPage = await pfs.get(firstBitmapPageId, tx)
    const firstBitmapManager = factory.getManager(firstBitmapPage) as BitmapPageManager
    const nextBitmapPageId = firstBitmapManager.getNextPageId(firstBitmapPage)

    expect(nextBitmapPageId).not.toBe(-1)

    const secondBitmapPage = await pfs.get(nextBitmapPageId, tx) as BitmapPage
    const secondBitmapManager = factory.getManager(secondBitmapPage) as BitmapPageManager

    const expectedBitIndex = highPageId - ((4096 - 100) * 8)
    const isSet = secondBitmapManager.getBit(secondBitmapPage, expectedBitIndex)

    await tx.commit()

    expect(isSet).toBe(true)
    await db.close()
  })
})
