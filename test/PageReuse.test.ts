
import fs from 'node:fs'
import path from 'node:path'
import { DataplyAPI } from '../src/core/DataplyAPI'
import { PageManagerFactory, MetadataPageManager, BitmapPageManager, PageManager, DataPageManager } from '../src/core/Page'
import { PageFileSystem } from '../src/core/PageFileSystem'
import type { BitmapPage } from '../src/types'

const TEST_DB = path.join(__dirname, 'page_reuse_test.db')

describe('Page Reuse', () => {
  const cleanup = async () => {
    if (fs.existsSync(TEST_DB)) await fs.promises.unlink(TEST_DB)
  }

  beforeEach(cleanup)
  afterEach(cleanup)

  test('should reuse freed pages', async () => {
    const db = new DataplyAPI(TEST_DB, { pageSize: 4096, pageCacheCapacity: 100 })

    await db.init()
    const pfs = (db as any).pfs as PageFileSystem

    // Page Capacity: 4096 - 100 (Header) = 3996 bytes body.
    // Row: Header (13) + SlotOffset (2) + Body.
    // If Body = 1000. Total per row ~= 1015 bytes.
    // 3 rows = 3045 bytes. 4 rows = 4060 bytes (Overflows).
    // So max 3 rows per page (with 1000 bytes body).

    const dataSize = 1000
    const pks: number[] = []

    // 1. Fill Page 2 (3 rows)
    console.log('--- Phase 1: Allocating Page 2 ---')
    for (let i = 0; i < 3; i++) pks.push(await db.insert(new Uint8Array(dataSize)))

    // 2. Fill Page 3 (3 rows)
    console.log('--- Phase 1: Allocating Page 3 ---')
    for (let i = 0; i < 3; i++) pks.push(await db.insert(new Uint8Array(dataSize)))

    // 3. Fill Page 4 Partially (1 row) -> Last Insert Page
    console.log('--- Phase 1: Allocating Page 4 (Partial) ---')
    const lastPagePk = await db.insert(new Uint8Array(dataSize))
    pks.push(lastPagePk)

    let tx = db.createTransaction()
    const pageCountBeforeDelete = await pfs.getPageCount(tx)
    console.log('Page Count Before Delete:', pageCountBeforeDelete) // Expect 5 (Meta, Bitmap, Data2, Data3, Data4)
    await tx.commit()

    // 4. Delete rows in Page 2 and Page 3 to free them
    // Pks 1-6 are in Page 2 and 3.
    console.log('--- Phase 2: Deleting rows from Page 2 and 3 ---')
    for (let i = 0; i < 6; i++) {
      await db.delete(pks[i])
    }

    // Verify Free List
    tx = db.createTransaction()
    const metadata = await pfs.getMetadata(tx)
    const metadataManager = new PageManagerFactory().getManager(metadata) as MetadataPageManager
    const freeHead = metadataManager.getFreePageId(metadata)
    console.log('Free Head after delete:', freeHead)

    // We expect Page 2 and 3 to be in the Free List (Order depends on delete order, usually LIFO)
    // Since we deleted sequentially, Page 2 freed first, then Page 3.
    // Stack: Metadata -> 3 -> 2 -> -1

    expect(freeHead).not.toBe(-1)
    await tx.commit()

    // 5. Trigger Reuse
    // Currently Last Insert Page is Page 4. It has 1 row.
    // We need to fill Page 4 to force appendNewPage.
    // Capacity 3. Has 1. Need 2 more to fill.
    // The 3rd insert should check for space, fail, and call appendNewPage.

    console.log('--- Phase 3: Filling Last Page (Page 4) ---')
    // Insert 2 more rows to fill Page 4
    await db.insert(new Uint8Array(dataSize)) // 2nd row in Page 4
    await db.insert(new Uint8Array(dataSize)) // 3rd row in Page 4 (Full)

    console.log('--- Phase 3: Triggering Allocation (Should Reuse) ---')
    // This insert should trigger new page allocation
    await db.insert(new Uint8Array(dataSize))

    // Verify
    tx = db.createTransaction()

    // Check Metadata Last Insert Page ID
    const metaReuse = await pfs.getMetadata(tx)
    const lastInsertPageId = metadataManager.getLastInsertPageId(metaReuse)
    console.log('New Last Insert Page ID (Should be Reused):', lastInsertPageId)

    // Instead of hardcoding 2 or 3, we verify that the page ID is one that was previously allocated
    // and that the total page count did NOT increase (proving reuse).

    const finalPageCount = await pfs.getPageCount(tx)
    console.log('Final Page Count:', finalPageCount)

    // Page Count should be same as before delete (initially 6, after delete 6, after reuse 6)
    // Because we reused a page, we didn't append a new one.
    expect(finalPageCount).toBe(pageCountBeforeDelete)

    // Check Bitmap
    const bitmapPageId = metadataManager.getBitmapPageId(metaReuse)
    const bitmapPage = await pfs.get(bitmapPageId, tx) as BitmapPage
    const bitmapManager = new PageManagerFactory().getManager(bitmapPage) as BitmapPageManager

    // The reused page should be marked Used (0)
    const isUsed = !bitmapManager.getBit(bitmapPage, lastInsertPageId)
    console.log(`Bitmap for Reused Page ${lastInsertPageId} (expect Used/False):`, bitmapManager.getBit(bitmapPage, lastInsertPageId))
    expect(isUsed).toBe(true)

    await tx.commit()

    await db.close()
  })
})
