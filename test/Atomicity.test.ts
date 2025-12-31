
import { PageFileSystem } from '../src/core/PageFileSystem'
import { VirtualFileSystem } from '../src/core/VirtualFileSystem'
import { Transaction } from '../src/core/transaction/Transaction'
import { LockManager } from '../src/core/transaction/LockManager'
import fs from 'node:fs'
import path from 'node:path'

// Mock VFS acessor since it is protected in PFS
class TestPageFileSystem extends PageFileSystem {
  public getVFS(): VirtualFileSystem {
    return this.vfs
  }
}

describe('Atomicity (Transaction)', () => {
  const testDir = path.join(__dirname, 'temp_atomicity_test')
  const dbPath = path.join(testDir, 'test.db')
  const walPath = path.join(testDir, 'shard.wal')
  const pageSize = 1024

  let pfs: TestPageFileSystem
  let fd: number
  let lockManager: LockManager
  let txIdCounter = 0

  beforeAll(() => {
    if (!fs.existsSync(testDir)) {
      fs.mkdirSync(testDir)
    }
  })

  afterAll(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true })
    }
    if (fs.existsSync(walPath)) {
      fs.unlinkSync(walPath)
    }
  })

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    if (fs.existsSync(walPath)) fs.unlinkSync(walPath)

    fd = fs.openSync(dbPath, 'w+')
    pfs = new TestPageFileSystem(fd, pageSize, walPath)
    lockManager = new LockManager()
    txIdCounter = 0

    // Initialize Metadata (Page 0) manually to avoid issues with basic page allocation
    const metaPage = new Uint8Array(pageSize) // Empty page 0
    // We rely on auto-commit logic of setPage here (no tx passed)
    // Page 0 becomes metadata.
    await pfs.setPage(0, metaPage)
  })

  afterEach(async () => {
    await pfs.close()
    try { fs.closeSync(fd) } catch (e) { }
    if (fs.existsSync(walPath)) fs.unlinkSync(walPath)
  })

  const createTx = () => {
    // Note: In real app, Shard/LockManager helps create this.
    return new Transaction(++txIdCounter, pfs.getVFS(), lockManager)
  }

  test('should rollback changes (undo memory)', async () => {
    const tx = createTx()

    // 1. Modify Page 1
    const page1 = new Uint8Array(pageSize).fill(1)
    await pfs.setPage(1, page1, tx)

    // 2. Modify Page 2
    const page2 = new Uint8Array(pageSize).fill(2)
    await pfs.setPage(2, page2, tx)

    // 3. Rollback
    await tx.rollback()

    // 4. Verify rollback
    const readPage1 = await pfs.get(1)
    const readPage2 = await pfs.get(2)

    // Should be empty/zeros
    expect(readPage1).toEqual(new Uint8Array(pageSize))
    expect(readPage2).toEqual(new Uint8Array(pageSize))
  })

  test('should commit changes (persist to WAL/Disk)', async () => {
    const tx = createTx()

    const page1 = new Uint8Array(pageSize).fill(5)
    await pfs.setPage(1, page1, tx)

    await tx.commit()

    // Close and reopen to verify persistence
    await pfs.close()
    fd = fs.openSync(dbPath, 'r+')
    const pfs2 = new TestPageFileSystem(fd, pageSize, walPath)

    const readPage1 = await pfs2.get(1)
    expect(readPage1).toEqual(page1)

    await pfs2.close()
  })

  test('should recover from crash (Redo from WAL)', async () => {
    const { LogManager } = require('../src/core/LogManager')
    const logManager = new LogManager(walPath, pageSize)
    await logManager.open()

    const pages = new Map<number, Uint8Array>()
    const page1Data = new Uint8Array(pageSize).fill(99)
    pages.set(1, page1Data) // Simulatre Page 1 being in WAL

    await logManager.append(pages)
    await logManager.close()

    await pfs.close()

    if (fs.existsSync(walPath)) {
      console.log('WAL Size before recovery:', fs.statSync(walPath).size)
    }

    fd = fs.openSync(dbPath, 'r+')
    const pfsRecovery = new TestPageFileSystem(fd, pageSize, walPath)

    const recoveredPage1 = await pfsRecovery.get(1)
    expect(recoveredPage1).toEqual(page1Data)

    await pfsRecovery.close()
  })

  test('should handle multiple sequential transactions', async () => {
    // Tx 1: Set Page 1 = 10
    const tx1 = createTx()
    await pfs.setPage(1, new Uint8Array(pageSize).fill(10), tx1)
    await tx1.commit()

    // Tx 2: Set Page 2 = 20
    const tx2 = createTx()
    await pfs.setPage(2, new Uint8Array(pageSize).fill(20), tx2)
    await tx2.commit()

    // Tx 3: Modify Page 1 -> 30 but Rollback
    const tx3 = createTx()
    await pfs.setPage(1, new Uint8Array(pageSize).fill(30), tx3)
    await tx3.rollback()

    // Verify
    const page1 = await pfs.get(1)
    const page2 = await pfs.get(2)

    expect(page1).toEqual(new Uint8Array(pageSize).fill(10)) // From Tx 1 (unchanged by Tx 3)
    expect(page2).toEqual(new Uint8Array(pageSize).fill(20)) // From Tx 2
  })

  test('should rollback properly when modifying same page multiple times', async () => {
    // Setup: Page 1 = 10
    const tx1 = createTx()
    await pfs.setPage(1, new Uint8Array(pageSize).fill(10), tx1)
    await tx1.commit()

    const tx2 = createTx()
    // Modify 1 -> 20
    await pfs.setPage(1, new Uint8Array(pageSize).fill(20), tx2)
    // Modify 1 -> 30 (overwrite dirty page)
    await pfs.setPage(1, new Uint8Array(pageSize).fill(30), tx2)

    // Verify current state in memory from within transaction context (if we supported reading uncommitted data? but we don't have isolation levels yet)
    // Wait, pfs.get(1, tx2) SHOULD read the dirty page from tx2 context if implemented.
    // VirtualFileSystem.read doesn't check undoBuffers/dirtyPages for read-your-own-writes?
    // Let's check VFS.read.
    // VFS.read calls _readPage. _readPage checks this.cache.
    // VFS.write update this.cache AND puts logic in txDirtyPages.
    // So YES, since cache is updated, we read our own writes.
    // BUT, does cache getting updated mean OTHER transactions see it?
    // YES. VirtualFileSystem currently has a SINGLE cache.
    // This implies READ UNCOMMITTED isolation level effectively for in-memory pages?
    // Wait, if Tx A writes to Page 1, it updates `this.cache`.
    // Tx B reads Page 1, it gets `this.cache`.
    // So Tx B sees Tx A's writes immediately?
    // THIS IS A PROBLEM if we want Read Committed or Repeatable Read.
    // But for "Atomicity" test it's fine.
    // Ideally VFS should overlay tx-specific pages on top of global cache.
    // VFS Implementation Review:
    // write(...) -> this.cache.set(pageIndex, page).
    // This confirms global visibility of uncommitted writes within the node process.
    // Fix later: VFS needs `txCache`.
    // For now, let's proceed with this test assuming internal visibility is verified.

    expect((await pfs.get(1, tx2))[0]).toBe(30)

    await tx2.rollback()

    // Should return to 10 (committed state)
    // Rollback logic: restores from undoBuffers to this.cache.
    expect((await pfs.get(1))[0]).toBe(10)
  })
})
