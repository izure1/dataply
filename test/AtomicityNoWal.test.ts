
import { PageFileSystem } from '../src/core/PageFileSystem'
import { LockManager } from '../src/core/transaction/LockManager'
import { Transaction } from '../src/core/transaction/Transaction'
import fs from 'node:fs'
import path from 'node:path'

class TestPageFileSystem extends PageFileSystem {
  public getVFS() {
    return this.vfs
  }
}

describe('Atomicity No-WAL (Optional WAL)', () => {
  const testDir = path.join(__dirname, 'temp_atomicity_nowal_test')
  const dbPath = path.join(testDir, 'test.db')
  const walPath = path.join(testDir, 'shard.wal') // Should NOT be created
  const pageSize = 1024

  let pfs: TestPageFileSystem
  let fd: number
  let lockManager: LockManager
  let txIdCounter = 0

  const createTx = () => new Transaction(++txIdCounter, pfs.getVFS(), lockManager)

  beforeAll(() => {
    if (!fs.existsSync(testDir)) {
      fs.mkdirSync(testDir)
    }
  })

  afterAll(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true })
    }
  })

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    if (fs.existsSync(walPath)) fs.unlinkSync(walPath)

    fd = fs.openSync(dbPath, 'w+')
    pfs = new TestPageFileSystem(fd, pageSize, undefined)
    lockManager = new LockManager()
    txIdCounter = 0
  })

  afterEach(async () => {
    await pfs.close()
    try { fs.closeSync(fd) } catch (e) { }
  })

  test('should NOT create wal file on commit', async () => {
    const tx = createTx()
    await pfs.setPage(1, new Uint8Array(pageSize).fill(1), tx)
    await tx.commit()

    const files = fs.readdirSync(testDir)
    const walFiles = files.filter(f => f.endsWith('.wal'))
    expect(walFiles.length).toBe(0)
  })

  test('should still allow rollback in memory', async () => {
    // Setup initial state
    const tx1 = createTx()
    await pfs.setPage(1, new Uint8Array(pageSize).fill(10), tx1)
    await tx1.commit()

    // Modify and Rollback
    const tx2 = createTx()
    await pfs.setPage(1, new Uint8Array(pageSize).fill(20), tx2)

    // Check dirty state (20) - tx2 should see its own changes
    let page = await pfs.get(1, tx2)
    expect(page[0]).toBe(20)

    await tx2.rollback()

    // Check restored state (10) - no tx, should see committed data
    page = await pfs.get(1)
    expect(page[0]).toBe(10)
  })

  test('should persist data on commit (direct sync)', async () => {
    const tx = createTx()
    await pfs.setPage(1, new Uint8Array(pageSize).fill(50), tx)
    await tx.commit()

    await pfs.close()
    fs.closeSync(fd)

    // Reopen to verify persistence
    const fd2 = fs.openSync(dbPath, 'r+')
    const pfs2 = new TestPageFileSystem(fd2, pageSize, undefined)

    const page = await pfs2.get(1)
    expect(page[0]).toBe(50)

    await pfs2.close()
    fs.closeSync(fd2)
  })
})
