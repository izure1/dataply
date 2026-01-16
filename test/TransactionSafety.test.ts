import fs from 'node:fs'
import path from 'node:path'
import { VirtualFileSystem } from '../src/core/VirtualFileSystem'
import { Transaction } from '../src/core/transaction/Transaction'
import { LockManager } from '../src/core/transaction/LockManager'
import { TransactionContext } from '../src/core/transaction/TxContext'

describe('TransactionSafety', () => {
  const TEST_FILE = path.join(__dirname, 'test_safety.dat')
  let fd: number
  let vfs: VirtualFileSystem
  let lockManager: LockManager
  let txContext: TransactionContext

  beforeEach(() => {
    if (fs.existsSync(TEST_FILE)) {
      fs.unlinkSync(TEST_FILE)
    }
    fd = fs.openSync(TEST_FILE, 'w+')
    lockManager = new LockManager()
    txContext = new TransactionContext()
  })

  afterEach(async () => {
    if (vfs) {
      await vfs.close()
    }
    if (fd) {
      try {
        fs.closeSync(fd)
      } catch (e) { }
    }
    if (fs.existsSync(TEST_FILE)) {
      try {
        fs.unlinkSync(TEST_FILE)
      } catch (e) { }
    }
  })

  test('should not lose data when cache evicts dirty pages (Transaction Pinning)', async () => {
    const pageSize = 1024
    const cacheCapacity = 2 // 매우 작은 캐시 용량 (2페이지)
    vfs = new VirtualFileSystem(fd, pageSize, cacheCapacity)

    const tx = new Transaction(1, txContext, vfs, lockManager)

    // 1. 5개의 페이지에 데이터를 씀 (캐시 용량 2를 초과)
    const pageDataList = [
      new Uint8Array(pageSize).fill(1),
      new Uint8Array(pageSize).fill(2),
      new Uint8Array(pageSize).fill(3),
      new Uint8Array(pageSize).fill(4),
      new Uint8Array(pageSize).fill(5),
    ]

    for (let i = 0; i < 5; i++) {
      await vfs.write(i * pageSize, pageDataList[i], tx)
    }

    // 2. 캐시에서 쫓겨났을 첫 번째 페이지를 조회 (트랜잭션 Dirty Pages에서 가져와야 함)
    const readPage0 = await vfs.read(0, pageSize, tx)
    expect(readPage0).toEqual(pageDataList[0])

    const readPage2 = await vfs.read(2 * pageSize, pageSize, tx)
    expect(readPage2).toEqual(pageDataList[2])

    // 3. 커밋 수행
    await tx.commit()

    // 4. 디스크 파일 크기 확인 (5페이지 분량)
    const stats = fs.fstatSync(fd)
    expect(stats.size).toBe(pageSize * 5)

    // 5. 디스크에서 직접 읽어서 데이터 정합성 확인
    for (let i = 0; i < 5; i++) {
      const diskBuf = Buffer.alloc(pageSize)
      fs.readSync(fd, diskBuf, 0, pageSize, i * pageSize)
      expect(new Uint8Array(diskBuf)).toEqual(pageDataList[i])
    }
  })

  test('should prioritize transaction dirty pages over VFS cache', async () => {
    const pageSize = 1024
    vfs = new VirtualFileSystem(fd, pageSize, 100)
    const tx = new Transaction(1, txContext, vfs, lockManager)

    // 1. 페이지 0에 데이터 A를 씀
    const dataA = new Uint8Array(pageSize).fill(0xA)
    await vfs.write(0, dataA, tx)

    // 2. 같은 페이지 0에 데이터 B를 덮어씀
    const dataB = new Uint8Array(pageSize).fill(0xB)
    await vfs.write(0, dataB, tx)

    // 3. 조회 시 데이터 B(트랜잭션 내 최신)가 나와야 함
    const read = await vfs.read(0, pageSize, tx)
    expect(read).toEqual(dataB)

    await tx.commit()
  })

  test('rollback should properly restore cache using undo pages', async () => {
    const pageSize = 1024
    vfs = new VirtualFileSystem(fd, pageSize, 100)

    // 초기 데이터 작성 및 커밋
    const initialData = new Uint8Array(pageSize).fill(0x1)
    const tx1 = new Transaction(1, txContext, vfs, lockManager)
    await vfs.write(0, initialData, tx1)
    await tx1.commit()

    // 수정 후 롤백
    const tx2 = new Transaction(2, txContext, vfs, lockManager)
    const modifiedData = new Uint8Array(pageSize).fill(0x2)
    await vfs.write(0, modifiedData, tx2)

    // 트랜잭션 내에서는 수정된 데이터가 보여야 함
    expect(await vfs.read(0, pageSize, tx2)).toEqual(modifiedData)

    await tx2.rollback()

    // 롤백 후에는 다시 초기 데이터가 보여야 함 (캐시가 복구되어야 함)
    const tx3 = new Transaction(3, txContext, vfs, lockManager)
    const readAfterRollback = await vfs.read(0, pageSize, tx3)
    expect(readAfterRollback).toEqual(initialData)
  })
})
