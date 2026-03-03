import fs from 'node:fs'
import path from 'node:path'
import { DataplyAPI } from '../src/core/DataplyAPI'

describe('TransactionSerialize', () => {
  const DB_PATH = path.join(__dirname, 'serialize_test.db')

  beforeEach(() => {
    if (fs.existsSync(DB_PATH)) {
      fs.unlinkSync(DB_PATH)
    }
  })

  afterEach(async () => {
    if (fs.existsSync(DB_PATH)) {
      try { fs.unlinkSync(DB_PATH) } catch (e) { }
    }
  })

  test('commit should wait for concurrent insertBatch to finish (Promise.all pattern)', async () => {
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const tx = db.createTransaction()

    // Promise.all으로 insertBatch와 commit을 동시에 실행
    // serialize가 없다면 commit이 insert 도중에 실행되어 데이터 손실 가능
    const dataList = Array.from({ length: 20 }, (_, i) => `data-${i}`)
    const [pks] = await Promise.all([
      db.insertBatch(dataList, true, tx),
      tx.commit()
    ])

    // commit 이후에도 모든 데이터가 정상적으로 조회되어야 함
    for (let i = 0; i < pks.length; i++) {
      const result = await db.select(pks[i], false)
      expect(result).toBe(`data-${i}`)
    }

    // 메타데이터 rowCount도 정확해야 함
    const metadata = await db.getMetadata()
    expect(metadata.rowCount).toBe(20)

    await db.close()
  })

  test('rollback should wait for concurrent insert to finish', async () => {
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const tx = db.createTransaction()

    // insert와 rollback을 동시에 호출
    const [pk] = await Promise.all([
      db.insert('test-data', true, tx),
      tx.rollback()
    ])

    // rollback이 insert 후에 실행되므로, 데이터는 커밋되지 않음
    const result = await db.select(pk, false)
    expect(result).toBe(null)

    await db.close()
  })

  test('multiple concurrent operations on same tx should serialize correctly', async () => {
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const tx = db.createTransaction()

    // 여러 insert를 동시에 호출해도 직렬화되어 정상 동작해야 함
    const [pk1, pk2, pk3] = await Promise.all([
      db.insert('first', true, tx),
      db.insert('second', true, tx),
      db.insert('third', true, tx),
    ])

    await tx.commit()

    // 모든 데이터가 정상 조회되어야 함
    expect(await db.select(pk1, false)).toBe('first')
    expect(await db.select(pk2, false)).toBe('second')
    expect(await db.select(pk3, false)).toBe('third')

    const metadata = await db.getMetadata()
    expect(metadata.rowCount).toBe(3)

    await db.close()
  })

  test('serialize should preserve execution order (FIFO)', async () => {
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    const tx = db.createTransaction()

    // 순서: insert → delete → commit
    // serialize가 FIFO를 보장하므로 insert가 먼저 완료되고, delete가 실행된 후 commit
    const pk = await db.insert('to-delete', true, tx)

    await Promise.all([
      db.delete(pk, true, tx),
      tx.commit()
    ])

    // delete가 commit보다 먼저 실행되어야 하므로 데이터는 삭제됨
    const result = await db.select(pk, false)
    expect(result).toBe(null)

    const metadata = await db.getMetadata()
    expect(metadata.rowCount).toBe(0)

    await db.close()
  })
})
