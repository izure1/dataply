import fs from 'node:fs'
import path from 'node:path'
import { DataplyAPI } from '../src/core/DataplyAPI'

describe('TransactionSafety (DataplyAPI)', () => {
  const DB_PATH = path.join(__dirname, 'safety_integrated.db')

  beforeEach(() => {
    if (fs.existsSync(DB_PATH)) {
      fs.unlinkSync(DB_PATH)
    }
  })

  afterEach(async () => {
    if (fs.existsSync(DB_PATH)) {
      try {
        fs.unlinkSync(DB_PATH)
      } catch (e) { }
    }
  })

  test('Isolation: Disk size should not increase before commit', async () => {
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    // 초기 상태의 물리적 디스크 크기 측정
    const initialDiskSize = fs.statSync(DB_PATH).size

    const tx = db.createTransaction()

    // 1. 대량의 데이터를 삽입하여 파일 확장이 필요한 상황을 만듦
    // (보통 3페이지 초기화 상태이므로, 그 이상의 데이터를 넣음)
    const largeData = new Uint8Array(5000).fill(0x1)
    const pks: number[] = []

    // 여러 번 삽입하여 확실히 페이지 확장을 유도
    for (let i = 0; i < 10; i++) {
      pks.push(await db.insert(largeData, true, tx))
    }

    // [검증 1: 격리성] 커밋 전이므로 물리적 디스크 크기는 변하지 않아야 함
    const currentDiskSize = fs.statSync(DB_PATH).size
    expect(currentDiskSize).toBe(initialDiskSize)

    // [검증 2: 가시성] 트랜잭션 내부(Uncommitted)에서는 데이터가 조회가 되어야 함
    for (const pk of pks) {
      const selected = await db.select(pk, true, tx)
      expect(selected).toEqual(largeData)
    }

    // 2. 커밋 수행
    await tx.commit()

    // [검증 3: 지속성] 커밋 후에는 물리적 디스크 크기가 늘어나 있어야 함
    const finalDiskSize = fs.statSync(DB_PATH).size
    expect(finalDiskSize).toBeGreaterThan(initialDiskSize)

    // [검증 4: 데이터 정합성] 새로운 트랜잭션에서도 데이터가 잘 보여야 함
    const selectedAfter = await db.select(pks[0], true)
    expect(selectedAfter).toEqual(largeData)

    await db.close()
  })

  test('Safety: Data should not be lost when cache is small (LRU Eviction resilience)', async () => {
    // 캐시 용량을 매우 작게 설정 (데이터 페이지 2개 분량 수준)
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096, pageCacheCapacity: 100 })
    await db.init()

    const tx = db.createTransaction()

    // 10개의 각기 다른 페이지에 쓰기가 발생하도록 유도 (데이터 크기와 횟수 조절)
    const dataSize = 2000 // 한 페이지에 약 2개 들어감
    const pks: number[] = []

    for (let i = 0; i < 20; i++) {
      const data = new Uint8Array(dataSize).fill(i)
      pks.push(await db.insert(data, true, tx))
    }

    // 중간 조회: 캐시에서 쫓겨났을 법한 첫 번째 데이터 확인
    // (Transaction이 Dirty Page를 본딩하고 있으므로 성공해야 함)
    const firstData = await db.select(pks[0], true, tx)
    expect(firstData).toEqual(new Uint8Array(dataSize).fill(0))

    await tx.commit()

    // 커밋 후 전체 데이터 재검증
    for (let i = 0; i < 20; i++) {
      const selected = await db.select(pks[i], true)
      expect(selected).toEqual(new Uint8Array(dataSize).fill(i))
    }

    await db.close()
  })

  test('Rollback: Restores previous state and does not affect disk if rolled back', async () => {
    const db = new DataplyAPI(DB_PATH, { pageSize: 4096 })
    await db.init()

    // 1. 초기 데이터 삽입 및 커밋
    const initialPk = await db.insert("initial")
    const diskSizeAfterFirst = fs.statSync(DB_PATH).size

    // 2. 수정 트랜잭션 시작
    const tx = db.createTransaction()
    await db.update(initialPk, "modified", tx)
    await db.insert("new data", true, tx)

    // 트랜잭션 내 확인
    expect(await db.select(initialPk, false, tx)).toBe("modified")

    // 3. 롤백 수행
    await tx.rollback()

    // [검증] 물리적 디스크 크기가 변하지 않았는지 확인
    expect(fs.statSync(DB_PATH).size).toBe(diskSizeAfterFirst)

    // [검증] 데이터가 원복되었는지 확인
    expect(await db.select(initialPk)).toBe("initial")

    await db.close()
  })
})
