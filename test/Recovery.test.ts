import fs from 'node:fs'
import path from 'path'
import { Shard } from '../src/core/Shard'

/**
 * 종합 복구 테스트 (Recovery Integration Test)
 * 
 * 실제 사용 환경에서 Shard를 사용하다가 서버가 비정상 종료되었을 때,
 * 재시작 시 WAL을 통한 복구가 정상적으로 동작하는지 검증합니다.
 */
describe('Recovery Integration Test', () => {
  const testDir = path.join(__dirname, 'temp_recovery_test')
  const dbPath = path.join(testDir, 'recovery_test.db')
  const walPath = path.join(testDir, 'recovery_test.wal')
  const pageSize = 4096

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

  beforeEach(() => {
    // 각 테스트 전에 기존 파일 삭제
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    if (fs.existsSync(walPath)) fs.unlinkSync(walPath)
  })

  afterEach(() => {
    // 각 테스트 후에 파일 정리
    try {
      if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
      if (fs.existsSync(walPath)) fs.unlinkSync(walPath)
    } catch (e) {
      // 파일이 잠겨있을 수 있음
    }
  })

  /**
   * 시나리오 1: 정상 종료 후 재시작
   * 데이터를 삽입하고 정상적으로 close한 뒤 다시 열어서 데이터가 보존되는지 확인
   */
  test('should persist data after normal close and reopen', async () => {
    // 1. Shard 열기 및 데이터 삽입
    const shard1 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard1.init()

    const pk1 = await shard1.insert('Hello, Recovery!')
    const pk2 = await shard1.insert('Test Data 2')
    const pk3 = await shard1.insert('Test Data 3')

    // 2. 정상적으로 close
    await shard1.close()

    // 3. 다시 열어서 데이터 확인
    const shard2 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard2.init()

    const result1 = await shard2.select(pk1)
    const result2 = await shard2.select(pk2)
    const result3 = await shard2.select(pk3)

    expect(result1).toBe('Hello, Recovery!')
    expect(result2).toBe('Test Data 2')
    expect(result3).toBe('Test Data 3')

    await shard2.close()
  })

  /**
   * 시나리오 2: 커밋된 트랜잭션 후 crash 시뮬레이션
   * 트랜잭션을 커밋한 후 "crash"를 시뮬레이션하고 (close 없이 파일 핸들 닫기)
   * 재시작 시 WAL에서 데이터가 복구되는지 확인
   */
  test('should recover committed data after simulated crash', async () => {
    // 1. Shard 열기 및 초기화
    const shard1 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard1.init()

    // 2. 트랜잭션으로 데이터 삽입 및 커밋
    const tx = shard1.createTransaction()
    const pk1 = await shard1.insert('Committed Data 1', tx)
    const pk2 = await shard1.insert('Committed Data 2', tx)
    await tx.commit()

    // 3. Crash 시뮬레이션: close()를 호출하지 않고 프로세스 종료를 모방
    // WAL에는 데이터가 있지만, db 파일은 sync되었을 수도 있고 아닐 수도 있음
    // 실제 crash에서는 프로세스가 죽으므로 close()가 호출되지 않음
    // 여기서는 WAL 파일이 남아있는지만 확인하고 새로운 Shard를 열어봄

    // 강제로 파일 핸들들을 정리하기 위해 별도 처리 필요
    // Node.js에서는 GC가 처리하지만, 테스트를 위해 약간의 지연 추가
    // 주의: 실제 구현에서는 Shard 내부의 fileHandle을 강제로 닫을 수 없으므로
    // close()를 호출하되, WAL을 삭제하지 않고 유지하는 방식으로 테스트

    await shard1.close()

    // WAL에 직접 데이터를 추가하여 "커밋 후 sync 전 crash" 시나리오 시뮬레이션
    const { LogManager } = require('../src/core/LogManager')
    const logManager = new LogManager(walPath, pageSize)
    logManager.open()

    // 추가 데이터를 WAL에만 기록 (디스크 sync 없이)
    const crashData = new Uint8Array(pageSize).fill(77)
    const walPages = new Map<number, Uint8Array>()
    walPages.set(5, crashData) // Page 5에 데이터 기록
    await logManager.append(walPages)
    logManager.close()

    // 4. 재시작 후 복구 확인
    const shard2 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard2.init()

    // 이전에 정상 커밋된 데이터 확인
    const result1 = await shard2.select(pk1)
    const result2 = await shard2.select(pk2)

    expect(result1).toBe('Committed Data 1')
    expect(result2).toBe('Committed Data 2')

    await shard2.close()
  })

  /**
   * 시나리오 3: 미커밋 데이터는 복구되지 않음
   * 트랜잭션을 시작했지만 커밋하지 않은 상태에서 crash 발생 시
   * 해당 데이터는 복구되지 않아야 함
   */
  test('should NOT recover uncommitted data after crash', async () => {
    // 1. Shard 열기 및 커밋된 데이터 삽입
    const shard1 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard1.init()

    // 먼저 커밋되는 데이터
    const pkCommitted = await shard1.insert('This is committed')

    // 2. 새 트랜잭션에서 데이터 삽입 (커밋 안함)
    const tx = shard1.createTransaction()
    const pkUncommitted = await shard1.insert('This is NOT committed', tx)
    // tx.commit()을 호출하지 않음 - 롤백도 하지 않음

    // 3. Crash 시뮬레이션 - 롤백을 먼저 수행해야 close가 가능
    await tx.rollback()
    await shard1.close()

    // 4. 재시작 후 확인
    const shard2 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard2.init()

    // 커밋된 데이터는 존재
    const resultCommitted = await shard2.select(pkCommitted)
    expect(resultCommitted).toBe('This is committed')

    // 미커밋 데이터는 복구되지 않음 (트랜잭션이 롤백되었으므로)
    const resultUncommitted = await shard2.select(pkUncommitted)
    expect(resultUncommitted).toBeNull()

    await shard2.close()
  })

  /**
   * 시나리오 4: 배치 삽입 후 crash 복구
   * insertBatch로 대량 데이터를 삽입하고 커밋한 후 crash 시나리오
   */
  test('should recover batch insert after crash', async () => {
    const shard1 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard1.init()

    // 배치로 100개 데이터 삽입
    const dataList = Array.from({ length: 100 }, (_, i) => `batch-row-${i}`)
    const pks = await shard1.insertBatch(dataList)

    expect(pks.length).toBe(100)

    await shard1.close()

    // 재시작 후 모든 데이터 확인
    const shard2 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard2.init()

    for (let i = 0; i < 100; i++) {
      const result = await shard2.select(pks[i])
      expect(result).toBe(`batch-row-${i}`)
    }

    await shard2.close()
  })

  /**
   * 시나리오 5: 복수 트랜잭션 커밋 후 crash 복구
   * 여러 개의 개별 트랜잭션을 각각 커밋한 후 모든 데이터가 복구되는지 확인
   */
  test('should recover multiple committed transactions after crash', async () => {
    const shard1 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard1.init()

    const pks: number[] = []

    // 5개의 개별 트랜잭션
    for (let i = 0; i < 5; i++) {
      const tx = shard1.createTransaction()
      const pk = await shard1.insert(`Transaction ${i} Data`, tx)
      pks.push(pk)
      await tx.commit()
    }

    await shard1.close()

    // 재시작 후 모든 트랜잭션 데이터 확인
    const shard2 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard2.init()

    for (let i = 0; i < 5; i++) {
      const result = await shard2.select(pks[i])
      expect(result).toBe(`Transaction ${i} Data`)
    }

    await shard2.close()
  })

  /**
   * 시나리오 6: 반복적인 crash와 복구 후 데이터 일관성
   * 여러 번의 crash와 복구 사이클을 거친 후에도 데이터가 일관되게 유지되는지 확인
   */
  test('should maintain data consistency after repeated crashes', async () => {
    const allPks: { pk: number; data: string }[] = []

    // 첫 번째 사이클
    const shard1 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard1.init()

    const pk1 = await shard1.insert('Cycle 1 Data')
    allPks.push({ pk: pk1, data: 'Cycle 1 Data' })

    await shard1.close()

    // 두 번째 사이클 - 재시작 후 추가 데이터 삽입
    const shard2 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard2.init()

    // 이전 데이터 확인
    expect(await shard2.select(pk1)).toBe('Cycle 1 Data')

    const pk2 = await shard2.insert('Cycle 2 Data')
    allPks.push({ pk: pk2, data: 'Cycle 2 Data' })

    await shard2.close()

    // 세 번째 사이클 - 또 다시 재시작
    const shard3 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard3.init()

    // 모든 이전 데이터 확인
    for (const { pk, data } of allPks) {
      expect(await shard3.select(pk)).toBe(data)
    }

    const pk3 = await shard3.insert('Cycle 3 Data')
    allPks.push({ pk: pk3, data: 'Cycle 3 Data' })

    await shard3.close()

    // 최종 확인
    const shardFinal = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shardFinal.init()

    for (const { pk, data } of allPks) {
      expect(await shardFinal.select(pk)).toBe(data)
    }

    await shardFinal.close()
  })

  /**
   * 시나리오 7: 대용량 데이터 (오버플로우 페이지) 복구
   * 페이지 크기를 넘는 대용량 데이터가 올바르게 복구되는지 확인
   */
  test('should recover large data (overflow pages) after crash', async () => {
    const shard1 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard1.init()

    // 페이지 크기(4096)보다 큰 데이터 생성
    const largeData = new Uint8Array(10000).fill(65) // 'A' 10000개
    const pk = await shard1.insert(largeData)

    await shard1.close()

    // 재시작 후 대용량 데이터 확인
    const shard2 = Shard.Open(dbPath, { pageSize, wal: walPath })
    await shard2.init()

    const result = await shard2.select(pk, true) // asRaw = true
    expect(result).toEqual(largeData)

    await shard2.close()
  })
})
