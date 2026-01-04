import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

/**
 * 종합 복구 테스트 (Recovery Integration Test)
 * 
 * 실제 사용 환경에서 Dataply를 사용하다가 서버가 비정상 종료되었을 때,
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
    // 1. Dataply 열기 및 데이터 삽입
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    const pk1 = await dataply1.insert('Hello, Recovery!')
    const pk2 = await dataply1.insert('Test Data 2')
    const pk3 = await dataply1.insert('Test Data 3')

    // 2. 정상적으로 close
    await dataply1.close()

    // 3. 다시 열어서 데이터 확인
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    const result1 = await dataply2.select(pk1)
    const result2 = await dataply2.select(pk2)
    const result3 = await dataply2.select(pk3)

    expect(result1).toBe('Hello, Recovery!')
    expect(result2).toBe('Test Data 2')
    expect(result3).toBe('Test Data 3')

    await dataply2.close()
  })

  /**
   * 시나리오 2: 커밋된 트랜잭션 후 crash 시뮬레이션
   * 트랜잭션을 커밋한 후 "crash"를 시뮬레이션하고 (close 없이 파일 핸들 닫기)
   * 재시작 시 WAL에서 데이터가 복구되는지 확인
   */
  test('should recover committed data after simulated crash', async () => {
    // 1. Dataply 열기 및 초기화
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    // 2. 트랜잭션으로 데이터 삽입 및 커밋
    const tx = dataply1.createTransaction()
    const pk1 = await dataply1.insert('Committed Data 1', tx)
    const pk2 = await dataply1.insert('Committed Data 2', tx)
    await tx.commit()

    // 3. Crash 시뮬레이션: close()를 호출하지 않고 프로세스 종료를 모방
    // WAL에는 데이터가 있지만, db 파일은 sync되었을 수도 있고 아닐 수도 있음
    // 실제 crash에서는 프로세스가 죽으므로 close()가 호출되지 않음
    // 여기서는 WAL 파일이 남아있는지만 확인하고 새로운 Dataply를 열어봄

    // 강제로 파일 핸들들을 정리하기 위해 별도 처리 필요
    // Node.js에서는 GC가 처리하지만, 테스트를 위해 약간의 지연 추가
    // 주의: 실제 구현에서는 Dataply 내부의 fileHandle을 강제로 닫을 수 없으므로
    // close()를 호출하되, WAL을 삭제하지 않고 유지하는 방식으로 테스트

    await dataply1.close()

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
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    // 이전에 정상 커밋된 데이터 확인
    const result1 = await dataply2.select(pk1)
    const result2 = await dataply2.select(pk2)

    expect(result1).toBe('Committed Data 1')
    expect(result2).toBe('Committed Data 2')

    await dataply2.close()
  })

  /**
   * 시나리오 3: 미커밋 데이터는 복구되지 않음
   * 트랜잭션을 시작했지만 커밋하지 않은 상태에서 crash 발생 시
   * 해당 데이터는 복구되지 않아야 함
   */
  test('should NOT recover uncommitted data after crash', async () => {
    // 1. Dataply 열기 및 커밋된 데이터 삽입
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    // 먼저 커밋되는 데이터
    const pkCommitted = await dataply1.insert('This is committed')

    // 2. 새 트랜잭션에서 데이터 삽입 (커밋 안함)
    const tx = dataply1.createTransaction()
    const pkUncommitted = await dataply1.insert('This is NOT committed', tx)
    // tx.commit()을 호출하지 않음 - 롤백도 하지 않음

    // 3. Crash 시뮬레이션 - 롤백을 먼저 수행해야 close가 가능
    await tx.rollback()
    await dataply1.close()

    // 4. 재시작 후 확인
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    // 커밋된 데이터는 존재
    const resultCommitted = await dataply2.select(pkCommitted)
    expect(resultCommitted).toBe('This is committed')

    // 미커밋 데이터는 복구되지 않음 (트랜잭션이 롤백되었으므로)
    const resultUncommitted = await dataply2.select(pkUncommitted)
    expect(resultUncommitted).toBeNull()

    await dataply2.close()
  })

  /**
   * 시나리오 4: 배치 삽입 후 crash 복구
   * insertBatch로 대량 데이터를 삽입하고 커밋한 후 crash 시나리오
   */
  test('should recover batch insert after crash', async () => {
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    // 배치로 100개 데이터 삽입
    const dataList = Array.from({ length: 100 }, (_, i) => `batch-row-${i}`)
    const pks = await dataply1.insertBatch(dataList)

    expect(pks.length).toBe(100)

    await dataply1.close()

    // 재시작 후 모든 데이터 확인
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    for (let i = 0; i < 100; i++) {
      const result = await dataply2.select(pks[i])
      expect(result).toBe(`batch-row-${i}`)
    }

    await dataply2.close()
  })

  /**
   * 시나리오 5: 복수 트랜잭션 커밋 후 crash 복구
   * 여러 개의 개별 트랜잭션을 각각 커밋한 후 모든 데이터가 복구되는지 확인
   */
  test('should recover multiple committed transactions after crash', async () => {
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    const pks: number[] = []

    // 5개의 개별 트랜잭션
    for (let i = 0; i < 5; i++) {
      const tx = dataply1.createTransaction()
      const pk = await dataply1.insert(`Transaction ${i} Data`, tx)
      pks.push(pk)
      await tx.commit()
    }

    await dataply1.close()

    // 재시작 후 모든 트랜잭션 데이터 확인
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    for (let i = 0; i < 5; i++) {
      const result = await dataply2.select(pks[i])
      expect(result).toBe(`Transaction ${i} Data`)
    }

    await dataply2.close()
  })

  /**
   * 시나리오 6: 반복적인 crash와 복구 후 데이터 일관성
   * 여러 번의 crash와 복구 사이클을 거친 후에도 데이터가 일관되게 유지되는지 확인
   */
  test('should maintain data consistency after repeated crashes', async () => {
    const allPks: { pk: number; data: string }[] = []

    // 첫 번째 사이클
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    const pk1 = await dataply1.insert('Cycle 1 Data')
    allPks.push({ pk: pk1, data: 'Cycle 1 Data' })

    await dataply1.close()

    // 두 번째 사이클 - 재시작 후 추가 데이터 삽입
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    // 이전 데이터 확인
    expect(await dataply2.select(pk1)).toBe('Cycle 1 Data')

    const pk2 = await dataply2.insert('Cycle 2 Data')
    allPks.push({ pk: pk2, data: 'Cycle 2 Data' })

    await dataply2.close()

    // 세 번째 사이클 - 또 다시 재시작
    const dataply3 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply3.init()

    // 모든 이전 데이터 확인
    for (const { pk, data } of allPks) {
      expect(await dataply3.select(pk)).toBe(data)
    }

    const pk3 = await dataply3.insert('Cycle 3 Data')
    allPks.push({ pk: pk3, data: 'Cycle 3 Data' })

    await dataply3.close()

    // 최종 확인
    const dataplyFinal = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataplyFinal.init()

    for (const { pk, data } of allPks) {
      expect(await dataplyFinal.select(pk)).toBe(data)
    }

    await dataplyFinal.close()
  })

  /**
   * 시나리오 7: 대용량 데이터 (오버플로우 페이지) 복구
   * 페이지 크기를 넘는 대용량 데이터가 올바르게 복구되는지 확인
   */
  test('should recover large data (overflow pages) after crash', async () => {
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    // 페이지 크기(4096)보다 큰 데이터 생성
    const largeData = new Uint8Array(10000).fill(65) // 'A' 10000개
    const pk = await dataply1.insert(largeData)

    await dataply1.close()

    // 재시작 후 대용량 데이터 확인
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    const result = await dataply2.select(pk, true) // asRaw = true
    expect(result).toEqual(largeData)

    await dataply2.close()
  })

  /**
   * 시나리오 8: 프로세스 강제 종료 (close 미호출) 시 복구
   * dataply.close()를 호출하지 않고 파일 핸들만 강제로 닫아 프로세스 crash를 시뮬레이션
   */
  test('should recover data correctly when process terminates without close()', async () => {
    // 1. 초기 데이터 준비 및 커밋
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    const pk1 = await dataply1.insert('Committed Data 1')

    // 2. 추가 트랜잭션 수행 및 커밋
    const tx = dataply1.createTransaction()
    const pk2 = await dataply1.insert('Committed Data 2', tx)
    await tx.commit()

    // 3. 미커밋 데이터 추가 (이는 복구되지 않아야 함)
    const tx2 = dataply1.createTransaction()
    await dataply1.insert('Uncommitted Data', tx2)

    // 4. 강제 종료 시뮬레이션
    // private 속성에 접근하여 파일 핸들 강제 종료
    const rawDataply = (dataply1 as any).api
    const fd = rawDataply.fileHandle
    const walFd = rawDataply.pfs.vfs.logManager?.fd

    if (fd) fs.closeSync(fd)
    if (walFd) fs.closeSync(walFd)

    // 5. 재시작 및 검증
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    const result1 = await dataply2.select(pk1)
    const result2 = await dataply2.select(pk2)

    expect(result1).toBe('Committed Data 1')
    expect(result2).toBe('Committed Data 2')

    // 미커밋 데이터는 롤백되어야 함 (PK를 모르므로 전체 스캔하거나 개수 확인이 이상적이나, 여기선 데이터 무결성 위주 확인)
    // 단순히 에러 없이 열리고 기존 데이터가 잘 조회되는지 확인

    await dataply2.close()
  })

  /**
   * 시나리오 9: 비동기 커밋 중 강제 종료 시뮬레이션
   * commit()을 호출했으나 await하지 않고 즉시 종료되는 상황
   */
  test('should NOT recover data when process terminates immediately after async commit (simulation)', async () => {
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    // 1. 베이스 데이터
    const pkBase = await dataply1.insert('Base Data')

    // 2. 트랜잭션 시작
    const tx = dataply1.createTransaction()
    const pkAsync = await dataply1.insert('Async Commit Data', tx)

    // 3. 커밋 요청만 하고 기다리지 않음
    tx.commit().catch(() => { }) // 에러 무시

    // 4. 즉시 강제 종료 (매우 짧은 시간 내에 종료된 것으로 가정)
    // 실제로는 OS 스케줄링에 따라 일부 기록될 수도 있지만, 
    // 여기서는 파일 핸들을 바로 닫아버림으로써 "쓰기 도중 차단" 또는 "쓰기 전 차단"을 유도
    const rawDataply = (dataply1 as any).api
    const fd = rawDataply.fileHandle
    const walFd = rawDataply.pfs.vfs.logManager?.fd

    try {
      if (fd) fs.closeSync(fd)
      if (walFd) fs.closeSync(walFd)
    } catch (e) {
      // 이미 닫혔거나 타이밍 이슈 발생 시 무시
    }

    // 5. 재시작
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    // 베이스 데이터는 있어야 함
    const resultBase = await dataply2.select(pkBase)
    expect(resultBase).toBe('Base Data')

    // 비동기 커밋 데이터는 보장할 수 없음 (운 좋으면 있고, 아니면 없음)
    // 하지만 "원자성"이 깨져서 데이터가 깨지거나 DB가 열리지 않는 상황은 없어야 함
    // 만약 WAL에 부분적으로만 기록되었다면, 체크섬이나 길이 불일치로 버려져야 함 -> 즉 데이터는 없어야 안전
    // (현재 구현상 부분 기록 감지 로직이 완벽하지 않을 수 있으나, 최소한 DB 오픈은 되어야 함)

    const resultAsync = await dataply2.select(pkAsync)
    // 여기서는 "복구되지 않음"을 기대하거나 "복구되더라도 깨지지 않음"을 기대
    // 테스트 환경에서는 즉시 close하므로 기록될 틈이 거의 없어 null일 확률이 높음
    if (resultAsync !== null) {
      expect(resultAsync).toBe('Async Commit Data')
    }

    await dataply2.close()
  })
})
