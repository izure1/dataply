import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'
import { WALManager } from '../src/core/WALManager'

describe('WAL Verification Test', () => {
  const testDir = path.join(__dirname, 'temp_wal_verify')
  const dbPath = path.join(testDir, 'verify.db')
  const walPath = path.join(testDir, 'verify.wal')
  const pageSize = 4096

  beforeAll(async () => {
    if (!fs.existsSync(testDir)) {
      await fs.promises.mkdir(testDir)
    }
  })

  afterAll(async () => {
    if (fs.existsSync(testDir)) {
      await fs.promises.rm(testDir, { recursive: true, force: true })
    }
  })

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    if (fs.existsSync(walPath)) fs.unlinkSync(walPath)
  })

  test('should record data in WAL and recover after crash during checkpoint', async () => {
    // 1. 초기 데이터 준비 (WAL 활성화)
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    // 2. WALManager의 clear() 메서드를 Mocking하여 WAL이 지워지지 않게 함 (Checkpoint 도중 크래시 시뮬레이션)
    const api = (dataply1 as any).api
    const walManager = api.pfs.walManager
    const originalClear = walManager.clear.bind(walManager)

    let clearCalled = false
    walManager.clear = async () => {
      clearCalled = true
      // clear를 하지 않고 리턴 (크래시 시뮬레이션)
    }

    // 3. 데이터 삽입 및 커밋
    const pk = await dataply1.insert('Critical WAL Data')

    expect(clearCalled).toBe(true)

    // 4. WAL 파일이 비어있지 않아야 함 (clear를 건너뛰었으므로)
    const stats = fs.statSync(walPath)
    expect(stats.size).toBeGreaterThan(0)

    // 5. 강제 종료 (DB 파일만 닫기, WAL은 데이터가 남아있는 상태)
    fs.closeSync(api.fileHandle)
    walManager.close()

    // 6. DB 파일 손상 시뮬레이션 (선택 사항: 여기서는 단순히 WAL 덕분에 복구되는지 확인)
    // 실제로는 DB 파일 쓰기가 완료되었을 수도 있으므로, 
    // WAL 복구 로직이 실행되는지 확인하는 것이 중요함.

    // 7. 재시작 및 복구 확인
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init() // 여기서 walManager.recover()가 실행됨

    const result = await dataply2.select(pk)
    expect(result).toBe('Critical WAL Data')

    await dataply2.close()
  })

  test('should maintain atomicity between data and metadata/index', async () => {
    // 이 테스트는 WAL이 인덱스 페이지와 메인 데이터를 모두 포함하는지 검증합니다.
    const dataply1 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply1.init()

    const api = (dataply1 as any).api
    const walManager = api.pfs.walManager

    // clear Mocking
    walManager.clear = async () => { }

    // 대량의 데이터를 삽입하여 여러 페이지(메타데이터, 데이터, 인덱스)가 변경되도록 유도
    const tx = dataply1.createTransaction()
    const pk = await dataply1.insert('Atomicity Test Data', tx)
    await tx.commit()

    // WAL 파일에서 기록된 유니크 페이지 개수 확인
    const restoredPages = walManager.readAllSync()

    // 최소한 메타데이터 페이지(0)와 데이터 페이지가 포함되어야 함
    expect(restoredPages.has(0)).toBe(true)
    expect(restoredPages.size).toBeGreaterThan(1)

    await dataply1.close()
  })
})
