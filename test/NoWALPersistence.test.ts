import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

describe('NoWAL Persistence Bug', () => {
  const TEST_FILE = path.join(__dirname, 'test_nowal_persistence.dat')
  const WAL_FILE = path.join(__dirname, 'test_nowal_persistence.wal')

  afterEach(async () => {
    for (const f of [TEST_FILE, WAL_FILE]) {
      if (fs.existsSync(f)) {
        try { await fs.promises.unlink(f) } catch { }
      }
    }
  })

  test('WITHOUT WAL: dirty pages should be flushed to disk after commit', async () => {
    // WAL 없이 데이터 삽입
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096, pagePreallocationCount: 10, walCheckpointThreshold: 1 })
    await dataply.init()

    // 초기 파일 크기 기록
    const initialSize = fs.statSync(TEST_FILE).size
    console.log(`[NO WAL] Initial file size: ${initialSize}`)

    // 데이터 삽입 (내부적으로 commit이 호출됨)
    // 페이지 사이즈(4096)를 초과하는 데이터를 삽입하여 강제 페이지 추가를 유도
    const largeData = 'a'.repeat(5000)
    const pk = await dataply.insert(largeData)

    // 커밋 직후 파일 크기 확인 - flush가 되었다면 파일 크기가 증가해야 함
    const sizeAfterCommit = fs.statSync(TEST_FILE).size
    console.log(`[NO WAL] File size after commit (before close): ${sizeAfterCommit}`)
    console.log(`[NO WAL] Initial file size: ${initialSize}`)

    // 핵심 검증: 추가된 페이지들(pagePreallocationCount = 10)로 인해 파일 크기가 증가해야 함
    expect(sizeAfterCommit).toBeGreaterThan(initialSize)

    await dataply.close()
  })

  test('WITH WAL: dirty pages should be flushed to disk after checkpoint', async () => {
    // WAL 포함하여 데이터 삽입
    const dataply = new Dataply(TEST_FILE, {
      pageSize: 4096,
      wal: WAL_FILE,
      walCheckpointThreshold: 1 // 즉시 체크포인트
    })
    await dataply.init()

    // 초기 파일 크기 기록
    const initialSize = fs.statSync(TEST_FILE).size
    console.log(`[WITH WAL] Initial file size: ${initialSize}`)

    // 데이터 삽입 (내부적으로 commit이 호출됨)
    const pk = await dataply.insert('Hello, With WAL!')

    // 커밋 직후 파일 크기 확인
    const sizeAfterCommit = fs.statSync(TEST_FILE).size
    console.log(`[WITH WAL] File size after commit (before close): ${sizeAfterCommit}`)

    // 디스크에서 직접 읽어보기 (mvcc-api 기반)
    const rootTx = (dataply as any).api.pfs.rootTransaction
    const writeBufferSize = rootTx.writeBuffer.size
    console.log(`[WITH WAL] Root writeBuffer size after commit: ${writeBufferSize}`)

    // WAL 있으면 체크포인트 후 rootTx writeBuffer가 비어있어야 함
    expect(writeBufferSize).toBe(0)

    await dataply.close()
  })

  test('WITHOUT WAL: data should persist after close and reopen', async () => {
    // 1. WAL 없이 데이터 삽입
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply.init()

    const pk = await dataply.insert('Persistence Test')
    await dataply.close()

    // 2. 다시 열어서 데이터 확인
    const dataply2 = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply2.init()

    const result = await dataply2.select(pk)
    expect(result).toBe('Persistence Test')

    await dataply2.close()
  })
})
