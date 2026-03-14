import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

describe('NoWAL Persistence Bug', () => {
  const TEST_FILE = path.join(__dirname, 'test_nowal_persistence.dat')
  const WAL_FILE = path.join(__dirname, 'test_nowal_persistence.wal')

  // afterEach(async () => {
  //   for (const f of [TEST_FILE, WAL_FILE]) {
  //     if (fs.existsSync(f)) {
  //       try { await fs.promises.unlink(f) } catch { }
  //     }
  //   }
  // })

  test('WITHOUT WAL: dirty pages should be flushed to disk after commit', async () => {
    // WAL 없이 데이터 삽입
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096, pagePreallocationCount: 10, walCheckpointThreshold: 1, logLevel: 1 })
    await dataply.init()

    // 초기 파일 크기 기록
    const initialSize = fs.statSync(TEST_FILE).size
    console.log(`[NO WAL] Initial file size: ${initialSize}`)

    // 데이터 삽입 (내부적으로 commit이 호출됨)
    const pk = await dataply.insert('Hello, No WAL!')

    // 커밋 직후 파일 크기 확인 - flush가 되었다면 파일 크기가 증가해야 함
    const sizeAfterCommit = fs.statSync(TEST_FILE).size
    console.log(`[NO WAL] File size after commit (before close): ${sizeAfterCommit}`)
    console.log(`[NO WAL] Initial file size: ${initialSize}`)

    // 핵심 검증: WAL 없이도 커밋 후 디스크에 데이터가 기록되어야 함
    // 새 데이터 삽입에 의한 페이지 할당으로 파일 크기가 증가해야 함
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

    // 디스크에서 직접 읽어보기
    const strategy = (dataply as any).api.pfs.pageStrategy
    const dirtyPagesCount = strategy.dirtyPages.size
    console.log(`[WITH WAL] Dirty pages count after commit: ${dirtyPagesCount}`)

    // WAL 있으면 체크포인트 후 dirty pages가 0이어야 함
    expect(dirtyPagesCount).toBe(0)

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
