import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

describe('WAL Bypass Reproduction Test', () => {
  const testDir = path.join(__dirname, 'temp_wal_repro')
  const dbPath = path.join(testDir, 'repro.db')
  const walPath = path.join(testDir, 'repro.wal')
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

  test('should record changes in WAL file when committed', async () => {
    // 1. WAL 기능을 활성화하여 Dataply 인스턴스 생성
    const dataply = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply.init()

    // 2. 데이터 삽입 및 커밋
    // 내부적으로 Transaction.commit()이 호출됨
    await dataply.insert('Should be in WAL')

    // 3. WAL 파일의 크기 확인
    // 정상적이라면 최소한 (PageID(4) + PageData(4096)) * N + CommitMarker(4100) 만큼의 크기가 있어야 함
    if (fs.existsSync(walPath)) {
      const stats = fs.statSync(walPath)
      console.log(`WAL File Size after commit: ${stats.size} bytes`)

      // 기댓값: stats.size > 0
      // 실제값(예상되는 버그): stats.size === 0 (파일이 생성되지 않았거나 비어 있음)
      expect(stats.size).toBeGreaterThan(0)
    } else {
      console.log('WAL File does not exist after commit!')
      fail('WAL file should exist but it does not.')
    }

    await dataply.close()
  })
})
