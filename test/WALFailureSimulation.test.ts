import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'
import { MetadataPageManager, PageManagerFactory } from '../src/core/Page'

describe('WAL Failure Simulation Test', () => {
  const testDir = path.join(__dirname, 'temp_failure_sim')
  const dbPath = path.join(testDir, 'fail_sim.db')
  const walPath = path.join(testDir, 'fail_sim.wal')
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

  /**
   * 시뮬레이션 1: 커밋 마커 누락
   * 데이터 엔트리는 존재하지만 마커(0xFFFFFFFF)가 없는 경우 해당 데이터가 무시되는지 확인
   */
  test('should ignore entries without a commit marker', async () => {
    // 1. 정상 데이터 1개 삽입 (커밋됨)
    const dataply = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply.init()
    const pk1 = await dataply.insert('Healthy Data')
    await dataply.close()

    // 2. WAL 파일에 커밋 마커 없이 데이터만 강제 주입
    const walFd = fs.openSync(walPath, 'a+')
    const entrySize = 4 + pageSize
    const buffer = new Uint8Array(entrySize)
    const view = new DataView(buffer.buffer)

    // Page ID = 5, Data = 'Ghost Data'
    view.setUint32(0, 5, true)
    const ghostData = new TextEncoder().encode('Ghost Data'.padEnd(pageSize, '\0'))
    buffer.set(ghostData, 4)

    fs.writeSync(walFd, buffer, 0, entrySize)
    fs.fsyncSync(walFd)
    fs.closeSync(walFd)

    // 3. 다시 열었을 때 Ghost Data가 없어야 함
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply2.init()

    expect(await dataply2.select(pk1)).toBe('Healthy Data')

    // Page 5가 실제로 반영되지 않았는지 로우 레벨 API로 확인 (직접 select로는 확인 불가하므로 에러 없는지만 확인)
    // 혹은 Dataply의 row count 등이 늘어나지 않았음을 확인
    const metadata = await dataply2.getMetadata()
    expect(metadata.rowCount).toBe(1)

    await dataply2.close()
  })

  /**
   * 시뮬레이션 2: 부분적 페이지 기록
   * 페이지 데이터 쓰기 도중 파일이 뚝 끊겨서 entrySize(4+4096)보다 작은 데이터가 남은 경우
   */
  test('should handle partial entry write without crashing', async () => {
    const dataply = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply.init()
    await dataply.insert('Base Data')
    await dataply.close()

    // WAL 파일 끝에 100바이트만 쓰기 (불완전한 엔트리)
    const walFd = fs.openSync(walPath, 'a+')
    const brokenBuffer = new Uint8Array(100).fill(0xAA)
    fs.writeSync(walFd, brokenBuffer)
    fs.fsyncSync(walFd)
    fs.closeSync(walFd)

    // 다시 열었을 때 에러가 발생하지 않아야 함
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })

    // init() 도중 readAllSync 루프가 도는데, 이때 잔여 바이트를 안전하게 처리하는지 확인
    await expect(dataply2.init()).resolves.not.toThrow()

    expect(await dataply2.select(1)).toBe('Base Data')
    await dataply2.close()
  })

  /**
   * 시뮬레이션 3: 체크섬 손상
   * 커밋 마커까지 완벽히 기록되었으나, 중간에 데이터가 오염된 경우
   */
  test('should skip recovery for entries with corrupted checksums', async () => {
    const dataply = new Dataply(dbPath, { pageSize, wal: walPath })
    await dataply.init()
    await dataply.insert('Original')
    await dataply.close()

    // WAL 파일에 유효한 엔트리와 커밋 마커를 쓰되, 체크섬을 망가뜨림
    const walFd = fs.openSync(walPath, 'a+')
    const entrySize = 4 + pageSize
    const buffer = new Uint8Array(entrySize)
    const view = new DataView(buffer.buffer)

    // Page ID = 1 (메타데이터 페이지 혹은 데이터 페이지)
    view.setUint32(0, 1, true)
    buffer.fill(0xFF, 4) // 오염된 데이터

    // 엔트리 쓰기
    fs.writeSync(walFd, buffer)

    // 커밋 마커 쓰기 (마치 정상 커밋된 것처럼)
    const marker = new Uint8Array(entrySize)
    new DataView(marker.buffer).setUint32(0, 0xFFFFFFFF, true)
    fs.writeSync(walFd, marker)

    fs.fsyncSync(walFd)
    fs.closeSync(walFd)

    // 다시 열었을 때 체크섬 에러 로그를 남기거나 건너뛰되, 크래시가 나지 않아야 함
    const dataply2 = new Dataply(dbPath, { pageSize, wal: walPath })

    // recover() 내의 verifyChecksum 로직이 동작하고 "Ignoring changes" 경고와 함께 진행되어야 함
    await expect(dataply2.init()).resolves.not.toThrow()

    // 기존 데이터는 안전해야 함
    expect(await dataply2.select(1)).toBe('Original')

    await dataply2.close()
  })
})
