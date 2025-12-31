import fs from 'node:fs'
import path from 'path'
import { Shard } from '../src/core/Shard'

describe('Shard', () => {
  const TEST_FILE = path.join(__dirname, 'test_shard.dat')

  afterEach(async () => {
    if (fs.existsSync(TEST_FILE)) {
      try {
        // Shard.Open으로 열린 파일 핸들을 닫아줘야 삭제가 가능할 수 있음
        // 테스트 코드 내에서 close 호출 확인
        await fs.promises.unlink(TEST_FILE)
      } catch (e) {
        // 이미 삭제되었거나 잠긴 경우
      }
    }
  })

  test('should create and initialize a new shard file', async () => {
    const shard = Shard.Open(TEST_FILE, { pageSize: 4096 })

    await shard.init()

    expect(fs.existsSync(TEST_FILE)).toBe(true)

    // 파일 크기 확인 (최소 1개 페이지 - 메타데이터 페이지)
    const stats = fs.statSync(TEST_FILE)
    expect(stats.size).toBeGreaterThanOrEqual(4096)

    await shard.close()
  })

  test('should verify a valid shard file', async () => {
    // 먼저 파일 생성
    const shard1 = Shard.Open(TEST_FILE)
    await shard1.init()
    await shard1.close()

    // 다시 열기
    const shard2 = Shard.Open(TEST_FILE)
    await shard2.init()
    expect(shard2).toBeInstanceOf(Shard)
    await shard2.close()
  })

  test('should throw error for invalid shard file', () => {
    // 빈 파일 생성
    fs.writeFileSync(TEST_FILE, 'invalid data')

    expect(() => {
      Shard.Open(TEST_FILE)
    }).toThrow('Invalid shard file')
  })

  describe('insert and select', () => {
    let shard: Shard

    beforeEach(async () => {
      shard = Shard.Open(TEST_FILE, { pageSize: 8192 })
      await shard.init()
    })

    afterEach(async () => {
      await shard.close()
    })

    test('should insert and select a string', async () => {
      const data = 'Hello, World!'
      const pk = await shard.insert(data)

      const result = await shard.select(pk)
      expect(result).toBe(data)
    })

    test('should insert and select a buffer', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      const pk = await shard.insert(data)

      const result = await shard.select(pk, true)
      expect(result).toEqual(data)
    })

    test('should insert and select large data (overflow)', async () => {
      // Create data larger than one page (8192 bytes)
      const data = new Uint8Array(10000).fill(65) // 'A'
      const pk = await shard.insert(data)

      const result = await shard.select(pk, true)
      expect(result).toEqual(data)
    })

    test('should insert multiple rows and select them', async () => {
      const count = 100
      const pks: number[] = []

      for (let i = 0; i < count; i++) {
        const pk = await shard.insert(`row-${i}`)
        pks.push(pk)
      }

      for (let i = 0; i < count; i++) {
        const result = await shard.select(pks[i])
        expect(result).toBe(`row-${i}`)
      }
    })

    test('should return null for non-existent PK', async () => {
      // Insert one row to ensure file is initialized
      await shard.insert('test')

      const result = await shard.select(999999)
      expect(result).toBeNull()
    })
  })
})
