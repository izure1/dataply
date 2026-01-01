import fs from 'node:fs'
import path from 'node:path'
import { Shard } from '../src/core/Shard'

describe('Shard Update', () => {
  const testFileRaw = path.join(__dirname, 'update_test.shard')
  const testFile = path.resolve(testFileRaw)
  let shard: Shard

  beforeEach(async () => {
    if (fs.existsSync(testFile)) {
      fs.unlinkSync(testFile)
    }
    shard = new Shard(testFile)
    await shard.init()
  })

  afterEach(async () => {
    await shard.close()
    if (fs.existsSync(testFile)) {
      fs.unlinkSync(testFile)
    }
  })

  it('should update row in place if new data is shorter', async () => {
    const pk = await shard.insert('hello world')
    await shard.update(pk, 'hello')

    const result = await shard.select(pk, false)
    expect(result).toBe('hello')
  })

  it('should update row by moving if new data is longer', async () => {
    const pk = await shard.insert('short')
    const longData = 'this is a much longer data that will definitely not fit in the original space if it was packed, but since we use exact allocation, any growth causes move'
    await shard.update(pk, longData)

    const result = await shard.select(pk, false)
    expect(result).toBe(longData)
  })

  it('should support multiple updates (chaining)', async () => {
    const pk = await shard.insert('v1')
    await shard.update(pk, 'version 2 is longer')
    await shard.update(pk, 'v3') // shorter than v2, should overwrite in Target1
    await shard.update(pk, 'version 4 is even longer than version 2') // should move to Target2

    const result = await shard.select(pk, false)
    expect(result).toBe('version 4 is even longer than version 2')
  })

  it('should update overflow row in place (overflow page overwrite)', async () => {
    // pageSize is 8192, so > 8100 should be overflow
    const bigData = 'X'.repeat(8100)
    const pk = await shard.insert(bigData)

    const bigData2 = 'Y'.repeat(8150)
    await shard.update(pk, bigData2)

    const result = await shard.select(pk, false)
    expect(result?.length).toBe(8150)
    expect(result?.[0]).toBe('Y')
  })

  it('should work with transactions', async () => {
    const pk = await shard.insert('initial')
    const tx = shard.createTransaction()
    await shard.update(pk, 'updated in tx', tx)

    // 이전에 읽으면 initial이어야 함 (MVCC 지원 여부에 따라 다르지만, 현재 구현상 tx 내에서만 보임)
    const beforeCommit = await shard.select(pk, false)
    expect(beforeCommit).toBe('initial')

    await tx.commit()

    const afterCommit = await shard.select(pk, false)
    expect(afterCommit).toBe('updated in tx')
  })
})
