import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

describe('Dataply Update', () => {
  const testFileRaw = path.join(__dirname, 'update_test.dataply')
  const testFile = path.resolve(testFileRaw)
  let dataply: Dataply

  beforeEach(async () => {
    if (fs.existsSync(testFile)) {
      await fs.promises.unlink(testFile)
    }
    dataply = new Dataply(testFile)
    await dataply.init()
  })

  afterEach(async () => {
    await dataply.close()
    if (fs.existsSync(testFile)) {
      await fs.promises.unlink(testFile)
    }
  })

  test('should update row in place if new data is shorter', async () => {
    const pk = await dataply.insert('hello world')
    await dataply.update(pk, 'hello')

    const result = await dataply.select(pk, false)
    expect(result).toBe('hello')
  })

  test('should update row by moving if new data is longer', async () => {
    const pk = await dataply.insert('short')
    const longData = 'this is a much longer data that will definitely not fit in the original space if it was packed, but since we use exact allocation, any growth causes move'
    await dataply.update(pk, longData)

    const result = await dataply.select(pk, false)
    expect(result).toBe(longData)
  })

  test('should support multiple updates (chaining)', async () => {
    const pk = await dataply.insert('v1')
    await dataply.update(pk, 'version 2 is longer')
    await dataply.update(pk, 'v3') // shorter than v2, should overwrite in Target1
    await dataply.update(pk, 'version 4 is even longer than version 2') // should move to Target2

    const result = await dataply.select(pk, false)
    expect(result).toBe('version 4 is even longer than version 2')
  })

  test('should update overflow row in place (overflow page overwrite)', async () => {
    // pageSize is 8192, so > 8100 should be overflow
    const bigData = 'X'.repeat(8100)
    const pk = await dataply.insert(bigData)

    const bigData2 = 'Y'.repeat(8150)
    await dataply.update(pk, bigData2)

    const result = await dataply.select(pk, false)
    expect(result?.length).toBe(8150)
    expect(result?.[0]).toBe('Y')
  })

  test('should work with transactions', async () => {
    const pk = await dataply.insert('initial')
    const tx = dataply.createTransaction()
    await dataply.update(pk, 'updated in tx', tx)

    // 이전에 읽으면 initial이어야 함 (MVCC 지원 여부에 따라 다르지만, 현재 구현상 tx 내에서만 보임)
    const beforeCommit = await dataply.select(pk, false)
    expect(beforeCommit).toBe('initial')

    await tx.commit()

    const afterCommit = await dataply.select(pk, false)
    expect(afterCommit).toBe('updated in tx')
  })
})
