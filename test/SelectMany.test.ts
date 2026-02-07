import fs from 'node:fs'
import path from 'node:path'
import { Dataply } from '../src/core/Dataply'

describe('Dataply selectMany', () => {
  const TEST_FILE = path.join(__dirname, 'test_selectmany.dat')

  let dataply: Dataply

  beforeEach(async () => {
    if (fs.existsSync(TEST_FILE)) {
      try { await fs.promises.unlink(TEST_FILE) } catch (e) { }
    }
    dataply = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply.init()
  })

  afterEach(async () => {
    await dataply.close()
    if (fs.existsSync(TEST_FILE)) {
      try { await fs.promises.unlink(TEST_FILE) } catch (e) { }
    }
  })

  test('should select multiple rows in the correct order', async () => {
    const data = ['A', 'B', 'C', 'D', 'E']
    const pks = await dataply.insertBatch(data)

    // Select in original order
    const results1 = await dataply.selectMany(pks)
    expect(results1).toEqual(data)

    // Select in reversed order
    const reversedPks = [...pks].reverse()
    const results2 = await dataply.selectMany(reversedPks)
    expect(results2).toEqual([...data].reverse())

    // Select sub-selection
    const subPks = [pks[0], pks[2], pks[4]]
    const results3 = await dataply.selectMany(subPks)
    expect(results3).toEqual(['A', 'C', 'E'])
  })

  test('should handle non-existent PKs by returning null', async () => {
    const data = ['A', 'B']
    const pks = await dataply.insertBatch(data)

    const mixedPks = [pks[0], 9999, pks[1], 8888]
    const results = await dataply.selectMany(mixedPks)

    expect(results).toEqual(['A', null, 'B', null])
  })

  test('should handle empty input array', async () => {
    const results = await dataply.selectMany([])
    expect(results).toEqual([])
  })

  test('should work correctly with large data (overflow)', async () => {
    const largeData1 = new Uint8Array(10000).fill(65)
    const largeData2 = new Uint8Array(10000).fill(66)

    const pk1 = await dataply.insert(largeData1)
    const pk2 = await dataply.insert(largeData2)

    const results = await dataply.selectMany([pk1, pk2], true)
    expect(results[0]).toEqual(largeData1)
    expect(results[1]).toEqual(largeData2)
  })

  test('should handle scattered PKs efficiently', async () => {
    // Insert 100 rows
    const pks: number[] = []
    for (let i = 0; i < 100; i++) {
      pks.push(await dataply.insert(`row-${i}`))
    }

    // Select some scattered PKs
    const targetPks = [pks[0], pks[50], pks[99]]
    const results = await dataply.selectMany(targetPks)
    expect(results).toEqual(['row-0', 'row-50', 'row-99'])
  })
})
