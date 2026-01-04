import { Dataply } from '../src/core/Dataply'
import path from 'node:path'
import fs from 'node:fs'

describe('Performance Benchmark', () => {
  const TEST_FILE = path.join(__dirname, 'perf_dataply.dat')
  const WAL_FILE = path.join(__dirname, 'perf_dataply.wal')

  afterEach(async () => {
    if (fs.existsSync(TEST_FILE)) {
      try { await fs.promises.unlink(TEST_FILE) } catch (e) { }
    }
    if (fs.existsSync(WAL_FILE)) {
      try { await fs.promises.unlink(WAL_FILE) } catch (e) { }
    }
  })

  test('Bulk Insert 10,000 small rows (batch)', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply.init()

    const count = 10000
    const dataList: Uint8Array[] = []
    for (let i = 0; i < count; i++) {
      dataList.push(new Uint8Array([1, 2, 3, 4, 5]))
    }

    console.time('Small Row Insert (Batch)')
    const start = performance.now()
    const pks = await dataply.insertBatch(dataList)
    const end = performance.now()
    console.timeEnd('Small Row Insert (Batch)')

    expect(pks.length).toBe(count)

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Small Row Insert (Batch)] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await dataply.close()
  }, 60000)

  test('Bulk Insert 100 small rows (individual)', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply.init()

    const count = 100
    const data = new Uint8Array([1, 2, 3, 4, 5])

    console.time('Small Row Insert (Individual)')
    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await dataply.insert(data)
    }
    const end = performance.now()
    console.timeEnd('Small Row Insert (Individual)')

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Small Row Insert (Individual)] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await dataply.close()
  }, 120000)

  test('Bulk Insert 100 small rows with WAL', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096, wal: WAL_FILE })
    await dataply.init()

    const count = 100
    const data = new Uint8Array([1, 2, 3, 4, 5])

    console.time('Small Row Insert (WAL)')
    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await dataply.insert(data)
    }
    const end = performance.now()
    console.timeEnd('Small Row Insert (WAL)')

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Small Row Insert (WAL)] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await dataply.close()
  }, 120000)

  test('Bulk Insert 100 medium rows (1KB)', async () => {
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const count = 100
    const data = new Uint8Array(1024).fill(65)

    console.time('Medium Row Insert')
    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await dataply.insert(data)
    }
    const end = performance.now()
    console.timeEnd('Medium Row Insert')

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Medium Row Insert] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await dataply.close()
  }, 60000)
})
