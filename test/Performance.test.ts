import { Shard } from '../src/core/Shard'
import path from 'path'
import fs from 'fs'

describe('Performance Benchmark', () => {
  const TEST_FILE = path.join(__dirname, 'perf_shard.dat')
  const WAL_FILE = path.join(__dirname, 'perf_shard.wal')

  afterEach(async () => {
    if (fs.existsSync(TEST_FILE)) {
      try { await fs.promises.unlink(TEST_FILE) } catch (e) { }
    }
    if (fs.existsSync(WAL_FILE)) {
      try { await fs.promises.unlink(WAL_FILE) } catch (e) { }
    }
  })

  test('Bulk Insert 10,000 small rows (batch)', async () => {
    const shard = Shard.Open(TEST_FILE, { pageSize: 4096 })
    await shard.init()

    const count = 10000
    const dataList: Uint8Array[] = []
    for (let i = 0; i < count; i++) {
      dataList.push(new Uint8Array([1, 2, 3, 4, 5]))
    }

    console.time('Small Row Insert (Batch)')
    const start = performance.now()
    const pks = await shard.insertBatch(dataList)
    const end = performance.now()
    console.timeEnd('Small Row Insert (Batch)')

    expect(pks.length).toBe(count)

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Small Row Insert (Batch)] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }, 60000)

  test('Bulk Insert 10,000 small rows (individual)', async () => {
    const shard = Shard.Open(TEST_FILE, { pageSize: 4096 })
    await shard.init()

    const count = 10000
    const data = new Uint8Array([1, 2, 3, 4, 5])

    console.time('Small Row Insert (Individual)')
    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await shard.insert(data)
    }
    const end = performance.now()
    console.timeEnd('Small Row Insert (Individual)')

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Small Row Insert (Individual)] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }, 120000)

  test('Bulk Insert 10,000 small rows with WAL', async () => {
    const shard = Shard.Open(TEST_FILE, { pageSize: 4096, wal: WAL_FILE })
    await shard.init()

    const count = 10000
    const data = new Uint8Array([1, 2, 3, 4, 5])

    console.time('Small Row Insert (WAL)')
    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await shard.insert(data)
    }
    const end = performance.now()
    console.timeEnd('Small Row Insert (WAL)')

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Small Row Insert (WAL)] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }, 120000)

  test('Bulk Insert 1,000 medium rows (1KB)', async () => {
    const shard = Shard.Open(TEST_FILE, { pageSize: 8192 })
    await shard.init()

    const count = 1000
    const data = new Uint8Array(1024).fill(65)

    console.time('Medium Row Insert')
    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await shard.insert(data)
    }
    const end = performance.now()
    console.timeEnd('Medium Row Insert')

    const duration = end - start
    const ops = (count / duration) * 1000

    console.log(`[Medium Row Insert] Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }, 60000)
})
