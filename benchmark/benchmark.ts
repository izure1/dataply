import { Shard } from '../src/core/Shard'
import path from 'path'
import fs from 'fs'

const TEST_FILE = path.join(__dirname, 'benchmark_shard.dat')
const WAL_FILE = path.join(__dirname, 'benchmark_shard.wal')

async function cleanup() {
  if (fs.existsSync(TEST_FILE)) {
    try { await fs.promises.unlink(TEST_FILE) } catch (e) { }
  }
  if (fs.existsSync(WAL_FILE)) {
    try { await fs.promises.unlink(WAL_FILE) } catch (e) { }
  }
}

async function benchmark() {
  console.log('--- Shard Performance Benchmark ---')

  // 1. Bulk Insert (Batch)
  {
    await cleanup()
    const shard = new Shard(TEST_FILE, { pageSize: 4096 })
    await shard.init()

    const count = 10000
    const dataList: Uint8Array[] = []
    for (let i = 0; i < count; i++) {
      dataList.push(new Uint8Array([1, 2, 3, 4, 5]))
    }

    const start = performance.now()
    await shard.insertBatch(dataList)
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    console.log(`[Bulk Insert (Batch)] Count: ${count}, Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }

  // 2. Bulk Insert (Individual)
  {
    await cleanup()
    const shard = new Shard(TEST_FILE, { pageSize: 4096 })
    await shard.init()

    const count = 100
    const data = new Uint8Array([1, 2, 3, 4, 5])

    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await shard.insert(data)
    }
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    console.log(`[Bulk Insert (Individual)] Count: ${count}, Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }

  // 3. Bulk Insert with WAL
  {
    await cleanup()
    const shard = new Shard(TEST_FILE, { pageSize: 4096, wal: WAL_FILE })
    await shard.init()

    const count = 100
    const data = new Uint8Array([1, 2, 3, 4, 5])

    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await shard.insert(data)
    }
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    console.log(`[Bulk Insert with WAL] Count: ${count}, Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }

  // 4. Medium Row Insert (1KB)
  {
    await cleanup()
    const shard = new Shard(TEST_FILE, { pageSize: 8192 })
    await shard.init()

    const count = 100
    const data = new Uint8Array(1024).fill(65)

    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await shard.insert(data)
    }
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    console.log(`[Medium Row Insert (1KB)] Count: ${count}, Total: ${duration.toFixed(2)}ms, OPS: ${ops.toFixed(2)}`)

    await shard.close()
  }

  await cleanup()
  console.log('--- Benchmark Finished ---')
}

benchmark().catch(console.error)
