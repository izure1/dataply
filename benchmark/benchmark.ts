import { Dataply } from '../src/core/Dataply'
import path from 'node:path'
import fs from 'node:fs'

const TEST_FILE = path.join(__dirname, 'benchmark_dataply.dat')
const WAL_FILE = path.join(__dirname, 'benchmark_dataply.wal')

async function cleanup() {
  if (fs.existsSync(TEST_FILE)) {
    try { await fs.promises.unlink(TEST_FILE) } catch (e) { }
  }
  if (fs.existsSync(WAL_FILE)) {
    try { await fs.promises.unlink(WAL_FILE) } catch (e) { }
  }
}

interface BenchmarkResult {
  name: string
  count: number
  totalTime: number
  ops: number
}

async function runSingleBenchmark(): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []

  // 1. Bulk Insert (Batch)
  {
    await cleanup()
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply.init()

    const count = 10000
    const dataList: Uint8Array[] = []
    for (let i = 0; i < count; i++) {
      dataList.push(new Uint8Array([1, 2, 3, 4, 5]))
    }

    const start = performance.now()
    await dataply.insertBatch(dataList)
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    results.push({ name: 'Bulk Insert (Batch)', count, totalTime: duration, ops })

    await dataply.close()
  }

  // 2. Bulk Insert (Individual)
  {
    await cleanup()
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply.init()

    const count = 100
    const data = new Uint8Array([1, 2, 3, 4, 5])

    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await dataply.insert(data)
    }
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    results.push({ name: 'Bulk Insert (Individual)', count, totalTime: duration, ops })

    await dataply.close()
  }

  // 3. Bulk Insert with WAL
  {
    await cleanup()
    const dataply = new Dataply(TEST_FILE, { pageSize: 4096, wal: WAL_FILE })
    await dataply.init()

    const count = 100
    const data = new Uint8Array([1, 2, 3, 4, 5])

    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await dataply.insert(data)
    }
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    results.push({ name: 'Bulk Insert with WAL', count, totalTime: duration, ops })

    await dataply.close()
  }

  // 4. Medium Row Insert (1KB)
  {
    await cleanup()
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const count = 100
    const data = new Uint8Array(1024).fill(65)

    const start = performance.now()
    for (let i = 0; i < count; i++) {
      await dataply.insert(data)
    }
    const end = performance.now()

    const duration = end - start
    const ops = (count / duration) * 1000
    results.push({ name: 'Medium Row Insert (1KB)', count, totalTime: duration, ops })

    await dataply.close()
  }

  // 5. selectMany Performance (500 PKs)
  {
    await cleanup()
    const dataply = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply.init()

    const count = 10000
    const selectCount = 500
    const data = new Uint8Array(100).fill(65)

    // Preparation: Insert data
    const allPks: number[] = []
    for (let i = 0; i < count / 1000; i++) {
      const pks = await dataply.insertBatch(Array(1000).fill(data))
      allPks.push(...pks)
    }

    const targetIndices = Array.from({ length: selectCount }, () => Math.floor(Math.random() * count))
    const targetPks = targetIndices.map(i => allPks[i])

    const start = performance.now()
    await dataply.selectMany(targetPks, true)
    const end = performance.now()

    const duration = end - start
    const ops = (selectCount / duration) * 1000
    results.push({ name: 'selectMany (500 PKs)', count: selectCount, totalTime: duration, ops })

    await dataply.close()
  }

  return results
}

async function benchmark() {
  console.log('--- Dataply Performance Benchmark (5 Runs Average) ---')

  const RUNS = 5
  const allResults: BenchmarkResult[][] = []

  for (let i = 0; i < RUNS; i++) {
    console.log(`Running iteration ${i + 1}/${RUNS}...`)
    const result = await runSingleBenchmark()
    allResults.push(result)
    // Cooldown
    await new Promise(resolve => setTimeout(resolve, 500))
  }

  console.log('\n--- Final Average Results ---')

  const categories = allResults[0].map(r => r.name)
  const finalResults: { name: string, avgTime: number, avgOps: number, count: number }[] = []

  for (const category of categories) {
    let totalOps = 0
    let totalTime = 0
    let count = 0

    for (const run of allResults) {
      const res = run.find(r => r.name === category)!
      totalOps += res.ops
      totalTime += res.totalTime
      count = res.count
    }

    const avgOps = totalOps / RUNS
    const avgTime = totalTime / RUNS

    finalResults.push({ name: category, avgTime, avgOps, count })

    console.log(`[${category}] Count: ${count}, Avg Time: ${avgTime.toFixed(2)}ms, Avg OPS: ${avgOps.toFixed(2)}`)
  }

  if (process.argv.includes('--json')) {
    const data = finalResults.map((r) => ({
      name: r.name,
      unit: 'ms',
      value: parseFloat(r.avgTime.toFixed(2))
    }))
    const outputPath = path.join(__dirname, 'benchmark-results.json')
    fs.writeFileSync(outputPath, JSON.stringify(data, null, 2))
    console.log(`\nBenchmark results saved to ${outputPath}`)
  }

  await cleanup()
  console.log('--- Benchmark Finished ---')
}

benchmark().catch(console.error)