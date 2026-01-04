import fs from 'node:fs'
import { Dataply } from '../src/core/Dataply'

describe('Page Size Persistence', () => {
  const TEST_FILE = 'test_pagesize.dataply'

  beforeEach(() => {
    if (fs.existsSync(TEST_FILE)) {
      fs.unlinkSync(TEST_FILE)
    }
  })

  afterEach(() => {
    if (fs.existsSync(TEST_FILE)) {
      fs.unlinkSync(TEST_FILE)
    }
  })

  it('should throw error when creating dataply with page size less than 4096', () => {
    expect(() => {
      new Dataply(TEST_FILE, { pageSize: 1024 })
    }).toThrow('Page size must be at least 4096 bytes')
  })

  it('should persist page size in metadata', async () => {
    const dataply1 = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply1.init()
    const pageSize1 = (dataply1 as any).api.options.pageSize
    expect(pageSize1).toBe(8192)
    await dataply1.close()

    // Re-open with different page size option - should be ignored
    const dataply2 = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply2.init()
    const pageSize2 = (dataply2 as any).api.options.pageSize

    expect(pageSize2).toBe(8192) // Should still be 8192

    // Check if PFS was initialized with correct page size
    expect((dataply2 as any).api.pfs.pageSize).toBe(8192)

    await dataply2.close()
  })

  it('should load persisted page size when opened without options', async () => {
    const dataply1 = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply1.init()
    await dataply1.close()

    const dataply2 = new Dataply(TEST_FILE)
    await dataply2.init()
    const pageSize2 = (dataply2 as any).api.options.pageSize

    expect(pageSize2).toBe(8192)

    await dataply2.close()
  })

  it('should work correctly with IO operations after reloading', async () => {
    // 1. Create with custom page size
    const dataply1 = new Dataply(TEST_FILE, { pageSize: 8192 })
    await dataply1.init()
    const pk = await dataply1.insert('test data')
    await dataply1.close()

    // 2. Re-open with default options (which usually defaults to something else or standard)
    // trying to pass a conflicting page size
    const dataply2 = new Dataply(TEST_FILE, { pageSize: 4096 })
    await dataply2.init()

    // 3. Verify data
    const data = await dataply2.select(pk)
    expect(data).toBe('test data')

    // 4. Insert new data
    const pk2 = await dataply2.insert('test data 2')
    const data2 = await dataply2.select(pk2)
    expect(data2).toBe('test data 2')

    await dataply2.close()
  })
})
