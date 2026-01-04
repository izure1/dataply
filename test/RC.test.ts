import { Dataply } from '../src/core/Dataply'
import { TxContext } from '../src/core/transaction/TxContext'
import { type RowTableEngine } from '../src/core/RowTableEngine'
import fs from 'node:fs'

const DB_PATH = 'temp_rc_test.db'
if (fs.existsSync(DB_PATH)) fs.unlinkSync(DB_PATH)

describe('Row Count Test', () => {
  let dataply: Dataply

  beforeEach(async () => {
    if (fs.existsSync(DB_PATH)) fs.unlinkSync(DB_PATH)
    dataply = new Dataply(DB_PATH)
    await dataply.init()
  })

  afterEach(async () => {
    await dataply.close()
    if (fs.existsSync(DB_PATH)) fs.unlinkSync(DB_PATH)
  })

  it('should track row count correctly on insert', async () => {
    const rowTableEngine = (dataply as any).api.rowTableEngine as RowTableEngine
    const tx = dataply.createTransaction()

    await TxContext.run(tx, async () => {
      let count = await rowTableEngine.getRowCount(tx)
      expect(count).toBe(0)

      await rowTableEngine.insert(new Uint8Array([1, 2, 3]), true, tx)

      count = await rowTableEngine.getRowCount(tx)
      expect(count).toBe(1)
    })
  })

  it('should track row count correctly on delete', async () => {
    const rowTableEngine = (dataply as any).api.rowTableEngine as RowTableEngine
    const tx = dataply.createTransaction()

    await TxContext.run(tx, async () => {
      const pk = await rowTableEngine.insert(new Uint8Array([1, 2, 3]), true, tx)
      expect(await rowTableEngine.getRowCount(tx)).toBe(1)

      await rowTableEngine.delete(pk, true, tx)
      expect(await rowTableEngine.getRowCount(tx)).toBe(0)
    })
  })

  it('should not change row count on update', async () => {
    const rowTableEngine = (dataply as any).api.rowTableEngine as RowTableEngine
    const tx = dataply.createTransaction()

    await TxContext.run(tx, async () => {
      const pk = await rowTableEngine.insert(new Uint8Array([1, 2, 3]), true, tx)
      expect(await rowTableEngine.getRowCount(tx)).toBe(1)

      // Update with same size
      await rowTableEngine.update(pk, new Uint8Array([4, 5, 6]), tx)
      expect(await rowTableEngine.getRowCount(tx)).toBe(1)

      const largeData = new Uint8Array(100).fill(1)
      await rowTableEngine.update(pk, largeData, tx)
      expect(await rowTableEngine.getRowCount(tx)).toBe(1)
    })
  })
})

