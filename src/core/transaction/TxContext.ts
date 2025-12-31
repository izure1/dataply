
import { AsyncLocalStorage } from 'node:async_hooks'
import { Transaction } from './Transaction'

const storage = new AsyncLocalStorage<Transaction>()

export const TxContext = {
  run: <T>(tx: Transaction, callback: () => T): T => {
    return storage.run(tx, callback)
  },
  get: (): Transaction | undefined => {
    return storage.getStore()
  }
}
