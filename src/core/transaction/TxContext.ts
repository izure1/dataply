
import { AsyncLocalStorage } from 'node:async_hooks'
import { Transaction } from './Transaction'

export class TransactionContext {
  private readonly storage = new AsyncLocalStorage<Transaction>()

  run<T>(tx: Transaction, callback: () => T): T {
    return this.storage.run(tx, callback)
  }

  get(): Transaction | undefined {
    return this.storage.getStore()
  }
}
