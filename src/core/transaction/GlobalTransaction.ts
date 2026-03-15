import { Transaction } from './Transaction'
import { Dataply } from '../Dataply'

/**
 * Global Transaction Manager.
 * Coordinates transactions across multiple instances (shards).
 * 
 * Note: Without WAL prepare phase, atomicity across shards is best-effort.
 */
export class GlobalTransaction {
  private transactions: Transaction[] = []
  private isCommitted: boolean = false
  private isRolledBack: boolean = false

  /**
   * Executes a global transaction across multiple Dataply instances using a callback.
   * Locks are acquired in the order instances are provided.
   * @param dbs Array of Dataply instances
   * @param callback Function to execute with the array of Transactions
   */
  static async Run<T>(
    dbs: Dataply[],
    callback: (txs: Transaction[]) => Promise<T>
  ): Promise<T> {
    const globalTx = new GlobalTransaction()
    const txs: Transaction[] = []
    const releases: (() => void)[] = []

    try {
      // 1. Acquire global write locks for all instances to prevent deadlocks with other local transactions
      for (const db of dbs) {
        const release = await (db as any).api.acquireWriteLock()
        releases.push(release)

        const tx = (db as any).api.createTransaction()
        tx.__setWriteLockRelease(release)
        txs.push(tx)
        globalTx.add(tx)
      }

      // 2. Execute business logic
      const result = await callback(txs)

      // 3. Commit all if successful
      await globalTx.commit()
      return result
    } catch (e) {
      // 4. Rollback all on failure
      await globalTx.rollback()

      // If any locks were acquired but tx creation failed halfway, release them manually
      // Those inside 'tx' will be released by globalTx.rollback()
      for (let i = txs.length; i < releases.length; i++) {
        releases[i]()
      }

      throw e
    }
  }

  protected constructor() { }

  /**
   * Adds a transaction to the global transaction.
   * @param tx Transaction to add
   */
  protected add(tx: Transaction) {
    this.transactions.push(tx)
  }

  /**
   * Commits all transactions.
   * Note: This is now a single-phase commit. For true atomicity across shards,
   * each instance's WAL provides durability, but cross-shard atomicity is best-effort.
   */
  protected async commit(): Promise<void> {
    if (this.isCommitted || this.isRolledBack) {
      throw new Error('Transaction is already finished')
    }

    try {
      await Promise.all(this.transactions.map(tx => tx.commit()))
      this.isCommitted = true
    } catch (e) {
      // On any commit failure, try to rollback uncommitted transactions
      // Note: Some transactions may have already committed - partial commit is possible
      await this.rollback()
      throw new Error(`Global commit failed: ${e}`)
    }
  }

  /**
   * Rolls back all transactions.
   */
  protected async rollback(): Promise<void> {
    if (this.isCommitted || this.isRolledBack) {
      return
    }

    await Promise.all(this.transactions.map(tx => tx.rollback().catch(() => { })))
    this.isRolledBack = true
  }
}
