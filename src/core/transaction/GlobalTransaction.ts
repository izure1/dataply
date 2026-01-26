import { Transaction } from './Transaction'

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
   * Adds a transaction to the global transaction.
   * @param tx Transaction to add
   */
  add(tx: Transaction) {
    this.transactions.push(tx)
  }

  /**
   * Commits all transactions.
   * Note: This is now a single-phase commit. For true atomicity across shards,
   * each instance's WAL provides durability, but cross-shard atomicity is best-effort.
   */
  async commit(): Promise<void> {
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
  async rollback(): Promise<void> {
    if (this.isCommitted || this.isRolledBack) {
      return
    }

    await Promise.all(this.transactions.map(tx => tx.rollback().catch(() => { })))
    this.isRolledBack = true
  }
}
