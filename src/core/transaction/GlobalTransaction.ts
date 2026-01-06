import { Transaction } from './Transaction'

/**
 * Global Transaction Manager.
 * Coordinates transactions across multiple instances (shards) using 2-Phase Commit (2PC).
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
   * Commits all transactions atomically.
   * Phase 1: Prepare (Write WAL)
   * Phase 2: Commit (Write Commit Marker & Checkpoint)
   */
  async commit(): Promise<void> {
    if (this.isCommitted || this.isRolledBack) {
      throw new Error('Transaction is already finished')
    }

    // Phase 1: Prepare
    try {
      await Promise.all(this.transactions.map(tx => tx.prepare()))
    } catch (e) {
      // If any prepare fails, rollback everything
      await this.rollback()
      throw new Error(`Global commit failed during prepare phase: ${e}`)
    }

    // Phase 2: Commit
    try {
      await Promise.all(this.transactions.map(tx => tx.commit()))
      this.isCommitted = true
    } catch (e) {
      // This is a critical failure (Partial Commit)
      // In a strict distributed system, we would need a coordinator log to recover.
      // Here, we just report the error. Ideally, the instances that failed commit 
      // can still recover from WAL since they are prepared.
      throw new Error(`Global commit failed during finalize phase: ${e}`)
    }
  }

  /**
   * Rolls back all transactions.
   */
  async rollback(): Promise<void> {
    if (this.isCommitted || this.isRolledBack) {
      return
    }

    await Promise.all(this.transactions.map(tx => tx.rollback()))
    this.isRolledBack = true
  }
}
