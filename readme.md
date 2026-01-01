![node.js workflow](https://github.com/izure1/shard/actions/workflows/node.js.yml/badge.svg)

# Shard

**Shard** is a lightweight, high-performance storage engine designed for Node.js. It provides reliable and fast data management by supporting MVCC (Multi-Version Concurrency Control), WAL (Write-Ahead Logging), and B+Tree indexing.

## Key Features

- **🚀 High-Performance B+Tree Indexing**: Supports fast data retrieval and management based on Primary Keys.
- **🛡️ MVCC Support**: Enables non-blocking read operations and guarantees data isolation between transactions.
- **📝 WAL (Write-Ahead Logging)**: Ensures data integrity and provides recovery capabilities in case of system failures.
- **💼 Transaction Mechanism**: Supports Commit and Rollback for atomic operations.
- **📦 Page-Based Storage**: Efficient page caching and disk I/O optimization through Virtual File System (VFS).
- **⌨️ TypeScript Support**: Provides comprehensive type definitions for all APIs.

## Installation

```bash
npm install shard
```

## Quick Start

```typescript
import { Shard } from 'shard'

// Open Shard instance
const shard = new Shard('./data.db', {
  pageSize: 8192,
  wal: './data.db.wal'
})

async function main() {
  // Initialization (Required)
  await shard.init()

  // Insert data
  const pk = await shard.insert('Hello, Shard!')
  console.log(`Inserted row with PK: ${pk}`)

  // Select data
  const data = await shard.select(pk)
  console.log(`Read data: ${data}`)

  // Close shard
  await shard.close()
}

main()
```

## Transaction Management

### Explicit Transactions
You can group multiple operations into a single unit of work to ensure atomicity.

```typescript
const tx = shard.createTransaction()

try {
  await shard.insert('Data 1', tx)
  await shard.update(pk, 'Updated Data', tx)
  
  await tx.commit() // Persist changes to disk and clear WAL on success
} catch (error) {
  await tx.rollback() // Revert all changes on failure (Undo)
}
```

### Auto-Transaction
If you omit the `tx` argument when calling methods like `insert`, `update`, or `delete`, Shard internally **creates an individual transaction automatically**.

- **Guaranteed Atomicity**: Even single operations are processed within an internal transaction, ensuring they are only finalized on success and rolled back on failure.
- **Performance Note**: For batch processing or multiple related operations, wrapping them in a single explicit transaction is significantly faster than relying on auto-transactions due to reduced I/O overhead.

## API Reference

### Shard Class

#### `constructor(file: string, options?: ShardOptions): Shard`
Opens a database file. If the file does not exist, it creates and initializes a new one.
- `options.pageSize`: Size of a page (Default: 8192, must be a power of 2)
- `options.wal`: Path to the WAL file. If omitted, WAL is disabled.

#### `async init(): Promise<void>`
Initializes the instance. Must be called before performing any CRUD operations.

#### `async insert(data: string | Uint8Array, tx?: Transaction): Promise<number>`
Inserts new data. Returns the Primary Key (PK) of the created row.

#### `async select(pk: number, asRaw?: boolean, tx?: Transaction): Promise<string | Uint8Array | null>`
Retrieves data based on the PK. Returns `Uint8Array` if `asRaw` is true.

#### `async update(pk: number, data: string | Uint8Array, tx?: Transaction): Promise<void>`
Updates existing data.

#### `async delete(pk: number, tx?: Transaction): Promise<void>`
Marks data as deleted.

#### `createTransaction(): Transaction`
Creates a new transaction instance.

#### `async close(): Promise<void>`
Closes the file handles and shuts down safely.

### Transaction Class

#### `async commit(): Promise<void>`
Permanently reflects all changes made during the transaction to disk and releases locks.

#### `async rollback(): Promise<void>`
Cancels all changes made during the transaction and restores the original state.

## Internal Architecture

Shard implements the core principles of modern database engines in a lightweight and efficient manner.

### 1. Layered Architecture
```mermaid
graph TD
    API[Shard API] --> RTE[Row Table Engine]
    RTE --> PFS[Page File System]
    PFS --> VFS[Virtual File System / Cache]
    VFS --> WAL[Write Ahead Log]
    VFS --> DISK[(Database File)]
    
    TX[Transaction Manager] -.-> VFS
    TX -.-> LM[Lock Manager]
```

### 2. Page-Based Storage and VFS Caching
- **Fixed-size Pages**: All data is managed in fixed-size units (default 8KB) called pages.
- **VFS Cache**: Minimizes disk I/O by caching frequently accessed pages in memory.
- **Dirty Page Tracking**: Tracks modified pages (Dirty) to synchronize them with disk efficiently only at the time of commit.

### 3. MVCC and Snapshot Isolation
- **Non-blocking Reads**: Read operations are not blocked by write operations.
- **Undo Log**: When a transaction modifies a page, it keeps the original data in an **Undo Buffer**. Other transactions trying to read the same page are served this snapshot to ensure consistent reads.
- **Rollback Mechanism**: Upon transaction failure, the Undo Buffer is used to instantly restore pages to their original state.

### 4. WAL (Write-Ahead Logging) and Crash Recovery
- **Performance and Reliability**: All changes are recorded in a sequential log file (WAL) before being written to the actual data file. This converts random writes into sequential writes for better performance and ensures data integrity.
- **Crash Recovery**: When restarting after an unexpected shutdown, Shard reads the WAL to automatically replay (Redo) any changes that weren't yet reflected in the data file.

### 5. Concurrency Control and Indexing
- **Page-level Locking**: Prevents data contention by controlling sequential access to pages through the `LockManager`.
- **B+Tree Index**: Uses a B+Tree structure guaranteeing $O(\log N)$ performance for maximized PK lookup efficiency.

## Performance

Shard is optimized for high-speed data processing. Below are the results of basic benchmark tests conducted on a local environment.

| Test Case | Count | Total Time | OPS (Operations Per Second) |
| :--- | :--- | :--- | :--- |
| **Bulk Insert (Batch)** | 10,000 | ~1,694ms | **~5,903 OPS** |
| **Bulk Insert (Individual)** | 100 | ~38ms | **~2,593 OPS** |
| **Bulk Insert with WAL** | 100 | ~854ms | **~117 OPS** |
| **Medium Row Insert (1KB)** | 100 | ~40ms | **~2,471 OPS** |

### Benchmark Analysis
- **Batching Efficiency**: Grouping operations into a single transaction is approximately **2.3x faster** than individual inserts by minimizing internal transaction management overhead.
- **WAL Trade-off**: Enabling Write-Ahead Logging ensures data durability but results in a significant performance decrease (approximately **22x slower** for individual inserts) due to synchronous I/O operations.
- **Node.js Optimization**: Shard is designed to provide competitive performance (over **5,000 OPS** in batch mode) for a pure TypeScript engine without native dependencies.

> [!NOTE]
> Tests were conducted on a standard local environment (Node.js v25+). Performance may vary depending on hardware specifications (especially SSD/HDD) and system load.

## License

MIT
