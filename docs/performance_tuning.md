# Performance Tuning Guide

Dataply provides several mechanisms to optimize data access and storage performance. This guide explains how to leverage these features effectively.

## 1. Batch Operations

Individual operations in Dataply are ACID-compliant and involve transaction overhead (locking, WAL logging, metadata updates). For bulk data handling, batch methods are significantly faster.

### `insertBatch` vs `insert`
- **`insert`**: Creates an internal transaction (or uses an external one) for each call. Multiple calls result in multiple I/O cycles for metadata and WAL.
- **`insertBatch`**: Processes multiple records within a single unit of work. It acquires locks once and updates the database metadata after processing the entire batch, reducing disk synchronization overhead.

### `selectMany` vs `select`
- **`select`**: Performs a single B+Tree traversal per PK.
- **`selectMany`**: Optimizes retrieval through **PK Clustering**. It sorts the requested PKs and groups them into clusters. This allows the engine to:
    - Perform range scans on the B+Tree instead of multiple point lookups.
    - Group physical reads by Page ID, minimizing the number of times the same page is loaded from disk.

### `deleteBatch` vs `delete`
- **`delete`**: Similar to `insert`, it performs deletion and metadata updates per PK.
- **`deleteBatch`**: Deletes multiple records within a single unit of work. Efficiently cleans up rows and their associated index entries under one transaction, significantly reducing overhead.

## 2. Using Explicit Transactions

When performing multiple related operations, always use an explicit transaction:

```typescript
await db.withWriteTransaction(async (tx) => {
  await db.insert(data1, tx);
  await db.insert(data2, tx);
});
```

This ensures that disk synchronization (fsync) and WAL checkpoints happen less frequently compared to auto-transactions.

## 3. Page Cache Configuration

The `pageCacheCapacity` option controls memory usage versus I/O performance.
- A larger cache reduces disk reads for frequently accessed data (hot pages).
- The default is 10,000 pages (~80MB for 8KB pages). Adjust this based on your available memory and dataset size.

## 4. WAL Checkpoints

The `walCheckpointThreshold` option (default: 1000) determines how often the Write-Ahead Log is synchronized back to the main data file.
- Higher values improve write throughput but increase recovery time after a crash.
- Lower values ensure the data file is kept up-to-date more frequently.
