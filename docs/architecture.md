# Dataply Architecture

This document describes the internal architecture of Dataply version 0.0.17-alpha.

## System Overview

Dataply is an embedded database based on B+Tree indexing and page-based storage engine. It supports transaction management and Write-Ahead Logging (WAL).

### Class Diagram

```mermaid
classDiagram
    class Dataply {
        +init()
        +createTransaction()
        +insert()
        +select()
    }

    class DataplyAPI {
        -PageFileSystem pfs
        -RowTableEngine rowTableEngine
        +createTransaction()
    }

    class Transaction {
        -Map dirtyPages
        -Map undoPages
        -PageMVCCStrategy pageStrategy
        +readPage(pageId)
        +writePage(pageId, data)
        +commit()
        +rollback()
    }

    class PageFileSystem {
        -PageMVCCStrategy pageStrategy
        -WALManager walManager
        +getPageStrategy()
        +get(pageId, tx)
        +setPage(pageId, data, tx)
        +appendNewPage(type, tx)
    }

    class PageMVCCStrategy {
        -LRUMap cache
        -int fileHandle
        +read(pageId)
        +write(pageId, data)
        +exists(pageId)
    }

    class WALManager {
        +recover(writePage)
        +prepareCommit(dirtyPages)
        +finalizeCommit(hasActiveTransactions)
    }

    class RowTableEngine {
        -BPTree bptree
        +insert()
        +selectByPK()
    }

    class BPTree {
        <<Library: serializable-bptree>>
        -SerializeStrategyAsync strategy
    }

    class RowIndexStrategy {
        -PageFileSystem pfs
        +read(id)
        +write(id, node)
        +id(isLeaf)
    }

    Dataply --> DataplyAPI
    DataplyAPI --> PageFileSystem
    DataplyAPI --> RowTableEngine
    DataplyAPI ..> Transaction : Creates

    RowTableEngine --> BPTree
    BPTree --> RowIndexStrategy : Uses
    RowIndexStrategy --> PageFileSystem : Reads/Writes Pages
    
    PageFileSystem --> PageMVCCStrategy : Exposes
    PageFileSystem --> WALManager : Recovery & WAL

    Transaction --> PageMVCCStrategy : I/O & Caching
    note for PageMVCCStrategy "Disk I/O & LRU Cache"
    note for Transaction "Dirty Buffer & Undo Snapshot"
    note for WALManager "WAL & Recovery"
    note for RowIndexStrategy "BPTree Integration (ID Reservation)"
    note for PageFileSystem "Logical Page Management"
```

### Component Details

#### 1. Dataply & DataplyAPI
- **Role**: Top-level interface for user interaction.
- **Responsibilities**:
  - Database initialization and shutdown (`init`, `close`).
  - Transaction context creation and management (`createTransaction`).
  - Delegation of requests to lower-level engines (`ROW_TABLE`, `KEY_VALUE`, etc.).

#### 2. Transaction
- **Role**: Unit of work manager for ACID transactions.
- **Responsibilities**:
  - **Dirty Buffer**: Temporarily buffer modified pages in memory within a transaction.
  - **Undo Snapshot**: Preserve the state of pages before modification to support rollback and isolation (MVCC).
  - **Lock Acquisition**: Request acquisition of page-level locks.

#### 3. PageMVCCStrategy
- **Role**: Strategy implementation responsible for physical storage and caching of data.
- **Responsibilities**:
  - **I/O Handling**: Perform actual read/write operations against the OS file system.
  - **LRU Cache**: Optimize performance by caching frequently accessed pages in memory.
  - Operates independently of transactions, ensuring consistent physical access.

#### 4. WALManager
- **Role**: Logging and recovery system for data integrity.
- **Responsibilities**:
  - **WAL (Write-Ahead Logging)**: Sequentially record page changes to a log file before writing to disk.
  - **Recovery**: Perform data recovery (Redo) based on logs in case of abnormal shutdown.

#### 5. PageFileSystem
- **Role**: Logical manager for page units.
- **Responsibilities**:
  - Page allocation and deallocation (`appendNewPage`, `freePage`).
  - Metadata management (page count, root node ID, etc.).
  - Translating logical page IDs to physical access via `PageMVCCStrategy`.

#### 6. RowIndexStrategy
- **Role**: Mediator between B+Tree and the page system.
- **Responsibilities**:
  - **ID Reservation**: Prevents MVCC conflicts by reserving IDs without immediately creating physical pages when B+Tree nodes are requested.
  - Handles serialization/deserialization between B+Tree node objects (JSON/Struct) and binary pages.

## Transaction Write Flow

The following diagram illustrates the call flow during data insertion. It shows how changes in the B+Tree are stored in the transaction buffer via the page system and persisted to disk at commit time.

```mermaid
sequenceDiagram
    participant User
    participant API as DataplyAPI
    participant Engine as RowTableEngine
    participant BPT as BPTree (MVCC)
    participant RIS as RowIndexStrategy
    participant PFS as PageFileSystem
    participant TX as Transaction
    participant Strat as PageMVCCStrategy
    participant Disk

    User->>API: insert(data)
    API->>API: createTransaction() (Implicit)
    API->>Engine: insert(data, tx)
    
    rect rgb(240, 240, 240)
        Note over Engine, BPT: BPTree Operation
        Engine->>BPT: create(key, value)
        BPT->>BPT: Internal MVCC logic
        BPT->>RIS: id(leaf)
        RIS->>PFS: reserve ID (metadata update)
        RIS-->>BPT: return nodeId
        
        BPT->>RIS: write(nodeId, nodeData)
        RIS->>PFS: get(pageId, tx)
        PFS->>TX: readPage(pageId)
        TX->>Strat: read(pageId) (if not in dirty)
        Strat-->>TX: page data
        TX-->>PFS: page data
        PFS-->>RIS: page buffer
        
        Note right of RIS: Serialize Node to Page
        RIS->>PFS: setPage(pageId, buffer, tx)
        PFS->>TX: writePage(pageId, buffer)
        TX->>TX: Add to Dirty Map
    end

    Engine-->>API: success
    
    User->>API: commit() (or auto-commit)
    API->>TX: commit()
    
    rect rgb(220, 250, 220)
        Note over TX, Disk: Commit Phase
        loop For each dirty page
            TX->>Strat: write(pageId, data)
            Strat->>Strat: Update LRU Cache
            Strat->>Disk: fs.write()
        end
        TX->>TX: Clear Dirty/Undo Maps
    end
    
    TX-->>API: committed
    API-->>User: result
```

## Refactoring Notes

### 1. Resolving MVCC Layer Conflicts
Previously, complex MVCC logic was handled at the VFS level. However, with `serializable-bptree` v8 introducing its own `mvcc-api`, a **Double MVCC Conflict** issue arose.

To resolve this, the structure was simplified as follows:
- **Transaction**: Lightweight management focusing only on `Dirty Buffer` and `Undo Snapshot` instead of complex state management.
- **PageMVCCStrategy**: Dedicated solely to pure disk I/O and caching (LRU).
- **BPTree Integration**: In `RowIndexStrategy`, page ID reservation was separated from actual creation to prevent the B+Tree's internal MVCC from misidentifying new keys as "already existing."

### 2. File System Role Separation
- **WALManager**: Dedicated to WAL (Write-Ahead Log) management and recovery.
- **PageFileSystem**: Handles logical page management and metadata processing.
- **PageMVCCStrategy**: Manages physical file access and caching strategies.

