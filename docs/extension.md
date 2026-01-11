# Extending Dataply Guide

Dataply provides a flexible extension system through the `DataplyAPI` class and a powerful Hook mechanism. This guide explains how to build custom functionality on top of the Dataply core.

---

## 🏗️ Core Concept: Hook System

Dataply's internal Hook system is powered by the [hookall](https://github.com/izure1/hookall) library. It allows you to intercept internal processes and inject custom logic at specific lifecycles.

- **Inheritance**: Extend `DataplyAPI` to access internal protected members (pfs, rowTableEngine, etc.).
- **Events**: Register listeners for events like `init`, `close`, and CRUD operations.
- **Interception**: Modify data or control flow before or after core operations.

### Hook Execution Flow (Waterfall Pattern)

The `trigger` method in Dataply (via `hookall`) acts as a data pipeline. Values are passed through each stage, transformed, and finally returned to the caller.

```mermaid
graph TD
    Start(["1. trigger(name, initialValue)"]) --> Before["2. Before Hooks:<br/>f(initialValue) -> val1"]
    Before --> Main[["3. Main Callback (Core):<br/>f(val1) -> val2"]]
    Main --> After["4. After Hooks:<br/>f(val2) -> finalValue"]
    After --> End(["5. Return finalValue to Caller"])

    style Main fill:#f9f,stroke:#333
    style Before fill:#dfd,stroke:#333
    style After fill:#dfd,stroke:#333
    style Start fill:#eee
    style End fill:#eee
```

1.  **Before Hooks**: Receive the `initialValue`. If multiple hooks exist, they process the value sequentially.
2.  **Main Callback**: Receives the processed value from the last Before Hook. This is the "Core Logic."
3.  **After Hooks**: Receive the return value from the Main Callback for final processing.

---

### Case Study: Database Initialization (`init`)

Now, let's see how the **Waterfall Pattern** above is applied to the actual `init` process. In this case, the `initialValue` is the newly created **Transaction (tx)** object.

```mermaid
graph TD
    Call(["1. DataplyAPI.init() called"]) --> NewTx["2. Create new Transaction() -> tx"]
    NewTx --> GetStatus["3. Check isNewlyCreated status"]
    GetStatus --> Trigger["4. trigger('init', tx, isNewlyCreated)"]

    subgraph Pipeline ["Waterfall Chain (from Diagram Above)"]
        direction TB
        Before["5. Before Hooks: Receive (tx, ...) -> Return tx"]
        Main[["6. Main Core Logic: Setup DB -> Return tx"]]
        After["7. After Hooks: Seed Data -> Return tx"]
        
        Before --> Main --> After
    end

    Trigger --> Before
    After --> Commit["8. Commit Transaction"]
    Commit --> End([9. Initialization Complete])

    style Main fill:#f9f,stroke:#333
    style Before fill:#dfd,stroke:#333
    style After fill:#dfd,stroke:#333
    style Pipeline fill:#f5f5ff,stroke:#666,stroke-dasharray: 5 5
```

> [!TIP]
> **Why this matters**: If you register a hook for `init`, you are stepping into the middle of this pipeline. You receive the `tx` that the Engine just created, perform your own operations (like seeding initial data), and pass the `tx` back so the Engine can finally commit it.

> [!CAUTION]
> **Value Chain**: If any hook fails to return a value, the subsequent stages will receive `undefined`, which usually leads to errors or transaction failures. Always return the data (or `tx` object).

> [!IMPORTANT]
> **Sequential Flow**: Hooks are executed sequentially. Each hook must return the `tx` object so the next hook or core logic can continue using the same transaction. If a hook fails to return `tx`, the chain will break.

---

## 🚀 Inheritance and Basic Hooks

The most common way to extend Dataply is to create a subclass of `DataplyAPI`.

### Basic Example: Initializing with Default Data
You can use the `onceAfter` hook to perform one-time setup tasks, such as creating initial configuration records when the database is first created.

```typescript
import { DataplyAPI, type DataplyOptions, Transaction } from 'dataply';

class MyExtendedAPI extends DataplyAPI {
  constructor(file: string, options: DataplyOptions) {
    super(file, options);

    // Register a hook that runs ONLY ONCE after 'init' completes
    this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
      if (isNewlyCreated) {
        console.log('✨ Database newly created. Seeding initial data...');
        
        // Always pass the 'tx' argument to prevent deadlocks (see Safety Rules below)
        const pk = await this.insertAsOverflow(
          JSON.stringify({ nickname: 'admin', role: 'superuser' }), 
          false, 
          tx
        );
        console.log(`✅ Welcome record created with PK: ${pk}`);
      }
      
      // CRITICAL: Always return the tx object
      return tx;
    });
  }
}
```

---

## 🛡️ Important: Transaction (tx) Safety Rules

When working within Hooks, following these rules is **mandatory** to prevent system hangs or data corruption.

### 1. Always Return the `tx` object
Asynchronous hooks (like `init`) receive a `tx` object and must return it. This ensures the transaction chain continues correctly.

### 2. Reuse the Provided `tx`
Inside a hook, you must pass the provided `tx` argument to any internal methods (`insert`, `update`, etc.).
> [!CAUTION]
> **Why?** When a hook is triggered, the database might already have an active **Write Lock** on specific pages. If you call a method WITHOUT passing the `tx`, Dataply will try to start a *new* transaction, which will wait indefinitely for the lock held by the hook itself—causing a **Deadlock**.

---

## 💡 Best Practice: Custom Metadata Strategy

You can store application-specific metadata (schema versions, settings, etc.) directly in the database by leveraging the uniqueness of the first record.

- **PK 1 is Special**: The first successful insertion in a new Dataply instance is guaranteed to have `pk: 1`.
- **Custom Metadata Page**: Use `pk: 1` as your own "Internal Configuration Page".
- **Update Logic**: Instead of inserting new records, keep your configuration updated at `pk: 1`.

### Implementation Example
```typescript
this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
  if (isNewlyCreated) {
    // Save versioning info at PK 1
    const initialConfig = { version: '1.0.0', lastUpdate: Date.now() };
    await this.insertAsOverflow(JSON.stringify(initialConfig), false, tx);
  } else {
    // Load existing config
    const config = await this.select(1, false, tx);
    console.log('🛠️ Current Schema Version:', JSON.parse(config).version);
  }
  return tx;
});
```

---

## 📦 Recommended Architecture: Business Logic Wrapping

For professional applications, wrap your extended API in a service class to provide a clean, high-level interface.

```typescript
class MyDatabaseService {
  protected readonly api: MyExtendedAPI;

  constructor(file: string, options?: DataplyOptions) {
    this.api = new MyExtendedAPI(file, options ?? {});
  }

  async start() {
    await this.api.init();
  }

  async upsertConfig(newConfig: object) {
    // runWithDefault automates internal transaction handling
    return this.api.runWithDefault(async (tx) => {
      return this.api.update(1, JSON.stringify(newConfig), tx);
    });
  }

  async stop() {
    await this.api.close();
  }
}
```

---

## 🔍 Hook Event Reference

| Event | Trigger Point | Arguments | Return Value |
| :--- | :--- | :--- | :--- |
| `init` | Called during `DataplyAPI.init()` | `(tx: Transaction, isNewlyCreated: boolean)` | `Promise<Transaction>` |
| `close` | Called during `DataplyAPI.close()` | `()` | `Promise<void>` |

> [!TIP]
> For advanced use cases, you can access protected members like `this.pfs` (Page File System) or `this.rowTableEngine` within your subclass to perform low-level page manipulations.

---
*For internal type definitions, see the `DataplyAPIAsyncHook` interface in [DataplyAPI.ts](../src/core/DataplyAPI.ts).*
