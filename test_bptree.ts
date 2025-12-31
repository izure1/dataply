
import { BPTreeAsync, InMemoryStoreStrategyAsync, NumericComparator } from 'serializable-bptree'

async function test() {
  const strategy = new InMemoryStoreStrategyAsync(4) // Order 4
  const comparator = new NumericComparator()
  const tree = new BPTreeAsync(strategy, comparator)

  console.log('Inserting PK 1 -> RID 100')
  await tree.init()
  await tree.insert(1, 100)

  let keys = await tree.keys({ equal: 1 })
  console.log('Keys after insert:', keys.size) // Should be 1
  console.log('Value:', keys.values().next().value) // Should be 100

  console.log('Deleting PK 1 with WRONG value 999 (delete(1, 999))')
  await tree.delete(1, 999)
  keys = await tree.keys({ equal: 1 })
  console.log('Keys after delete(1, 999):', keys.size) // Should be 1 (Not deleted)

  console.log('Deleting PK 1 with CORRECT value 100 (delete(1, 100))')
  await tree.delete(1, 100)
  keys = await tree.keys({ equal: 1 })
  console.log('Keys after delete(1, 100):', keys.size) // Should be 0

  console.log('Re-inserting PK 1 -> RID 200')
  await tree.insert(1, 200)
  keys = await tree.keys({ equal: 1 })
  console.log('Keys after re-insert:', keys.size)
  console.log('Value:', keys.values().next().value) // Should be 200

  // Clean up? In memory, no need.
}

test().catch(console.error)
