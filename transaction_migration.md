# 트랜잭션 마이그레이션

## 기존

```typescript
const tx = db.createTransaction()

await db.insert('test', tx)
await tx.commit()
```

## 마이그레이션

```typescript
// db.createTransaction 메서드는 private 또는 protected로 변경되어서 사용자가 호출할 수 없음

// 대신 withReadTransaction, 또는 withWriteTransaction, withReadStreamTransaction 사용
// 이 방식은 쓰기 작업 도중에 다른 쓰기 작업이 발생할 경우, 직렬화하여 처리하도록 도와줍니다.
await db.withWriteTransaction(async (tx) => {
  await db.insert('test', tx)
})
```