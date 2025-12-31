# Shard

Shard는 Node.js를 위해 설계된 경량화된 고성능 저장 엔진입니다. MVCC(Multi-Version Concurrency Control), WAL(Write-Ahead Logging), 그리고 B+Tree 인덱싱을 지원하여 안정적이고 빠른 데이터 관리를 제공합니다.

## 주요 특징

- **🚀 고성능 B+Tree 인덱싱**: Primary Key를 기반으로 한 빠른 데이터 검색 및 관리를 지원합니다.
- **🛡️ MVCC 지원**: 비차단(Non-blocking) 읽기 작업을 지원하며, 트랜잭션 간의 데이터 격리를 보장합니다.
- **📝 WAL (Write-Ahead Logging)**: 시스템 장애 발생 시에도 데이터 무결성을 유지하고 복구할 수 있는 기능을 제공합니다.
- **💼 트랜잭션 매커니즘**: 원자적 작업을 위한 Commit 및 Rollback 기능을 지원합니다.
- **📦 페이지 기반 저장**: 가상 파일 시스템(VFS)을 통한 효율적인 페이지 캐싱 및 디스크 I/O 최적화를 수행합니다.
- **⌨️ TypeScript 지원**: 모든 API에 대해 완벽한 타입 정의를 제공합니다.

## 설치

```bash
npm install shard
```

## 빠른 시작

```typescript
import { Shard } from 'shard'

async function main() {
  // Shard 인스턴스 오픈
  const shard = Shard.Open('./data.db', {
    pageSize: 8192,
    wal: './data.db.wal'
  })

  // 초기화 (필수)
  await shard.init()

  // 데이터 삽입
  const pk = await shard.insert('Hello, Shard!')
  console.log(`Inserted row with PK: ${pk}`)

  // 데이터 조회
  const data = await shard.select(pk)
  console.log(`Read data: ${data}`)

  // 샤드 종료
  await shard.close()
}

main()
```

## 트랜잭션 사용법

```typescript
const tx = await shard.createTransaction()

try {
  await shard.insert('Important Data', tx)
  await shard.update(pk, 'Updated Data', tx)
  
  // 변경 사항 커밋
  await tx.commit()
} catch (error) {
  // 오류 발생 시 롤백
  await tx.rollback()
}
```

## API 레퍼런스

### Shard 클래스

#### `static Open(file: string, options?: ShardOptions): Shard`
데이터베이스 파일을 엽니다. 파일이 존재하지 않으면 새로 생성하고 초기화합니다.
- `options.pageSize`: 페이지 크기 (기본값: 8192, 2의 거듭제곱이어야 함)
- `options.wal`: WAL 파일 경로. 생략 시 WAL 기능이 비활성화됩니다.

#### `async init(): Promise<void>`
인스턴스를 초기화합니다. CRUD 작업을 수행하기 전에 반드시 호출해야 합니다.

#### `async insert(data: string | Uint8Array, tx?: Transaction): Promise<number>`
새 데이터를 삽입합니다. 생성된 행의 Primary Key(PK)를 반환합니다.

#### `async select(pk: number, asRaw?: boolean, tx?: Transaction): Promise<string | Uint8Array | null>`
PK를 기반으로 데이터를 조회합니다. `asRaw`가 true이면 `Uint8Array`를 반환합니다.

#### `async update(pk: number, data: string | Uint8Array, tx?: Transaction): Promise<void>`
기존 데이터를 업데이트합니다.

#### `async delete(pk: number, tx?: Transaction): Promise<void>`
데이터를 삭제 표시합니다.

#### `async createTransaction(): Promise<Transaction>`
새로운 트랜잭션 인스턴스를 생성합니다.

#### `async close(): Promise<void>`
파일 핸들을 닫고 안전하게 종료합니다.

### Transaction 클래스

#### `async commit(): Promise<void>`
트랜잭션 중 발생한 모든 변경 사항을 영구적으로 디스크에 반영하고 락을 해제합니다.

#### `async rollback(): Promise<void>`
트랜잭션 중 발생한 모든 변경 사항을 취소하고 원래 상태로 되돌립니다.

## 작동 방식

Shard는 내부적으로 데이터를 **고정 크기 페이지** 단위로 관리합니다. 가상 파일 시스템(VFS) 계층은 자주 액세스하는 페이지를 메모리에 캐싱하여 디스크 I/O를 최소화하며, 모든 변경 사항은 먼저 WAL에 기록된 후 디스크에 동기화되므로 예기치 못한 종료 시에도 안전하게 복구됩니다.

## 라이선스

MIT
