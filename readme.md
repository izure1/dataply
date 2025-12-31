# Shard

Shard는 데이터를 단일 파일에 삽입하고, 빠른 로딩을 위한 데이터 관리 시스템입니다.

## 설치

```bash
npm install shard
```

## 사용법

```ts
import { Shard } from 'shard'

const shard = Shard.Open('./data.db', {
  pageSize: Math.pow(2, 12),
})

const key = 188231123
console.log(await shard.get(key))
```

## API

### Shard.Open(file: string, options?: ShardOptions): Shard

### get(key: number): Promise<string>

### set(value: string): Promise<number>

### delete(key: number): Promise<boolean>

### has(key: number): Promise<boolean>

### close(): Promise<void>

## 작동 방식



## 라이센스

MIT
