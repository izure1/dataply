window.BENCHMARK_DATA = {
  "lastUpdate": 1770411160503,
  "repoUrl": "https://github.com/izure1/dataply",
  "entries": {
    "Dataply Storage Benchmark": [
      {
        "commit": {
          "author": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "committer": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "distinct": true,
          "id": "7a46217f2aae5b5f017547af43b9b0c188bbf71a",
          "message": "benchmark: 벤치마크 업데이트",
          "timestamp": "2026-02-05T09:21:36+09:00",
          "tree_id": "06eecfa195edce4c69cdaceb92abfc6121082914",
          "url": "https://github.com/izure1/dataply/commit/7a46217f2aae5b5f017547af43b9b0c188bbf71a"
        },
        "date": 1770251348097,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 303.81,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 83.89,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 82.23,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 95.6,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "committer": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "distinct": true,
          "id": "0253a48fadf26ae5c166686ce755295ed97cab23",
          "message": "chore: 버전 업데이트",
          "timestamp": "2026-02-05T22:23:00+09:00",
          "tree_id": "28da7e02e2c75755b2d144738644759c4928bce1",
          "url": "https://github.com/izure1/dataply/commit/0253a48fadf26ae5c166686ce755295ed97cab23"
        },
        "date": 1770297860516,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 296.96,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 79.93,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 81.04,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 93.91,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "committer": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "distinct": true,
          "id": "1107d885d1204ce865c7af1a8a788a43c73534ef",
          "message": "chore: 메모리 누수 문제로 인한 종속성 라이브러리 및 코드 업데이트",
          "timestamp": "2026-02-07T00:53:40+09:00",
          "tree_id": "dd691c961a5d2d1429064eb148f770b4aea11c7d",
          "url": "https://github.com/izure1/dataply/commit/1107d885d1204ce865c7af1a8a788a43c73534ef"
        },
        "date": 1770393252267,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 297.54,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 79.99,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 80.03,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 91.78,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "committer": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "distinct": true,
          "id": "853c5a102ca16398a09623f799425b035c2e2e64",
          "message": "fix: wal을 지정했음에도 wal 파일에 기록하지 않던 오류를 수정합니다",
          "timestamp": "2026-02-07T04:31:16+09:00",
          "tree_id": "36885df1e64d15d3d0738cb93c2acbd47af0b857",
          "url": "https://github.com/izure1/dataply/commit/853c5a102ca16398a09623f799425b035c2e2e64"
        },
        "date": 1770406305831,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 305.36,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 82.59,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 232.6,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 98.78,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "committer": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "distinct": true,
          "id": "7fe00662b7b252503686f3fc40385e8628bc323a",
          "message": "fix: 커밋 시 체크포인트 없이 즉각 삭제하지 않고, 체크포인트를 도입함으로써 성능 향상",
          "timestamp": "2026-02-07T04:59:25+09:00",
          "tree_id": "e3cbdfbc368c019567aae896236e88f6696dbf15",
          "url": "https://github.com/izure1/dataply/commit/7fe00662b7b252503686f3fc40385e8628bc323a"
        },
        "date": 1770407990540,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 295,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 81.91,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 203.94,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 93.88,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "committer": {
            "email": "izure@naver.com",
            "name": "izure",
            "username": "izure1"
          },
          "distinct": true,
          "id": "c1ce57a95390808cd442042b718174ab861548f8",
          "message": "feat: walCheckpointThreshold 옵션이 이제는 커밋 횟수가 아니라, 총 변경된 페이지 개수를 의미합니다. 따라서 wal 파일의 최대 크기를 좀 더 예측 가능하게 관리할 수 있습니다",
          "timestamp": "2026-02-07T05:50:48+09:00",
          "tree_id": "64161ab10ecfbf7e90e65e271771af4fa7410152",
          "url": "https://github.com/izure1/dataply/commit/c1ce57a95390808cd442042b718174ab861548f8"
        },
        "date": 1770411160219,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 296.22,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 78.93,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 206.26,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 93.72,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}