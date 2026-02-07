window.BENCHMARK_DATA = {
  "lastUpdate": 1770424927862,
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
          "id": "445c8b4b3ad9b89139f13a4d2ea523fbef9f8774",
          "message": "feat: wal 지연쓰기를 이용하여 성능을 최적화합니다.",
          "timestamp": "2026-02-07T06:51:26+09:00",
          "tree_id": "79a40dfbfdf0e466c0a8ab24ea92b62092900f68",
          "url": "https://github.com/izure1/dataply/commit/445c8b4b3ad9b89139f13a4d2ea523fbef9f8774"
        },
        "date": 1770414745036,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 295.66,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 66.84,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 202.86,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 79.94,
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
          "id": "52a6a63bfc89b2b948cee1be32cf8a50c1ab8d1f",
          "message": "chore: 버전 업데이트",
          "timestamp": "2026-02-07T07:05:55+09:00",
          "tree_id": "d6d7a0aa31f7d9d2b773e8611808b8f8445ac81a",
          "url": "https://github.com/izure1/dataply/commit/52a6a63bfc89b2b948cee1be32cf8a50c1ab8d1f"
        },
        "date": 1770415584140,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 293.63,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 63.69,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 213.76,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 77.54,
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
          "id": "8c64b956661ea480f2a0f7a36893925e4af16b10",
          "message": "feat: selectMany 메서드 추가. 이로 인해 대량의 데이터를 조회할 때, 한 번의 b+tree 순회로 전부 처리할 수 있습니다. O(1)의 성능을 보장합니다.",
          "timestamp": "2026-02-07T09:41:08+09:00",
          "tree_id": "cdc57f64e3d1148cc32985702fc3f8bb7e921a16",
          "url": "https://github.com/izure1/dataply/commit/8c64b956661ea480f2a0f7a36893925e4af16b10"
        },
        "date": 1770424927555,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 365.74,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 71.4,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 172.11,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 87.69,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 117.07,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}