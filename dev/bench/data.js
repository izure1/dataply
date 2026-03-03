window.BENCHMARK_DATA = {
  "lastUpdate": 1772551621572,
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
          "id": "45bdcf0775158252d34b27e33ffdc5e2de0d9b2b",
          "message": "feat: 성능 최적화를 위해 내부 rid를 찾는 b+tree는 이제 pageCacheCapacity 옵션의 영향을 받지 않으며, 대신 가용 메모리의 5~10% 정도를 사용하도록 자동 계산됩니다.",
          "timestamp": "2026-02-07T10:09:12+09:00",
          "tree_id": "05a19a9c0522dec380ce0c3fb7068edb5cb9cb84",
          "url": "https://github.com/izure1/dataply/commit/45bdcf0775158252d34b27e33ffdc5e2de0d9b2b"
        },
        "date": 1770426583461,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 354.65,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 71.38,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 261.51,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 92.42,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 119.42,
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
          "id": "1587a33a497131a8a3f18296dedc327bf7e007fd",
          "message": "feat: primary key b+tree 인덱스의 최대치가 여유 메모리의 5% -> 10%로 상향 조정되었습니다.",
          "timestamp": "2026-02-07T11:25:03+09:00",
          "tree_id": "921626ccbabbb3c948fb98979cebc62f4eb01e03",
          "url": "https://github.com/izure1/dataply/commit/1587a33a497131a8a3f18296dedc327bf7e007fd"
        },
        "date": 1770431171515,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 344.03,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 73.99,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 208.06,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 88.11,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 120.72,
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
          "id": "c90074cab8e45edbcbc13ed5b16c59bbf0946e27",
          "message": "performance: 조회 성능 최적화를 위해 fetchRowsByRids 메서드 추가 및 serializable-bptree 종속성 라이브러리 업데이트",
          "timestamp": "2026-02-08T04:20:57+09:00",
          "tree_id": "c36c1535346370d461eece05aa49d1167c4b792d",
          "url": "https://github.com/izure1/dataply/commit/c90074cab8e45edbcbc13ed5b16c59bbf0946e27"
        },
        "date": 1770492105611,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 338.87,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 72.04,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 215.58,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 88.33,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 106.12,
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
          "id": "6d36839ea35f8d4912ca488a8b20936c9e3f4f37",
          "message": "performance: 조회 성능 향상을 위한 개선 사항입니다",
          "timestamp": "2026-02-08T11:12:55+09:00",
          "tree_id": "6f42172b758d506ff99f23487fdb53536d61aa3d",
          "url": "https://github.com/izure1/dataply/commit/6d36839ea35f8d4912ca488a8b20936c9e3f4f37"
        },
        "date": 1770516803200,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 337.01,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 72.52,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 209.22,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 83.65,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 105.41,
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
          "id": "e2746a23bce37ded7cb30a1d24a481cc9fdcfd2b",
          "message": "chore: 버전 업데이트",
          "timestamp": "2026-02-11T04:13:47+09:00",
          "tree_id": "72ad97493dacde7b45a38a63c4b2738da4f31c10",
          "url": "https://github.com/izure1/dataply/commit/e2746a23bce37ded7cb30a1d24a481cc9fdcfd2b"
        },
        "date": 1770750864275,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 355.61,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 75.11,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 207.36,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 96.07,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 111.78,
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
          "id": "633125d0dd45f8b78af5b985be4e7e9ced1f8d1f",
          "message": "chore: mvcc-api, serializable-bptree 종속성 라이브러리 버전 업데이트",
          "timestamp": "2026-02-12T21:24:05+09:00",
          "tree_id": "45b318fb198ef21066eeac63dc37684e05b4a52f",
          "url": "https://github.com/izure1/dataply/commit/633125d0dd45f8b78af5b985be4e7e9ced1f8d1f"
        },
        "date": 1770899076722,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 540.95,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 67.53,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 167.29,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 87.86,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 115.24,
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
          "id": "62e8e1d310d3e3555869d0688179fe0f74a3637a",
          "message": "chore: 버전 오탈자 수정",
          "timestamp": "2026-02-12T21:25:07+09:00",
          "tree_id": "653a220b3f5c39a104b09d80c54e923d7ffc843f",
          "url": "https://github.com/izure1/dataply/commit/62e8e1d310d3e3555869d0688179fe0f74a3637a"
        },
        "date": 1770899141113,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 535.51,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 68.76,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 198.85,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 81.04,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 113.19,
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
          "id": "6a185e48b478ac6890e1fed73d528ae1fc154d79",
          "message": "chore: 성능 개선을 위한 종속성 라이브러리 업데이트",
          "timestamp": "2026-02-15T23:22:42+09:00",
          "tree_id": "3ce418ec40cda9e2bda632accea8a85cb6a6368e",
          "url": "https://github.com/izure1/dataply/commit/6a185e48b478ac6890e1fed73d528ae1fc154d79"
        },
        "date": 1771165409311,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 1556.98,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 54.34,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 142.28,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 65.24,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 116.04,
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
          "id": "aae78d51c1f3559121c5fe723cf563a572a2905c",
          "message": "chore: 성능 개선을 위한 종속성 라이브러리 업데이트",
          "timestamp": "2026-02-18T05:19:16+09:00",
          "tree_id": "b12eb31c248f203465ee8a209f02024b167c734f",
          "url": "https://github.com/izure1/dataply/commit/aae78d51c1f3559121c5fe723cf563a572a2905c"
        },
        "date": 1771359614957,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 1199.83,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 52.99,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 150.19,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 63.36,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 118.87,
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
          "id": "63989903ad0774c38b0f2b61e48e3c326fc8e002",
          "message": "fix: serializable-bptree 종속성 라이브러리 오류로 인한 버전 업데이트",
          "timestamp": "2026-02-18T09:06:39+09:00",
          "tree_id": "1f0d1ae72240d29899ecc64c4f410b8ce583095a",
          "url": "https://github.com/izure1/dataply/commit/63989903ad0774c38b0f2b61e48e3c326fc8e002"
        },
        "date": 1771373244213,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 1230.5,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 55.04,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 176.2,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 65.14,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 114.03,
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
          "id": "b6d06c0ed0d64539241844c3dc7d37ce84dc80db",
          "message": "chore: 성능 개선을 위한 종속성 라이브러리 버전 업데이트",
          "timestamp": "2026-02-20T01:01:22+09:00",
          "tree_id": "ba79463ad16882120560e3760af20c473c5675f8",
          "url": "https://github.com/izure1/dataply/commit/b6d06c0ed0d64539241844c3dc7d37ce84dc80db"
        },
        "date": 1771516930389,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 1221.26,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 61.07,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 171.34,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 55.01,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 114.13,
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
          "id": "d4f60b257c566f9aae96194a508f1691ae33ee83",
          "message": "chore: 버전 업데이트",
          "timestamp": "2026-02-20T01:56:48+09:00",
          "tree_id": "aabb0347ee7c1e49e3ca2e983a9d577954a6a47c",
          "url": "https://github.com/izure1/dataply/commit/d4f60b257c566f9aae96194a508f1691ae33ee83"
        },
        "date": 1771520253741,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 1281.79,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 52.68,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 204.78,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 57.1,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 115.6,
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
          "id": "a37351821b234dab1b8a30b9fe2b733078a833e0",
          "message": "feat: serializable-bptree 종속성 라이브러리 업데이트로 insertBatch 성능 최적화",
          "timestamp": "2026-02-26T09:55:51+09:00",
          "tree_id": "763ffd8b502ef85927d0c57a842e9e5bde3d2b61",
          "url": "https://github.com/izure1/dataply/commit/a37351821b234dab1b8a30b9fe2b733078a833e0"
        },
        "date": 1772067395341,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 962.36,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 44.52,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 144.61,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 54.57,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 117.83,
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
          "id": "3d31b510586110e134cfea2be9285b154ef99c09",
          "message": "performance: serializable-bptree 종속성 라이브러리 버전 업데이트로 성능을 개선합니다",
          "timestamp": "2026-02-26T10:58:51+09:00",
          "tree_id": "5582763b15dd5f548cecacf53658dc767df7196d",
          "url": "https://github.com/izure1/dataply/commit/3d31b510586110e134cfea2be9285b154ef99c09"
        },
        "date": 1772071159188,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 329.61,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 45.18,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 151.95,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 60.37,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 114.88,
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
          "id": "7be00d2f4c6a19e03f8d6c6e73fe275df1a9b20a",
          "message": "chore: 버전 업데이트",
          "timestamp": "2026-02-28T08:52:33+09:00",
          "tree_id": "6285c302ab8fc5a9f1ebe7fbd2e090ad36a0d565",
          "url": "https://github.com/izure1/dataply/commit/7be00d2f4c6a19e03f8d6c6e73fe275df1a9b20a"
        },
        "date": 1772236464094,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 353.44,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 42.28,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 162.61,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 64.35,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 113.97,
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
          "id": "5a75c6e8da550f24b0f656821d841264c8cd418b",
          "message": "performance: 배열 최소최대값 반환 함수 추가로 성능을 개선합니다",
          "timestamp": "2026-03-01T07:19:06+09:00",
          "tree_id": "f648960c0e49ffddf9f2d4be896aef1cce95677a",
          "url": "https://github.com/izure1/dataply/commit/5a75c6e8da550f24b0f656821d841264c8cd418b"
        },
        "date": 1772317187952,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 354.28,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 53.89,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 175.9,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 61.37,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 116.31,
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
          "id": "3901361383de5e1d0aa6c0b7efb918737918abb7",
          "message": "chore: 종속성 라이브러리 업데이트",
          "timestamp": "2026-03-02T06:55:19+09:00",
          "tree_id": "3b6d2998c23c217d27c8e47a47a7b7256f9197ac",
          "url": "https://github.com/izure1/dataply/commit/3901361383de5e1d0aa6c0b7efb918737918abb7"
        },
        "date": 1772402152884,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 328.36,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 49.06,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 164.4,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 60.47,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 110.31,
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
          "id": "e9e70ccbc524198763dd58868aae969be87da874",
          "message": "feat: hdd 환경에서 순차 읽기를 지원하기 위한 pagePreallocationCount 옵션을 추가합니다. 이는 페이지를 생성할 때, 한번에 많은 페이지를 할당하여 순차읽기를 도와줍니다.",
          "timestamp": "2026-03-02T19:16:32+09:00",
          "tree_id": "c774c02c0745d3e87efc4c001faf1b896a2355cc",
          "url": "https://github.com/izure1/dataply/commit/e9e70ccbc524198763dd58868aae969be87da874"
        },
        "date": 1772446622108,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 415.07,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 45.44,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 150.97,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 180.09,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 118.59,
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
          "id": "6373a72f79d03c23da1362ed2bf01d33899e1d77",
          "message": "fix: setFreePage 메서드에서 순차 적용 시 데드락이 발생할 수 있었던 오류를 수정합니다",
          "timestamp": "2026-03-02T20:03:19+09:00",
          "tree_id": "f2e137117f1c6cd6869950d7c7e7fb16d4e3a7f8",
          "url": "https://github.com/izure1/dataply/commit/6373a72f79d03c23da1362ed2bf01d33899e1d77"
        },
        "date": 1772449518012,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 419.75,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 46.8,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 294.68,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 192.36,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 110.82,
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
          "id": "b616bbad75a3a860079247867229c02b4e6e5042",
          "message": "fix: 데이터베이스의 크기가 커졌을 경우, 비트맵 확장이 필요할 경우 메타데이터가 업데이트되지 않아 무한재귀가 발생하던 오류를 수정합니다",
          "timestamp": "2026-03-03T01:30:32+09:00",
          "tree_id": "b3af8f513ee8706d38e2a493a6aefa9100ff38b8",
          "url": "https://github.com/izure1/dataply/commit/b616bbad75a3a860079247867229c02b4e6e5042"
        },
        "date": 1772471494155,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 416.64,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 50.41,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 182.9,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 191.85,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 116.54,
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
          "id": "d6f4471f0e3d7993381b483b6e2ae2deb96296a0",
          "message": "fix: insert, insertBatch, delete 등 메서드 실행 도중, transaction.commit을 병렬 호출할 경우, 직렬화를 무시한채 동작되므로 메타데이터가 망가지는 오류를 수정합니다.",
          "timestamp": "2026-03-03T20:03:10+09:00",
          "tree_id": "b53ca0ec87d84f4f80089b610827ff1961f6a39e",
          "url": "https://github.com/izure1/dataply/commit/d6f4471f0e3d7993381b483b6e2ae2deb96296a0"
        },
        "date": 1772535920085,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 410.22,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 47.12,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 175.4,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 191.33,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 107.24,
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
          "id": "cf9fe86c84f8f8485b3d297dc061bf1483f14517",
          "message": "fix: 병렬 작업 시, 데드락이 발생하던 오류를 수정합니다.",
          "timestamp": "2026-03-03T21:56:07+09:00",
          "tree_id": "2f8ff611026b300965f304e5059fabb67d944f9b",
          "url": "https://github.com/izure1/dataply/commit/cf9fe86c84f8f8485b3d297dc061bf1483f14517"
        },
        "date": 1772542605746,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 410.25,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 54.27,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 272.88,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 195.01,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 112.68,
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
          "id": "31554999212f2d1cb5e9059286fce5f757fe045c",
          "message": "feat: 불필요하므로 더이상 비트맵은 지원되지 않습니다. 다만 free page list 는 여전히 지원되며, 페이지 재할당 기능은 그대로 유지됩니다. 비트맵 업데이트가 삭제되므로 데이터베이스 안정성과, 성능이 향상됩니다.",
          "timestamp": "2026-03-03T23:21:03+09:00",
          "tree_id": "6c97e88e5e858ad33ca2b10e327982a043d04e6c",
          "url": "https://github.com/izure1/dataply/commit/31554999212f2d1cb5e9059286fce5f757fe045c"
        },
        "date": 1772547729911,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 380.54,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 48.64,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 187.47,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 134.04,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 113.36,
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
          "id": "cfc80fa5e7b4ffcef6c9aa6ac1c84e02fdb91d62",
          "message": "fix: 데이터베이스 파일 크기가 512MB가 넘을 경우 발생할 수 있던 오류를 수정합니다",
          "timestamp": "2026-03-04T00:25:14+09:00",
          "tree_id": "d296319b163bcb825dee800840cec3657b2a108c",
          "url": "https://github.com/izure1/dataply/commit/cfc80fa5e7b4ffcef6c9aa6ac1c84e02fdb91d62"
        },
        "date": 1772551621282,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (Batch)",
            "value": 376.04,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert (Individual)",
            "value": 48.46,
            "unit": "ms"
          },
          {
            "name": "Bulk Insert with WAL",
            "value": 182.72,
            "unit": "ms"
          },
          {
            "name": "Medium Row Insert (1KB)",
            "value": 135.12,
            "unit": "ms"
          },
          {
            "name": "selectMany (500 PKs)",
            "value": 113.73,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}