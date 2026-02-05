window.BENCHMARK_DATA = {
  "lastUpdate": 1770251348493,
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
      }
    ]
  }
}