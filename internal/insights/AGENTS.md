# internal/insights/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package row:
[../README.md](../README.md).

- This package stores and aggregates finished-request records. Pipeline
  stages stamp `carrier.RejectCode`. The relay stamps
  `Result.ErrorCode`. Do not reclassify failures here.
- `Record` never blocks the request path and never fails it. A full
  queue drops the incoming record and counts the drop.
- A failed batch write is counted. Do not retry it.
- Drain the queue in `Close` before the process exits.
- Keep the PostgreSQL pool at `poolMaxConns`. Identity, quota
  snapshots, and insights use separate pools.
- `GET /admin/insights` reads stored rows. Aggregation does not write.
