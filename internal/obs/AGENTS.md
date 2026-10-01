# internal/obs/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- This package records. `cmd/breakwater` chooses sinks and wires the
  fan-out. Aggregation for `/admin/insights` lives in
  `internal/insights`.
- `Record` does not block the request path. Overflow drops the oldest
  entry and counts the drop. A malformed entry or a failed write counts
  as a drop.
- `Flush` waits until the drain goroutine is idle, not only until the
  queue length is zero.
- `Close` drains before returning.
- Label names are `tenant`, `model`, `upstream`, `status`, plus `from`
  and `to` for failovers and `result` and `reason` for probes. Do not
  add request ids or paths as labels.
- `maxChildren` is 4096 label-set leaves per family. Further sets
  collapse into the reserved overflow leaf with `*` label values.
- Do not import `client_golang`. Render Prometheus text format 0.0.4
  from the hand-written registry. Store floats as bits in
  `atomic.Uint64`. Escape label values for backslash, quote, and
  newline only.
- Every `Metrics` recorder method is safe on a nil receiver.
- Pre-create the label-less child of `breakwater_logs_dropped_total`
  and `breakwater_insights_records_dropped_total` so a first scrape
  shows zero.
- A request that ends without a response uses status
  `StatusClientClosedRequest` (499).
