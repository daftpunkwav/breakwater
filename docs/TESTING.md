# Testing

Every package ships with its tests colocated (`foo_test.go` next to
`foo.go`, same or `_test` package) — Go's convention, kept deliberately:
no separate test tree, no parallel hierarchy to drift.

## Test taxonomy

| Kind | Where | What |
| ---- | ----- | ---- |
| Unit | `internal/<pkg>/*_test.go`, same package | One mechanism, one file: state machines, Lua-backed ledger semantics (via miniredis), token buckets, limit merge layers, transcoders, codecs, helpers. White-box where branches demand it. |
| Functional | `internal/server/chain_test.go`, `inference_formats_test.go`, `completions_test.go`, `model_authz_cache_test.go` | Full pipeline scenarios through the real routes: the S1–S8 acceptance scenarios, the three client formats, honest stream termination, cache refund accounting, the tier-authorization/cache interplay. |
| Integration | `*_integration_test.go`, env-gated | Real PostgreSQL and Redis (the CI workflow provisions both as service containers). Locally they skip unless `BREAKWATER_TEST_POSTGRES_DSN` / `BREAKWATER_TEST_REDIS_ADDR` are set. |
| Concurrency / race | everywhere, `-race` is the CI default | The invariant evidence: concurrent quota drains reconcile to zero error, stampede fetches run exactly once, breaker probe slots never double-grant, the concurrency gate never exceeds its ceiling, logger drops stay counted. |
| Benchmark | `*_bench_test.go` | SSE pump, singleflight stampede and concurrency-gate locking baselines (`go test -bench`). |

## Naming and cohesion

- One test file covers one cohesive subject; the file name states that
  subject (`stream_termination_test.go`, `load_validation_test.go`).
  Unrelated tests never share a file, however small the file.
- Shared fixtures live in an accurately named file of their own
  (`testupstream_test.go`, `teststore_test.go`).
- Tests assert observable behavior (status codes, bodies, balances,
  metrics), not internal call graphs.

## Invariant map

Every system invariant (TECH-SPEC §4) has at least one named test; the
matrix lives in the spec review notes and is kept in sync by review.

## Coverage

The bar is ≥95% statement coverage per package. Local measurement
splits into two classes:

- Packages whose tests are self-contained sit at 95–100% locally.
- The PostgreSQL-backed stores (identity, insights) keep a share of
  their SQL paths behind the DSN-gated integration tests; locally
  they read 85–90%, with every live-database branch covered when CI's
  coverage gate runs with both databases provisioned. The residual
  uncovered lines are defensive guards (`crypto/rand` failure
  branches, pool teardown) that cannot be triggered on a real host
  and are disclosed rather than faked.

Verify with:

    go test -cover ./...
