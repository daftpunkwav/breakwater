# internal/ agent rules

Package map: [README.md](README.md). Test taxonomy:
[../docs/TESTING.md](../docs/TESTING.md).

## Layout

- Add files inside an existing package. Do not add a top-level
  directory. Do not add a subpackage under a governance package.
- A governance package keeps its implementation, Lua scripts, sweeper,
  and pipeline middleware inside that package.
- Provider adapters stay flat in `upstream/`. One OpenAI-compatible
  adapter, selected by `base_url`, serves every OpenAI-compatible
  provider.
- HTTP endpoint shapes belong to `server/`. Wire-format shapes belong
  to `protocol/`.
- `httpserver/` imports only the standard library.
- `config/` imports no other project package.
- Store, backend, and option selection belongs to `cmd/*`.

## Inference chain

Outermost stages: carrier, request id, format, observation. Inside
observation the spend order is:

auth → model authorization → concurrency → limiter → quota → cache →
route → forward.

Cache is omitted when caching is disabled. Route and forward are the
inference handler, not extra middleware. `RecoveryStage` is the
innermost stage, still inside observation.

Retry, circuit breaking, and failover run inside the forward stage
(`relay/`), once per attempt. Settlement, cache write, and the
observation record run once per client request.

## Dependencies

`no-off-the-shelf-governance` in [.golangci.yml](../.golangci.yml) denies:

- `github.com/sony/gobreaker`
- `golang.org/x/time/rate`
- `golang.org/x/sync/singleflight`
- `github.com/go-redis/redis_rate`

`mockllm-stays-test-harness` denies
`github.com/daftpunkwav/breakwater/internal/mockllm` outside
`cmd/mockllm` and `internal/mockllm`.

## Tests

- Colocate tests (`foo_test.go` next to `foo.go`). Do not add a separate
  test tree.
- One test file covers one subject. Name the file for that subject.
  Shared fixtures get their own file (`testupstream_test.go`,
  `teststore_test.go`).
- Assert status codes, bodies, balances, and metrics.
- `*_integration_test.go` skips unless `BREAKWATER_TEST_POSTGRES_DSN`
  or `BREAKWATER_TEST_REDIS_ADDR` is set, matching the dependency that
  test needs.
- Benchmarks live in `*_bench_test.go`.
- CI requires `gofmt`, `go vet`, `golangci-lint`, and `go test -race`.
  Statement coverage is at least 95% per package on that run.
- Do not weaken, skip, or xfail a test to let a change pass.

## Docs

- A behavior change that a package README states updates `README.md`
  and `README.zh.md` in the same change.

## Package rules

- [affinity/AGENTS.md](affinity/AGENTS.md)
- [auth/AGENTS.md](auth/AGENTS.md)
- [cache/AGENTS.md](cache/AGENTS.md)
- [circuit/AGENTS.md](circuit/AGENTS.md)
- [config/AGENTS.md](config/AGENTS.md)
- [httpserver/AGENTS.md](httpserver/AGENTS.md)
- [insights/AGENTS.md](insights/AGENTS.md)
- [limiter/AGENTS.md](limiter/AGENTS.md)
- [mockllm/AGENTS.md](mockllm/AGENTS.md)
- [obs/AGENTS.md](obs/AGENTS.md)
- [pipeline/AGENTS.md](pipeline/AGENTS.md)
- [protocol/AGENTS.md](protocol/AGENTS.md)
- [quota/AGENTS.md](quota/AGENTS.md)
- [relay/AGENTS.md](relay/AGENTS.md)
- [retry/AGENTS.md](retry/AGENTS.md)
- [router/AGENTS.md](router/AGENTS.md)
- [server/AGENTS.md](server/AGENTS.md)
- [upstream/AGENTS.md](upstream/AGENTS.md)
