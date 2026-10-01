# cmd/ agent rules

Binary map: [README.md](README.md).

## Composition

- Wiring decisions live in `cmd/*`. Each root selects the store, the
  backend, and the options. `internal/*` packages stay capability leaves.
- Both binaries serve through `internal/httpserver.Run`.

## Binaries

- [`breakwater/AGENTS.md`](breakwater/AGENTS.md) — gateway composition root.
- [`mockllm/AGENTS.md`](mockllm/AGENTS.md) — CLI wrapper over
  `internal/mockllm`.

## Checks

- Format with `gofmt`. `go vet ./...` and `golangci-lint run ./...` pass.
- `go test -race -count=1 -timeout 300s ./...` is the CI bar.
- Statement coverage is at least 95% per package on the CI run, which
  provides Redis and PostgreSQL. The taxonomy is
  [../docs/TESTING.md](../docs/TESTING.md).
