# .github/ agent rules

Workflow: [workflows/ci.yml](workflows/ci.yml).

## Gate

`verify` is blocking. Order: `gofmt` check, `go vet`, `govulncheck`,
`golangci-lint`, `go test -race -cover`, the per-package 95% coverage
floor parsed from that test log, then both image builds.

- Parse the coverage floor from the race run that sets
  `BREAKWATER_TEST_REDIS_ADDR` and `BREAKWATER_TEST_POSTGRES_DSN`.
- Keep `pipefail` on the test step so a failing `go test` fails the job.
- Build `deploy/breakwater.Dockerfile` and `deploy/mockllm.Dockerfile`
  in this workflow. `push` stays false.

## Pins

A pin change is its own commit. Current pins:

- Runner: `ubuntu-24.04`
- `govulncheck`: `v1.7.0`
- `golangci-lint`: `v2.13`
- Go: the version in `go.mod`, via `go-version-file`
