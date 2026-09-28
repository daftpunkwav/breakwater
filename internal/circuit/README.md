# circuit/

> Language: **English** | [简体中文](README.zh.md)

The hand-written per-upstream circuit breaker: a three-state machine
(closed → open → half-open) that stops traffic toward a persistently
failing upstream and probes it back to health with exactly one request.
The package owns the machine, its port and the active-probe helper —
nothing else. What counts as a failure is the caller's policy, relayed
through `Outcome`; candidate pre-filtering belongs to
[`internal/router`](../router/); per-attempt granting and reporting
belong to the relay's executor ([exchange.go](../relay/exchange.go));
probe scheduling belongs to the assembly
([recovery.go](../../cmd/breakwater/recovery.go)). Deployments with
`BREAKWATER_CIRCUIT_ENABLED=false` compose the nop instead, so call
sites never branch on nil.

## Files

| File | Role |
| --- | --- |
| `circuit.go` | Contracts: `State` (`StateClosed` / `StateOpen` / `StateHalfOpen`), `Outcome` (Success / ClientFault / ServerFault / GatewayTerminated), `Permission`, the `Breaker` port (`Allow` / `StateOf` / `Reset`) |
| `breaker.go` | `Registry`: the process-local machine per upstream id — cooldown → single half-open probe, outcome accounting, operator `Reset`, transition observer |
| `nop.go` | `NopBreaker`: grants everything, forgets every outcome; holds no state |
| `prober.go` | `ActiveProbe`: runs one synthetic health check through the breaker's Allow/Report protocol, so recovery does not wait for real traffic to become the probe |

## Tests

`breaker_absorption_test.go` covers late, duplicate and abandoned
reports; `breaker_defaults_test.go` the zero-config substitutions;
`breaker_reset_test.go` the operator reset path; `nop_test.go` and
`prober_test.go` their own files.

## Invariants

- Open denies every call without touching the upstream; half-open admits
  exactly one outstanding probe — concurrent arrivals are denied, never
  queued.
- A granted call reports its outcome exactly once; double reports and
  abandoned permissions are absorbed. A probe whose holder never reports
  is reclaimed as a server fault by the next `Allow`, `StateOf` or
  `Report` after the probe deadline — the deadline is the only recovery
  signal; nothing detects panics or cancellations directly.
- `OutcomeGatewayTerminated` is no health evidence: closed keeps the
  failure counter as it is, and a truncated half-open probe returns to
  open with a fresh cooldown — a probe that proves nothing must not
  close the breaker.
- `StateOf` performs lazy transitions (open cooldown elapsed →
  half-open) but never allocates the probe slot; probing is `Allow`'s
  exclusive business, so a router read never consumes the half-open
  chance.
- State is process-local by design; the port stays replaceable for a
  shared backend.

Hand-written by discipline: the `no-off-the-shelf-governance` rule in
[.golangci.yml](../../.golangci.yml) denies `github.com/sony/gobreaker`.
Config: `BREAKWATER_CIRCUIT_ENABLED` / `_FAIL_THRESHOLD` / `_COOLDOWN` /
`_PROBE_TIMEOUT` (defaults on / 5 / 30s / 5s). Operator surface:
`GET /admin/breakers` and `POST /admin/breakers/{id}/reset`.
