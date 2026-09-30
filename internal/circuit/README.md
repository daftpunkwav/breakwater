# circuit/

> Language: **English** | [简体中文](README.zh.md)

The hand-written per-upstream circuit breaker, in three interchangeable
strategies behind one port: a three-state machine (closed → open →
half-open) that stops traffic toward a persistently failing upstream
and probes it back to health with exactly one request; a ratio guard
that denies a rising share of calls computed from a rolling outcome
window — built for upstreams whose failures are proportional (a
partially saturated backend degrades some requests while others
succeed) and never fully cuts traffic; and a slow-call machine that
runs the consecutive discipline on degradation evidence — the share of
slow completions in the window opens it — for upstreams that keep
answering while falling apart.
The package owns the machines, their port and the active-probe helper —
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
| `breaker.go` | `Registry`: the process-local consecutive-strategy machine per upstream id — cooldown → single half-open probe, outcome accounting, operator `Reset`, transition observer |
| `ratio.go` | `RatioRegistry`: the ratio-strategy guard per upstream id — a 10s rolling window (40 × 250ms buckets) of healthy answers, upstream failures and self-produced denials; `Allow` computes a deny probability from the window (tiny windows protected, trailing failures discount past accepts, healthy buckets dilute the ratio) and one call per forced-pass second is always admitted, so recovery never needs operator action. `StateOf` always reads closed — the guard is a probability, not a position |
| `slow.go` | `SlowRegistry`: the slow-call-strategy machine per upstream id — the consecutive discipline driven by a 10s rolling window of {slow, total} completions; a server fault is the strongest slow evidence, a window below the sample minimum never opens, and a healthy probe closes the breaker on an emptied window. The breaker never judges speed itself: slow completions arrive as `OutcomeSlow`, classified by the relay (time to first byte for streams, full duration otherwise) against the configured threshold |
| `nop.go` | `NopBreaker`: grants everything, forgets every outcome; holds no state |
| `prober.go` | `ActiveProbe`: runs one synthetic health check through the breaker's Allow/Report protocol, so recovery does not wait for real traffic to become the probe |

## Tests

`breaker_absorption_test.go` covers late, duplicate and abandoned
reports; `breaker_defaults_test.go` the zero-config substitutions;
`breaker_reset_test.go` the operator reset path; `ratio_test.go` the
ratio guard's protection floor, deny scaling, dilution, forced pass,
window expiry and reset; `slow_test.go` the slow-call machine's sample
minimum, ratio trigger, fault-as-slow evidence, probe recovery and
reset; `nop_test.go` and `prober_test.go` their own files.

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
- Ratio strategy: a denial is itself an event in the window, which is
  what keeps the deny probability up while the failure persists — the
  forced-pass admission (one per second) is the counterweight that
  keeps recovery observable. A window with no recent events admits
  everything; idle time carries no grudge. Denials surface on the
  `breakwater_circuit_denied_total` metric; the state gauge stays at
  closed because no transition ever fires.
- Slow-call strategy: the window's slow share opens the breaker only
  once the window holds at least ten samples, and a server fault is
  the strongest slow evidence there is (it feeds both counters). A
  healthy probe — slow or fast — closes the machine on an emptied
  window, so recovery starts from clean evidence instead of the
  backlog that opened the breaker. The breaker never judges speed
  itself: slow completions arrive as `OutcomeSlow`, classified by the
  caller.
- `OutcomeSlow` is health evidence for every strategy: the consecutive
  and slow-call machines treat it exactly like a success, and the
  ratio guard counts it into the healthy bucket.

Hand-written by discipline: the `no-off-the-shelf-governance` rule in
[.golangci.yml](../../.golangci.yml) denies `github.com/sony/gobreaker`.
Config: `BREAKWATER_CIRCUIT_ENABLED` / `_STRATEGY` (`consecutive`, the
default, `ratio`, or `slow-call`) / `_FAIL_THRESHOLD` / `_COOLDOWN` /
`_PROBE_TIMEOUT` / `_SLOW_RATIO` / `_SLOW_THRESHOLD` (defaults on /
consecutive / 5 / 30s / 5s / 0.5 / off; the threshold and cooldown
drive consecutive and slow-call, the ratio pair drives slow-call
only). A slow completion is health evidence to every strategy: the
consecutive and ratio machines absorb `OutcomeSlow` exactly like a
success. Operator surface: `GET /admin/breakers` and
`POST /admin/breakers/{id}/reset` (a ratio reset empties the window).
