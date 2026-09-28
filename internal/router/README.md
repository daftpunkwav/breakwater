# router/

> Language: **English** | [简体中文](README.zh.md)

Candidate selection: resolves a model to its ordered upstream instances,
with breaker-open and ineligible upstreams already excluded. The package
owns binding resolution, the two orderings (static configured order,
measured-latency order), the runtime operator switches and the latency
tracker. It does not execute failover — the relay walks the candidate
list as attempts and resolves `BREAKWATER_FALLBACKS` chains itself
([fallback.go](../relay/fallback.go)) — and it never accounts breaker
state, only consults it through the
[`internal/circuit`](../circuit/) port. The inference handler
([inference.go](../server/inference.go)) calls `Candidates` once per
request before the relay starts.

## Files

| File | Role |
| --- | --- |
| `router.go` | The `Router` port: `Candidates(ctx, model)` |
| `priority.go` | `Priority`: bindings in configured order, `"*"` wildcard, breaker pre-filter via `StateOf`, static/latency ordering with the near-tie leader draw |
| `control.go` | `Switch`: operator model/upstream disables, system auto-disables with reason and moment, `View` snapshot; `OnUpstreamEnable` fires when an upstream returns to rotation so the assembly can restore what fatal conditions retired inside it |
| `tracker.go` | `Strategy` and `ParseStrategy`; `Tracker`: per-upstream exchange-latency EWMA (alpha 0.25) plus a 1000 ms penalty per consecutive failure |

## Invariants

- The pre-filter is advisory: `Candidates` drops breaker-open entries via
  `StateOf`, but enforcement and accounting happen at the attempt —
  `Allow` may still deny a candidate that looked eligible.
- Binding order is immutable after construction; switches, tracker and
  breaker state are runtime overlays on top of it.
- The two disable channels stay apart: an operator disable is lifted only
  by an operator; an auto disable (fatal condition — dead credentials,
  exhausted quota) is lifted only by the recovery prober
  ([recovery.go](../../cmd/breakwater/recovery.go)). An operator enable
  clears both — human intent wins.
- Unknown names fail open on reads (enabled) and fail closed on setter
  calls (`ErrUnknownModel` / `ErrUnknownUpstream`), so an operator typo
  can neither toggle silently nor lock traffic out; wildcard deployments
  toggle concrete names on demand.
- The tracker only orders, never excludes: an untried upstream scores 0
  and is explored first, and a fast-but-failing upstream sinks below a
  slower healthy one until its first success clears the penalty entirely.

Error mapping is part of the contract: `ErrDisabled` → 403 (a deliberate
refusal, not a health condition), `ErrUnavailable` (bound but all
ineligible) → 503, no binding at all → 404. Admin surface:
`GET /admin/routing`, `PUT /admin/models/{id}`, `PUT /admin/upstreams/{id}`.
Config: `BREAKWATER_ROUTING_STRATEGY=static|latency`.
