# internal/router/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- `Candidates` drops breaker-open upstreams via `StateOf`. `Allow` may
  still deny a candidate that was listed. Do not treat the pre-filter
  as the grant.
- Binding order is fixed after construction. Switches, the tracker,
  and breaker state are runtime overlays.
- An operator disable is lifted only by an operator. An auto-disable
  is lifted only by the recovery prober in `cmd/breakwater`. An
  operator enable clears both.
- Unknown names read as enabled. Setter calls for an unknown name
  return `ErrUnknownModel` or `ErrUnknownUpstream`.
- `ErrDisabled` is an operator refusal. `ErrUnavailable` means the
  model is bound and every candidate is ineligible. No binding is a
  missing model. Status mapping lives in `internal/server`.
- The tracker orders and does not exclude. An untried upstream scores
  0 and sorts first. A consecutive-failure penalty is 1000ms each and
  clears on the first success.
- Strategy is `static` or `latency`. Latency order uses exchange-latency
  EWMA with alpha 0.25. Near-tied candidates may trade the lead per
  request. Configured order breaks the remaining ties.
- `OnUpstreamEnable` fires when an upstream returns to rotation.
