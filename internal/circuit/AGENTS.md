# internal/circuit/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- All strategies implement one port. `NopBreaker` is the breaker-less
  implementation. Do not import `github.com/sony/gobreaker`.
- Open denies every call and does not contact the upstream. Half-open
  admits one outstanding probe. Concurrent arrivals during that probe
  are denied.
- A granted call reports exactly one outcome. Absorb a double report
  and an abandoned permission.
- Reclaim a probe whose holder never reports as a server fault on the
  next `Allow`, `StateOf`, or `Report` after the probe deadline.
- `OutcomeGatewayTerminated` is not health evidence. Closed leaves the
  failure counter unchanged. A half-open probe with that outcome
  returns to open with a fresh cooldown.
- `StateOf` may move open to half-open when the cooldown has elapsed.
  It does not take the probe slot. Only `Allow` does.
- Breaker state is process-local.
- The caller classifies slowness and reports `OutcomeSlow`. This
  package does not measure duration.
- Consecutive and ratio strategies count `OutcomeSlow` as success.
- Slow-call, while closed: `OutcomeSlow` and `OutcomeServerFault`
  increment the slow count. A fast success increments only the total.
  `OutcomeGatewayTerminated` increments nothing. Open when the window
  holds at least `slowMinSamples` (10) and the slow share reaches
  `SlowRatio`.
- Slow-call, while half-open: `OutcomeGatewayTerminated` reclaims the
  probe to open. A non-fault report past the probe deadline does the
  same. `OutcomeServerFault` returns to open. Every other outcome,
  including `OutcomeSlow`, `OutcomeClientFault`, and a fast success,
  closes the machine and empties the window.
- Ratio strategy: a denial is an event in the window. Admit one call
  per `ratioForcePass` (1s). A window with no recent events admits
  every call. Publish denials on `breakwater_circuit_denied_total`.
  Leave the state gauge closed.
