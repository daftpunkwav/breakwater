# retry/

> Language: **English** | [简体中文](README.zh.md)

The bounded attempt loop: one client request decomposes into upstream
attempts under three caps — `MaxAttempts` in count, `AttemptTimeout` per
attempt, `OverallDeadline` for all attempts together. The package owns
the loop shape, the process-wide in-flight budget (retry storm
containment) and the retryability classifier. It does not select
candidates ([`internal/router`](../router/)), account breaker state
([`internal/circuit`](../circuit/)) or render client-visible errors —
the relay's forward stage owns the client response; the loop only
decides that no attempt remains. The relay's executor
([executor.go](../relay/executor.go)) drives `Execute` once per request
and folds each attempt's outcome into a `circuit.Outcome`.

## Files

| File | Role |
| --- | --- |
| `retry.go` | Contracts: `Policy` (the three caps plus backoff shape), `Classifier`, `Budget` |
| `loop.go` | `Execute`: the attempt loop — budget gating beyond the first attempt, full-jitter exponential backoff, `ErrBudgetExhausted` wrapping the triggering error |
| `classifier.go` | `DefaultClassifier` retryability table; `StatusError`; `ErrCommitted`; `ParseRetryAfter` (integer, fractional or HTTP-date form, capped at 60s); `StripRetryAfter` |

## Tests

`retry_after_test.go` covers hint parsing and preference over computed
backoff; `backoff_policy_test.go` and `backoff_classifier_test.go` the
ceiling math and the classification table; `attempt_deadline_test.go`
and `classifier_transport_test.go` the deadline layering and transport
failure cases.

## Invariants

- The first-byte boundary belongs to the loop, not the classifier: a
  failure after bytes reached the client is `ErrCommitted` and is
  returned untouched — no classification, no further attempt; a retry
  would replay a half-sent reply.
- Every retry beyond the first must acquire the process-wide `Budget`;
  an exhausted budget fails fast with `ErrBudgetExhausted`, wrapping the
  triggering attempt's error. The budget is a concrete type on purpose —
  a single in-process counter with no foreseeable shared backend.
- A `Retry-After` hint replaces the computed backoff only for the next
  attempt against the same upstream; the relay calls `StripRetryAfter`
  before failing over to a different candidate — one upstream's recovery
  schedule says nothing about another's.
- Client cancellation (`context.Canceled`) is never retried; timeouts
  and connection-level transport failures are, so one dead upstream
  becomes a failover instead of a failed request. 429 and 5xx are
  retryable, other 4xx are the client's fault.
- `MaxAttempts < 1` clamps to 1 — zero means one, never "retry forever";
  zero timeouts and deadlines mean absent.

Config: `BREAKWATER_RETRY_MAX_ATTEMPTS` (3), `_ATTEMPT_TIMEOUT` (30s),
`_OVERALL_DEADLINE` (60s), `_BACKOFF_INITIAL` (100ms), `_BACKOFF_MAX`
(2s) and `BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT` (64).
