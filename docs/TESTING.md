# Testing

> Language: **English** | [简体中文](TESTING.zh.md)

Every package ships with its tests colocated (`foo_test.go` next to
`foo.go`, same or `_test` package) — Go's convention, kept deliberately:
no separate test tree, no parallel hierarchy to drift.

## Test taxonomy

| Kind | Where | What |
| ---- | ----- | ---- |
| Unit | `internal/<pkg>/*_test.go`, same package | One mechanism, one file: state machines, Lua-backed ledger semantics (via miniredis), token buckets, limit merge layers, transcoders, codecs, helpers. White-box where branches demand it. |
| Functional | `internal/server/chain_test.go`, `inference_formats_test.go`, `completions_test.go`, `model_authz_cache_test.go` | Full pipeline scenarios through the real routes: the eight acceptance scenarios (key rejection, rate limiting, zero-settlement on failure, quota exhaustion, streaming through governance, tier denial, client-disconnect settlement and request-id echo), the three client formats, honest stream termination, cache refund accounting, the tier-authorization/cache interplay. |
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

Every mechanism below states the property it guarantees, the code that
owns it, and a test that fails when the property breaks. The test names
are the index — run any of them to check the property directly.

| Property | Owner | Guarding test |
| -------- | ----- | ------------- |
| A rejected request never reaches an upstream | `internal/server`, `internal/pipeline` | `TestChainRejectsMissingAndUnknownKeys`, `TestChainRateLimitsWithRetryAfter`, `TestChainQuotaExhaustionIsPaymentRequired`, `TestChainDeniesModelOutsideTier` |
| Concurrent cold requests for one cache key cause exactly one upstream fetch | `internal/cache` | `TestFlightSingleFetchUnderStampede`, `TestCacheMiddlewareConcurrentColdStartsFetchOnce` |
| A starter that walks away mid-fetch neither cancels the shared fetch nor fails the waiters riding it | `internal/cache` | `TestCacheSharedFetchSurvivesTheStarterLeaving`, `TestCacheMiddlewareWaiterSurvivesOwnerDisconnect` |
| A panicking fetch releases its waiters instead of wedging them | `internal/cache` | `TestFlightPanicReleasesWaiters` |
| A request never exceeds its attempt cap, and retries never exceed the global in-flight budget | `internal/retry` | `TestExecuteCapsAttempts`, `TestBudgetCapsInFlightRetries`, `TestBudgetReleaseAbsorbsImbalance` |
| After the first response byte, the loop never retries | `internal/retry`, `internal/relay` | `TestExecuteNeverClassifiesCommittedErrors`, `TestMessagesRouteStreamAbortTerminatesHonestly` |
| An open breaker denies every call; half-open admits exactly one probe and reclaims abandoned ones | `internal/circuit` | `TestBreakerConcurrentProbesExactlyOne`, `TestBreakerReclaimsAbandonedProbe` |
| The ratio guard denies a rising share of calls but never cuts traffic entirely — one call per forced-pass interval still gets through | `internal/circuit` | `TestRatioDenialTracksFailureShare`, `TestRatioForcePassAdmitsOnePerInterval` |
| The slow-call breaker opens only on a sampled window reaching the slow share (a thin window never opens, faults count as the strongest slow evidence), and a healthy probe closes it on an emptied window | `internal/circuit` | `TestSlowWindowNeedsSamples`, `TestSlowShareOpensTheBreaker`, `TestSlowProbeClosesOnAnEmptiedWindow` |
| A healthy attempt past the slow threshold reports as slow — Success-grade evidence every strategy absorbs; streams measure it by time to first byte | `internal/circuit`, `internal/relay` | `TestConsecutiveAbsorbsSlowAsHealth`, `TestSlowBufferedAttemptReportsSlow`, `TestSlowStreamTTFTReportsSlow` |
| A hard-down upstream's recovery probes space out on a doubling ladder; a healthy answer forgets the ladder | `cmd/breakwater` | `TestProbeBackoffLadder`, `TestRecoveryBackoffZeroKeepsFixedPace` |
| A reconcile round cannot outlive its own cadence; a wedged store aborts the round at the bound | `internal/quota` | `TestStartReconcilerBoundsAWedgedRound` |
| A broken stream keeps its delivered bytes and ends with one error event plus `[DONE]` | `internal/relay`, `internal/protocol` | `TestWriteAbortContract`, `TestWriteAbortAllCodes`, `TestMessagesRouteStreamAbortTerminatesHonestly` |
| The balance never over-drafts; debits minus refunds reconcile against the balance | `internal/quota` | `TestMemoryConcurrentDrainReconciles`, `TestRedisConcurrentDrainReconciles` |
| Every reservation has a lease record; abandoned leases are reclaimed | `internal/quota` | `TestStartSweeperReclaimsUntilCancelled` |
| A client gone mid-stream still settles by the tokens it consumed | `internal/quota`, `internal/server` | `TestChainClientDisconnectCancelsUpstreamAndSettlesByUsage` |
| A full observation buffer drops and counts rather than blocking handlers | `internal/obs`, `internal/insights` | `TestLoggerDropsOldestUnderPressure`, `TestRecordQueuesAndCountsDrops` |
| A failed batch write is counted, not retried silently | `internal/insights` | `TestCopyIntoDropsOnFailure` |
| The observation queue drains before the process exits | `cmd/breakwater`, `internal/obs`, `internal/insights` | `TestLoggerCloseDrains`, `TestWriteLoopFlushesOnClose` |

## Coverage

The bar is ≥95% statement coverage per package, and the CI coverage job
enforces it. That job provisions Redis and PostgreSQL, so the
database-gated integration tests run there — which is the environment the
bar is written for. A local `go test -cover ./...` without those two
services reads lower, because whole SQL paths sit behind
`BREAKWATER_TEST_POSTGRES_DSN` and `BREAKWATER_TEST_REDIS_ADDR`:

| Package | Local, no databases | With both services |
| ------- | ------------------- | ------------------ |
| `internal/auth` | 90.7% | ~96% |
| `internal/insights` | 87.3% | ~98% |
| `cmd/breakwater` | 94.0% | ~96% |

Every other package reads ≥95% locally, without any service. What
remains uncovered in `cmd/breakwater` even with both databases is a set
of defensive error returns that no valid configuration can reach: the
upstream adapter and the router are already built from validated
configuration, so their error branches exist for a future where they are
constructed some other way. Those are disclosed rather than faked with
tests that would pass regardless.

Verify the local figures with:

    go test -cover ./...
