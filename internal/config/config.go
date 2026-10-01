/**
 * @file config
 * @description Gateway configuration schema with safe defaults.
 *
 * Responsibilities:
 * - Declare every configuration value in one place
 * - Provide defaults so the gateway starts with zero configuration
 *
 * This package is a leaf: it must not import any other project package.
 * Values are loaded from the environment in env.go.
 */
package config

import "time"

// Config is the root configuration of the gateway process.
type Config struct {
	Server    Server
	Redis     Redis
	Postgres  Postgres
	Obs       Obs
	Quota     Quota
	Upstreams []Upstream
	Retry     Retry
	// Identity is the raw JSON identity set (tiers, tenants, keys) for
	// deployments without a database; its schema is owned by the auth
	// package, keeping this package a leaf.
	Identity string
	// Fallbacks maps a client-facing model to the ordered list of
	// fallback models tried when every candidate of the primary model
	// is exhausted. Membership against the configured model names is
	// validated at assembly, not here (this package is a leaf).
	Fallbacks map[string][]string
	// ContextLimits maps a client-facing model to its maximum input
	// token estimate; requests above the limit skip that model's
	// candidates. Zero entries are rejected at load.
	ContextLimits map[string]int64
	// ReconcileInterval paces the quota ledger reconciliation;
	// zero disables the protocol.
	ReconcileInterval time.Duration
	// UpstreamMaxInFlight caps the in-flight exchanges every single
	// upstream serves at once, protecting one saturated provider from
	// collecting the whole gateway's queue as it slows down. An attempt
	// refused by the ceiling fails over to the next candidate; zero
	// (the default) leaves upstream concurrency unbounded.
	UpstreamMaxInFlight int
	Cache               Cache
	Circuit             Circuit
	Security            Security
	// Probe configures the active recovery probing of upstreams.
	Probe Probe
	// Routing selects the candidate ordering policy of the router.
	Routing Routing
}

// Probe configures the active recovery loop: upstreams that were
// taken out of rotation (auto-disabled, or breaker-open) are probed
// on an interval and restored when a probe answers. Interval zero
// disables the loop — recovery then waits for real traffic.
type Probe struct {
	// Interval paces the recovery loop; zero disables it.
	Interval time.Duration
	// Timeout bounds one probe exchange.
	Timeout time.Duration
	// Threshold is how many consecutive healthy probes restore an
	// auto-disabled upstream; one failure resets the count. Values
	// above one keep a flapping upstream from cycling back in.
	Threshold int
	// BackoffMax, when positive, spaces an auto-disabled upstream's
	// probes out after failures: the first failed probe waits one
	// interval, each consecutive failure doubles the wait, capped here.
	// Zero keeps every tick probing.
	BackoffMax time.Duration
}

// Routing configures how the router orders eligible candidates.
type Routing struct {
	// Strategy is "static" (configured order, the default) or "latency"
	// (prefer the lowest measured upstream exchange latency). Health
	// gating (breakers) and operator switches apply in both modes.
	Strategy string
	// AffinityTTL, when positive, enables prompt-prefix affinity: the
	// gateway remembers which upstream served which prompt prefixes
	// and promotes the one holding the longest match to the head of
	// the candidate list, reusing its warm prompt cache. The value is
	// how long a recorded prefix stays fresh; zero (the default)
	// keeps the strategy order untouched.
	AffinityTTL time.Duration
}

// Cache configures the exact-match response cache.
type Cache struct {
	// Enabled turns the cache stage on; false bypasses entirely.
	Enabled bool
	// TTL is the base entry lifetime; the store adds jitter on top.
	TTL time.Duration
	// Capacity caps stored entries.
	Capacity int
}

// Circuit configures the per-upstream breaker. Enabled=false composes
// the nop breaker instead.
type Circuit struct {
	// Enabled composes the real breaker; false is the nop breaker.
	Enabled bool
	// Strategy selects the guard mechanism: "consecutive" (the default)
	// opens after a run of failures and re-admits one probe after the
	// cooldown; "ratio" denies a rising share of calls computed from a
	// rolling window of outcomes and always admits one call per
	// forced-pass interval, so it never fully cuts traffic; "slow-call"
	// runs the consecutive machine on degradation evidence — the share
	// of slow completions in the window instead of error runs — for
	// upstreams that keep answering while falling apart.
	Strategy string
	// FailThreshold drives the consecutive strategy only: the run of
	// server faults that opens the breaker.
	FailThreshold int
	// Cooldown drives the consecutive and slow-call strategies: how
	// long an open breaker waits before admitting one probe.
	Cooldown time.Duration
	// ProbeTimeout bounds a half-open probe of the consecutive
	// and slow-call strategies; an unanswered probe counts as a
	// failure at the deadline.
	ProbeTimeout time.Duration
	// SlowRatio drives the slow-call strategy only: the share of slow
	// completions in the window that opens the breaker (0.5 = half the
	// window).
	SlowRatio float64
	// SlowCallThreshold classifies attempts as slow for the slow-call
	// strategy: an attempt whose responsiveness (first byte for
	// streams, full duration otherwise) exceeds it reports as slow.
	// Zero disables the classification — attempts report fast however
	// long they take.
	SlowCallThreshold time.Duration
}

// Upstream is one OpenAI-compatible provider binding. List order across
// Upstreams doubles as failover priority per model: the first entry
// serving a model is its primary.
type Upstream struct {
	// ID names the upstream for routing and breaker state.
	ID string `json:"id"`
	// BaseURL is the scheme and host, without trailing slash.
	BaseURL string `json:"base_url"`
	// APIKey is the bearer token; empty for the mock upstream. When
	// APIKeys is also set, it leads the credential ring.
	APIKey string `json:"api_key"`
	// APIKeys are further bearer tokens rotated behind the same
	// upstream: provider rate limits are per credential, so the ring
	// spreads the upstream's traffic across all of them. The merged
	// list (api_key first, then api_keys) is the ring order.
	APIKeys []string `json:"api_keys"`
	// ProbeURL is the health endpoint; empty disables probing.
	ProbeURL string `json:"probe_url"`
	// Models are the model identifiers served; "*" is the wildcard. An
	// entry of the form "client=real" serves the client-facing name
	// "client" by forwarding the provider-real name "real" — the
	// gateway routes on the client name, the adapter rewrites the body.
	Models []string `json:"models"`
}

// Credentials merges the upstream's bearer tokens into one ordered
// ring: api_key first, then api_keys. The order fixes each
// credential's index for the ring's lifetime (retirement and the
// access trail reference it), not a serving priority — every alive
// credential serves.
func (u Upstream) Credentials() []string {
	keys := make([]string, 0, len(u.APIKeys)+1)
	if u.APIKey != "" {
		keys = append(keys, u.APIKey)
	}
	return append(keys, u.APIKeys...)
}

// Retry bounds the upstream attempt loop of every request.
type Retry struct {
	// MaxAttempts caps upstream attempts per client request.
	MaxAttempts int
	// AttemptTimeout bounds one upstream attempt.
	AttemptTimeout time.Duration
	// OverallDeadline bounds all attempts of one client request.
	OverallDeadline time.Duration
	// BackoffInitial and BackoffMax shape the exponential backoff.
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	// BudgetMaxInFlight caps concurrent retry attempts process-wide.
	// When BudgetPercent is zero this is the whole cap; with a share
	// budget configured it is unused.
	BudgetMaxInFlight int
	// BudgetPercent, when non-zero, replaces the fixed retry cap with
	// a share budget: retries may occupy at most this percentage of
	// the requests currently in flight, floored at BudgetMinInFlight.
	// Zero keeps the fixed cap.
	BudgetPercent int
	// BudgetMinInFlight is the share budget's floor: the retry cap
	// never drops below it however quiet the gateway is.
	BudgetMinInFlight int
	// StreamTimeout bounds a committed stream's whole body once the
	// reply headers arrived; zero lets the client own the stream's
	// lifetime. It exists because the per-attempt timeout would
	// otherwise kill legitimate long completions mid-stream.
	StreamTimeout time.Duration
	// StreamIdleTimeout bounds upstream silence inside a committed
	// stream: an upstream that stops producing for the whole window
	// loses the stream even while StreamTimeout would still allow it.
	// Zero, the default, disables the watchdog — thinking models can
	// legitimately stay silent for minutes between tokens.
	StreamIdleTimeout time.Duration
}

// Server holds HTTP listener settings.
type Server struct {
	// Addr is the listen address of the gateway.
	Addr string
	// ShutdownGrace bounds how long in-flight requests may finish after a
	// shutdown signal.
	ShutdownGrace time.Duration
}

// Redis holds connection settings for the hot governance state store
// (rate limit buckets, quota hot ledger). The response cache is
// process-local and does not use Redis.
type Redis struct {
	Addr string
	// Namespace prefixes every limiter and quota key. Two deployments
	// sharing one Redis instance would otherwise share tenant balances
	// and rate limit buckets: one environment would spend and refund the
	// other's money. Empty means the bare "bw:" prefix, which is correct
	// only when this gateway owns its Redis instance.
	Namespace string
	// TLS wraps the Redis connection in TLS: balances and rate limit
	// state cross the wire in plaintext otherwise, and a tampered
	// balance is free upstream spend. Off by default because the
	// in-process and localhost deployments Redis is usually paired with
	// need none; the certificate must verify against the system roots
	// with the server name taken from the address host.
	TLS bool
}

// Quota holds lease ledger sizing.
type Quota struct {
	// LeaseTTL is how long a RESERVED lease may live before the sweeper
	// reclaims and fully refunds it. Zero derives the value from the
	// request budget, which is the only safe default: a horizon shorter
	// than the longest request the gateway will run refunds a request
	// that really spent tokens, and the spend never reappears anywhere.
	// The derivation covers the unbounded case too (see
	// unboundedRequestLeaseTTL), so this only needs a value when the
	// operator wants a different horizon.
	LeaseTTL time.Duration
}

// leaseTTLHeadroom is the slack added to the request budget when the
// reclaim horizon is derived. The sweeper runs on an interval, so a
// lease that became reclaimable at exactly the budget would be swept
// while its request is still settling.
const leaseTTLHeadroom = time.Minute

// unboundedRequestLeaseTTL bounds a request that declares no upper
// limit of its own, and bounds the reclaim horizon derived from it. No
// horizon derived from the request budget can cover such a request, and
// a horizon shorter than the request silently refunds the tokens it
// really spent. The trade is deliberately asymmetric: a generous bound
// delays recovery of a crashed process's reservations, while a short one
// gives away real spend with nothing left to notice it.
const unboundedRequestLeaseTTL = 24 * time.Hour

// requestBudget is the longest one client request can hold a
// reservation: the attempt phase, plus a committed stream's body, which
// the overall deadline deliberately stops governing. ok is false when
// either bound is absent (0), in which case no horizon can be derived.
func (c Config) requestBudget() (time.Duration, bool) {
	if c.Retry.OverallDeadline <= 0 || c.Retry.StreamTimeout <= 0 {
		return 0, false
	}
	return c.Retry.OverallDeadline + c.Retry.StreamTimeout, true
}

// RequestCeiling is the longest one client request can run. It bounds
// work that must outlive the client that started it, because no request
// can legitimately consume upstream beyond it. A request that declares
// no upper limit reports the same generous ceiling the lease horizon
// falls back to, so a caller always gets a usable bound instead of
// "unlimited".
func (c Config) RequestCeiling() time.Duration {
	if budget, ok := c.requestBudget(); ok {
		return budget
	}
	return unboundedRequestLeaseTTL
}

// LeaseTTL is the reclaim horizon the ledger backends run with: the
// configured value, or the one derived from the request budget. Load
// rejects a configuration whose value is not enough to cover the
// longest request.
func (c Config) LeaseTTL() time.Duration {
	if c.Quota.LeaseTTL > 0 {
		return c.Quota.LeaseTTL
	}
	if budget, ok := c.requestBudget(); ok {
		return budget + leaseTTLHeadroom
	}
	return unboundedRequestLeaseTTL
}

// Postgres holds connection settings for the system of record
// (tenants, API keys, quota reconciliation records).
type Postgres struct {
	DSN string
}

// Obs holds observability subsystem sizing.
type Obs struct {
	// AccessLogQueueSize caps the in-memory access log queue; overflow
	// drops entries and must be counted explicitly.
	AccessLogQueueSize int
	// AccessLogPath is the JSONL file the access log writes to; empty
	// disables the file sink (metrics stay active).
	AccessLogPath string
	// InsightsDSN is the PostgreSQL database the monitoring and
	// assessment records persist to; empty disables the record store
	// (the access log and metrics stay active). Defaults to the main
	// identity DSN when that is configured.
	InsightsDSN string
}

// Security holds the management-surface credentials.
type Security struct {
	// AdminToken guards the admin API; empty disables authentication
	// (local development only).
	AdminToken string
}
