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
	Upstreams []Upstream
	Retry     Retry
	// Identity is the raw JSON identity set (tiers, tenants, keys) for
	// deployments without a database; its schema is owned by the auth
	// package, keeping this package a leaf.
	Identity string
	// ReconcileInterval paces the quota ledger reconciliation (PRD Q6);
	// zero disables the protocol.
	ReconcileInterval time.Duration
	Cache             Cache
	Circuit           Circuit
	Security          Security
	// Routing selects the candidate ordering policy of the router.
	Routing Routing
}

// Routing configures how the router orders eligible candidates.
type Routing struct {
	// Strategy is "static" (configured order, the default) or "latency"
	// (prefer the lowest measured upstream exchange latency). Health
	// gating (breakers) and operator switches apply in both modes.
	Strategy string
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
	Enabled       bool
	FailThreshold int
	Cooldown      time.Duration
	ProbeTimeout  time.Duration
}

// Upstream is one OpenAI-compatible provider binding. List order across
// Upstreams doubles as failover priority per model: the first entry
// serving a model is its primary.
type Upstream struct {
	// ID names the upstream for routing and breaker state.
	ID string `json:"id"`
	// BaseURL is the scheme and host, without trailing slash.
	BaseURL string `json:"base_url"`
	// APIKey is the bearer token; empty for the mock upstream.
	APIKey string `json:"api_key"`
	// ProbeURL is the health endpoint; empty disables probing.
	ProbeURL string `json:"probe_url"`
	// Models are the model identifiers served; "*" is the wildcard. An
	// entry of the form "client=real" serves the client-facing name
	// "client" by forwarding the provider-real name "real" — the
	// gateway routes on the client name, the adapter rewrites the body.
	Models []string `json:"models"`
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
	BudgetMaxInFlight int
	// StreamTimeout bounds a committed stream's whole body once the
	// reply headers arrived; zero lets the client own the stream's
	// lifetime. It exists because the per-attempt timeout would
	// otherwise kill legitimate long completions mid-stream.
	StreamTimeout time.Duration
}

// Server holds HTTP listener settings.
type Server struct {
	// Addr is the listen address of the gateway.
	Addr string
	// ShutdownGrace bounds how long in-flight requests may finish after a
	// shutdown signal (invariant I8).
	ShutdownGrace time.Duration
}

// Redis holds connection settings for the hot governance state store
// (rate limit buckets, quota hot ledger). The response cache is
// process-local and does not use Redis.
type Redis struct {
	Addr string
}

// Postgres holds connection settings for the system of record
// (tenants, API keys, quota reconciliation records).
type Postgres struct {
	DSN string
}

// Obs holds observability subsystem sizing.
type Obs struct {
	// AccessLogQueueSize caps the in-memory access log queue; overflow
	// drops entries and must be counted explicitly (invariant I7).
	AccessLogQueueSize int
	// AccessLogPath is the JSONL file the access log writes to; empty
	// disables the file sink (metrics stay active).
	AccessLogPath string
}

// Security holds the management-surface credentials.
type Security struct {
	// AdminToken guards the admin API; empty disables authentication
	// (local development only).
	AdminToken string
}
