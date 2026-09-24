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
	// Models are the model identifiers served; "*" is the wildcard.
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
// (rate limit buckets, quota hot ledger, response cache).
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
}
