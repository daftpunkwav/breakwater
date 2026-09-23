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
	Server   Server
	Redis    Redis
	Postgres Postgres
	Obs      Obs
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
