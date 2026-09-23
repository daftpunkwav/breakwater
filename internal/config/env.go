/**
 * @file env
 * @description Environment-based configuration loading with validation.
 *
 * Responsibilities:
 * - Read BREAKWATER_* environment variables
 * - Fall back to defaults and reject invalid values loudly
 */
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Defaults keep the gateway runnable with zero configuration. They are
// provisional tuning values and may change during implementation.
const (
	defaultAddr           = ":8080"
	defaultShutdownGrace  = 15 * time.Second
	defaultRedisAddr      = "127.0.0.1:6379"
	defaultAccessLogQueue = 4096
)

// Environment variable names.
const (
	envAddr           = "BREAKWATER_ADDR"
	envShutdownGrace  = "BREAKWATER_SHUTDOWN_GRACE"
	envRedisAddr      = "BREAKWATER_REDIS_ADDR"
	envPostgresDSN    = "BREAKWATER_POSTGRES_DSN"
	envAccessLogQueue = "BREAKWATER_ACCESS_LOG_QUEUE_SIZE"
)

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	cfg := Config{
		Server: Server{
			Addr:          envString(envAddr, defaultAddr),
			ShutdownGrace: defaultShutdownGrace,
		},
		Redis: Redis{
			Addr: envString(envRedisAddr, defaultRedisAddr),
		},
		Postgres: Postgres{
			DSN: strings.TrimSpace(os.Getenv(envPostgresDSN)),
		},
		Obs: Obs{
			AccessLogQueueSize: defaultAccessLogQueue,
		},
	}

	var err error
	if cfg.Server.ShutdownGrace, err = envDuration(envShutdownGrace, cfg.Server.ShutdownGrace); err != nil {
		return Config{}, err
	}
	if cfg.Obs.AccessLogQueueSize, err = envInt(envAccessLogQueue, cfg.Obs.AccessLogQueueSize); err != nil {
		return Config{}, err
	}
	if cfg.Server.ShutdownGrace <= 0 {
		return Config{}, fmt.Errorf("config: %s must be positive", envShutdownGrace)
	}
	if cfg.Obs.AccessLogQueueSize <= 0 {
		return Config{}, fmt.Errorf("config: %s must be positive", envAccessLogQueue)
	}
	return cfg, nil
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: parse %s=%q: %w", key, raw, err)
	}
	return d, nil
}

func envInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: parse %s=%q: %w", key, raw, err)
	}
	return n, nil
}
