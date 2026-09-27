/**
 * @file breaker_test
 * @description The breaker's composition at the assembly root: the
 * enabled and disabled modes, and the observation wiring the half-open
 * transition needs.
 */
package main

import (
	"context"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
)

func TestBuildBreakerHalfOpenObserver(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	breaker := buildBreaker(config.Circuit{Enabled: true, FailThreshold: 1, Cooldown: 2 * time.Millisecond, ProbeTimeout: time.Second}, metrics)

	perm, ok := breaker.Allow(context.Background(), "u")
	if !ok {
		t.Fatal("closed breaker denied")
	}
	perm.Report(circuit.OutcomeServerFault) // -> open

	time.Sleep(5 * time.Millisecond) // cooldown elapses
	if _, ok := breaker.Allow(context.Background(), "u"); !ok {
		t.Fatal("half-open must admit the probe")
	}
}

func TestBuildBreakerBothModes(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()

	if _, ok := buildBreaker(config.Circuit{Enabled: false}, metrics).(circuit.NopBreaker); !ok {
		t.Fatal("disabled circuit must compose the nop breaker")
	}

	breaker := buildBreaker(config.Circuit{Enabled: true, FailThreshold: 1, Cooldown: time.Hour, ProbeTimeout: time.Second}, metrics)
	perm, ok := breaker.Allow(context.Background(), "u")
	if !ok {
		t.Fatal("enabled breaker denied a fresh upstream")
	}
	perm.Report(circuit.OutcomeServerFault) // closed -> open, observed
	if got := breaker.StateOf(context.Background(), "u"); got != circuit.StateOpen {
		t.Fatalf("state = %s, want open", got)
	}
}
