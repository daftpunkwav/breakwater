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
	// The half-open transition runs on the registry's own clock: a
	// one-hour cooldown advanced by hand proves the transition is
	// elapsed-time driven, with no real-time sleep to outpace.
	cooldown := time.Hour
	now := time.Now()
	clock := now
	breaker := buildBreaker(config.Circuit{Enabled: true, FailThreshold: 1, Cooldown: cooldown, ProbeTimeout: time.Second}, metrics,
		circuit.WithClock(func() time.Time { return clock }))

	perm, ok := breaker.Allow(context.Background(), "u")
	if !ok {
		t.Fatal("closed breaker denied")
	}
	perm.Report(circuit.OutcomeServerFault) // -> open, opened at now

	clock = now.Add(cooldown) // the cooldown elapses on the breaker's clock
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

// TestBuildBreakerRatioStrategy: the ratio strategy composes the
// windowed probability guard — the state gauge stays closed through
// failures, denials ride the denial counter, and Reset admits again.
func TestBuildBreakerRatioStrategy(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	breaker := buildBreaker(config.Circuit{Enabled: true, Strategy: "ratio"}, metrics)

	// Saturate the window with failure evidence. From the sixth event
	// on a denial is a legal outcome under the real random source, and
	// a denial feeds the window too — both paths build the pressure.
	for i := 0; i < 20; i++ {
		perm, ok := breaker.Allow(context.Background(), "u")
		if !ok {
			continue
		}
		perm.Report(circuit.OutcomeServerFault)
	}
	if got := breaker.StateOf(context.Background(), "u"); got != circuit.StateClosed {
		t.Fatalf("ratio state = %s, want closed", got)
	}

	// Denials are probabilistic (covered at the unit level); here the
	// wiring just has to hold under a denial draw and an operator reset.
	breaker.Allow(context.Background(), "u")
	breaker.Reset(context.Background(), "u")
	if perm, ok := breaker.Allow(context.Background(), "u"); !ok || perm == nil {
		t.Fatal("expected admission after Reset")
	}
}
