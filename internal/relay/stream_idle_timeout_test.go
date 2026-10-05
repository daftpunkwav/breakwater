/**
 * @file stream_idle_timeout_test
 * @description The streaming idle watchdog: an upstream that stops
 * producing loses the stream through the honest termination contract
 * as a gateway-originated timeout, activity holds it off, and the
 * disabled state changes nothing.
 */
package relay

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// stallingStream returns a response whose body first delivers one frame, then
// stalls forever — like a real HTTP body, reads fail with the
// context's error once the attempt's forward context is cancelled.
func stallingStream(ctx context.Context, stall time.Duration, resume io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		_, _ = fmt.Fprint(pw, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		if !waitOrDie(ctx, pw, stall) {
			return
		}
		if resume != nil {
			_, _ = io.Copy(pw, resume)
		}
		_ = pw.Close()
	}()
	return pr
}

// recordingBreaker grants everything and records every outcome, so a
// test can assert the verdict the exchange reported.
type recordingBreaker struct {
	mu      sync.Mutex
	outcome []circuit.Outcome
}

func (b *recordingBreaker) Allow(context.Context, string) (circuit.Permission, bool) {
	return &recordingPermission{b}, true
}

func (b *recordingBreaker) StateOf(context.Context, string) circuit.State { return circuit.StateClosed }

func (b *recordingBreaker) Reset(context.Context, string) {}

func (b *recordingBreaker) reported() []circuit.Outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]circuit.Outcome(nil), b.outcome...)
}

type recordingPermission struct{ b *recordingBreaker }

func (p *recordingPermission) Report(o circuit.Outcome) {
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	p.b.outcome = append(p.b.outcome, o)
}

// TestIdleWatchdogCutsSilentStream: after the commit the upstream goes
// quiet past the idle window — the stream is terminated through the
// honest error contract as an upstream timeout, and the breaker hears
// the gateway's own neutral verdict, not upstream evidence.
func TestIdleWatchdogCutsSilentStream(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		// Stalls long past the idle window; the stream ceiling would
		// only fire much later, so the cut must be the idle one.
		return sseResponse(stallingStream(ctx, 10*time.Second, nil)), nil
	}}
	breaker := &recordingBreaker{}
	exec := New(retry.Policy{MaxAttempts: 1}, nil,
		WithBreaker(breaker),
		WithStreamTimeout(time.Minute),
		WithStreamIdleTimeout(80*time.Millisecond))

	start := time.Now()
	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Aborted {
		t.Fatalf("stream completed: %q, want an idle cut", result.Body)
	}
	if result.ErrorCode != "upstream_timeout" {
		t.Fatalf("error code = %q, want upstream_timeout", result.ErrorCode)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cut took %v; the idle window, not the minute ceiling, must end the stream", elapsed)
	}
	if !strings.Contains(string(result.Body), "upstream_timeout") {
		t.Fatalf("client stream = %q, want the honest error frame", result.Body)
	}
	reported := breaker.reported()
	if len(reported) != 1 || reported[0] != circuit.OutcomeGatewayTerminated {
		t.Fatalf("breaker outcomes = %v, want exactly the gateway-terminated verdict", reported)
	}
}

// TestIdleWatchdogHeldOffByActivity: an upstream that keeps producing
// inside the window survives, however long the total run — activity
// re-arms the watchdog on every body read.
func TestIdleWatchdogHeldOffByActivity(t *testing.T) {
	t.Parallel()
	// Five frames, one every 40ms inside the 150ms window: ~200ms
	// total, past the idle window many times over.
	cand := sseDripUpstream(5, 40*time.Millisecond)
	exec := New(retry.Policy{MaxAttempts: 1}, nil, WithStreamIdleTimeout(150*time.Millisecond))

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Aborted {
		t.Fatalf("stream aborted: %q — steady activity must hold the watchdog off", result.Body)
	}
	if !strings.Contains(string(result.Body), "c5") {
		t.Fatalf("late chunks lost: %q", result.Body)
	}
}

// TestIdleWatchdogDisabledByDefault: with no idle window configured a
// permanently silent stream still ends — through the ceiling, exactly
// as before the watchdog existed.
func TestIdleWatchdogDisabledByDefault(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		return sseResponse(stallingStream(ctx, time.Minute, nil)), nil
	}}
	breaker := &recordingBreaker{}
	exec := New(retry.Policy{MaxAttempts: 1}, nil,
		WithBreaker(breaker),
		WithStreamTimeout(120*time.Millisecond))

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Aborted || result.ErrorCode != "upstream_timeout" {
		t.Fatalf("result = aborted %v code %q, want the ceiling cut as before",
			result.Aborted, result.ErrorCode)
	}
	reported := breaker.reported()
	if len(reported) != 1 || reported[0] != circuit.OutcomeGatewayTerminated {
		t.Fatalf("breaker outcomes = %v, want the ceiling's gateway-terminated verdict", reported)
	}
}
