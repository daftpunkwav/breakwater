/**
 * @file testhelpers_test
 * @description Shared test scaffolding for the package: bounded
 * flush/close drivers for the async logger and exposition renderers
 * for the metrics registry.
 */
package obs

import (
	"context"
	"strings"
	"testing"
	"time"
)

// flushWithTimeout drains the logger within a bounded wait, failing
// the test when the flush does not settle.
func flushWithTimeout(t *testing.T, l *Logger) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// closeWithTimeout shuts the logger down within a bounded wait and
// returns the error so each caller phrases its own failure.
func closeWithTimeout(t *testing.T, l *Logger) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return l.Close(ctx)
}

// renderExposition renders the registry's exposition, failing the test
// if the rendering errors, and returns the payload.
func renderExposition(t *testing.T, m *Metrics) string {
	t.Helper()
	var out strings.Builder
	if err := m.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	return out.String()
}

// assertExpositionContains renders the registry and requires every
// fragment to appear in the exposition.
func assertExpositionContains(t *testing.T, m *Metrics, want ...string) {
	t.Helper()
	text := renderExposition(t, m)
	for _, w := range want {
		if !strings.Contains(text, w) {
			t.Errorf("exposition missing %q\ngot:\n%s", w, text)
		}
	}
}
