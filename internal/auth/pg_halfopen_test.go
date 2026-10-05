/**
 * @file pg_halfopen_test
 * @description The half-open database shape: the TCP connection
 * establishes but no answer ever arrives. Resolution must fail at its
 * own bound instead of hanging until the client walks away, and the
 * failure must read as a transient outage, never as an unauthorized
 * verdict — identity stays fail-closed.
 */
package auth

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// halfOpenListener accepts connections and never speaks: the pool's
// dial succeeds, the query goes out, nothing comes back.
type halfOpenListener struct {
	net.Listener
}

func (h *halfOpenListener) acceptForever() {
	for {
		conn, err := h.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			// Hold the connection open without answering; drain whatever
			// the driver sends so TCP never pushes back, then just keep
			// reading until the test tears the listener down.
			_, _ = io.Copy(io.Discard, c)
		}(conn)
	}
}

func TestPGResolveFailsInsteadOfHangingOnAHalfOpenDatabase(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	half := &halfOpenListener{Listener: ln}
	go half.acceptForever()
	defer func() { _ = ln.Close() }()

	// Shorten the resolution bound so the test stays fast; restored on
	// exit. Not parallel: the bound is package state.
	saved := resolveTimeout
	resolveTimeout = 150 * time.Millisecond
	defer func() { resolveTimeout = saved }()

	store, err := NewPGStore(context.Background(),
		"postgres://breakwater:secret@"+ln.Addr().String()+"/breakwater", "")
	if err != nil {
		t.Fatalf("NewPGStore with a syntactically valid dsn: %v", err)
	}
	defer store.Close()

	start := time.Now()
	type resolved struct {
		tenant Tenant
		err    error
	}
	done := make(chan resolved, 1)
	go func() {
		tenant, err := store.Resolve(context.Background(), "sk-half-open")
		done <- resolved{tenant, err}
	}()
	var got resolved
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Resolve against a half-open database never returned: it hangs on the request context instead of failing at its own bound")
	}
	elapsed := time.Since(start)
	if got.err == nil {
		t.Fatalf("Resolve against a half-open database = (%+v, nil), want a failure", got.tenant)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Resolve returned after %s, want it bounded by the resolution timeout", elapsed)
	}
	if errors.Is(got.err, ErrUnauthorized) {
		t.Fatalf("err = %v, an outage must not surface as ErrUnauthorized", got.err)
	}
	if !strings.Contains(got.err.Error(), "resolve key") {
		t.Fatalf("err = %q, want the resolve-key wrap", got.err)
	}
}
