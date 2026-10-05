/**
 * @file serve_test
 * @description Lifecycle tests of the assembled gateway: serve-until-
 * cancelled with and without governance, startup failures surfacing as
 * returned errors, and the helpers of the assembly.
 */
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/jackc/pgx/v5"
)

// firstLine condenses a SQL statement for a failure message.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// baseEnv returns the environment entries of a minimal working gateway:
// one mock upstream, one static identity, a guarded admin surface (the
// armed-posture guard refuses to boot without it), an ephemeral port.
func baseEnv(addr string) []string {
	return []string{
		"BREAKWATER_ADDR=" + addr,
		`BREAKWATER_UPSTREAMS=[{"id":"mock","base_url":"http://127.0.0.1:1","models":["*"]}]`,
		`BREAKWATER_IDENTITY={"tiers":[{"id":"free","rpm":10,"tpm":1000,"max_tokens":64,"monthly_quota":1000,"allowed_models":["*"]}],"tenants":[{"id":"t1","name":"T1","tier":"free","keys":["k1"]}]}`,
		"BREAKWATER_ADMIN_TOKEN=test-admin",
	}
}

func setEnv(t *testing.T, entries []string) {
	t.Helper()
	for _, key := range []string{
		"BREAKWATER_REDIS_ADDR", "BREAKWATER_POSTGRES_DSN", "BREAKWATER_ACCESS_LOG_PATH",
		"BREAKWATER_UPSTREAMS", "BREAKWATER_IDENTITY", "BREAKWATER_ADDR",
		"BREAKWATER_ADMIN_TOKEN", "BREAKWATER_ALLOW_UNAUTHENTICATED",
	} {
		t.Setenv(key, "")
	}
	for _, entry := range entries {
		key, value, _ := strings.Cut(entry, "=")
		t.Setenv(key, value)
	}
}

// awaitGateway polls the liveness endpoint until the gateway answers,
// so the lifecycle tests interact with a server that is actually up
// instead of guessing a startup delay.
func awaitGateway(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway never came up on %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// serveUntilReady binds the listener, starts serve in the background
// and returns once the gateway answers, so a test cancels a server that
// is really running. Ownership of the listener transfers to serve.
func serveUntilReady(t *testing.T, ctx context.Context, cfg config.Config, listener net.Listener) chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, slog.New(slog.DiscardHandler), "test", listener) }()
	awaitGateway(t, listener.Addr().String())
	return done
}

// TestServeRunsAndStops pins the full assembled lifecycle: the gateway
// comes up serving, and a cancelled parent context then ends serve
// cleanly with a nil error.
func TestServeRunsAndStops(t *testing.T) {
	setEnv(t, baseEnv("127.0.0.1:0"))
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := serveUntilReady(t, ctx, cfg, listener)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// TestServeDrainTimeoutExitsCleanly pins the shutdown contract: a
// grace window that expires with requests still in flight is a
// shutdown with a warning, not a failure. Returning an error here would
// make every rolling restart that carries a long stream exit non-zero,
// which orchestrators read as a failed unit and restart in a loop.
func TestServeDrainTimeoutExitsCleanly(t *testing.T) {
	// An upstream that never answers, so a request stays in flight.
	// The handler parks on a test-owned channel, not the request
	// context: the gateway abandons the exchange at drain timeout and
	// its transport may keep the pooled connection open, so no TCP
	// close ever arrives — the test releases the handler itself.
	reached := make(chan struct{})
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		close(reached)
		<-release
	}))
	// LIFO: registered last runs first, so the handler is released
	// before its server drains.
	defer slow.Close()
	defer close(release)

	entries := baseEnv("127.0.0.1:0")
	entries = append(entries,
		`BREAKWATER_UPSTREAMS=[{"id":"slow","base_url":"`+slow.URL+`","models":["*"]}]`,
		"BREAKWATER_SHUTDOWN_GRACE=1ns", // any request still in flight outlives it
	)
	setEnv(t, entries)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// The listener stays bound and is handed to the server, so the
	// failure under test is the drain and not a port collision with
	// another package's test process.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}
	addr := listener.Addr().String()
	cfg.Server.Addr = addr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := serveUntilReady(t, ctx, cfg, listener)

	// Hold a request open across the shutdown signal; the upstream's
	// reached channel proves it is in flight before the cancel fires.
	// The client context is cancelled once serve has returned: the
	// drain timeout abandons the connection without closing it (the
	// process exit does that in production), so the test releases the
	// request itself — client-gone propagates to the upstream and the
	// deferred slow.Close can drain.
	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()
	go func() {
		req, _ := http.NewRequestWithContext(reqCtx, http.MethodPost,
			"http://"+addr+"/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
		req.Header.Set("Authorization", "Bearer k1")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the upstream")
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("a drain timeout is a clean shutdown, got: %v", err)
	}
	reqCancel()
}
func TestServeRedisModeWithIdentity(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, baseEnv("127.0.0.1:0"))
	t.Setenv("BREAKWATER_REDIS_ADDR", mr.Addr())
	t.Setenv("BREAKWATER_POSTGRES_DSN", "")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := serveUntilReady(t, ctx, cfg, listener)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// TestServeReconcilerNeedsReachableIdentity pins the reconciler's
// assembly contract: with Redis, a DSN and an interval armed, the
// worker enumerates tenants from the identity database — an
// unreachable one is a deployment failure surfaced at startup, not a
// silently missing reconcile loop.
func TestServeReconcilerNeedsReachableIdentity(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, baseEnv("127.0.0.1:0"))
	t.Setenv("BREAKWATER_REDIS_ADDR", mr.Addr())
	t.Setenv("BREAKWATER_POSTGRES_DSN", "postgres://breakwater:breakwater@127.0.0.1:1/db")
	t.Setenv("BREAKWATER_RECONCILE_INTERVAL", "1m")
	// The admin-token guard is not this test's subject: arm the surface
	// so Load passes and the failure comes from the unreachable database.
	t.Setenv("BREAKWATER_ADMIN_TOKEN", "test")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// The reconciler's assembly fails before anything listens, so serve
	// returns synchronously; the timeout only guards a regression that
	// would hang instead of failing.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := serve(ctx, cfg, slog.New(slog.DiscardHandler), "test", nil); err == nil {
		t.Fatal("serve: an unreachable identity database must fail the reconciler's startup")
	}
}

// TestServeWithoutIdentity covers the warn branch: a gateway with
// upstreams but no identity serves business routes without governance.
// config.Load refuses that posture (the unauthenticated-deployment
// guard), so the opt-in arms it explicitly — the serve() warn branch
// stays reachable for direct callers either way.
func TestServeWithoutIdentity(t *testing.T) {
	setEnv(t, []string{
		"BREAKWATER_ADDR=127.0.0.1:0",
		`BREAKWATER_UPSTREAMS=[{"id":"mock","base_url":"http://127.0.0.1:1","models":["*"]}]`,
		"BREAKWATER_IDENTITY=",
		"BREAKWATER_ALLOW_UNAUTHENTICATED=1",
	})
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := serveUntilReady(t, ctx, cfg, listener)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// TestServeReturnsStartupErrors pins that assembly failures surface as
// returned errors instead of exit codes. Each branch below trips a
// different assembly step after config.Load has passed.
func TestServeReturnsStartupErrors(t *testing.T) {
	t.Run("identity store", func(t *testing.T) {
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_POSTGRES_DSN", "not a valid dsn")
		// The admin-token guard is not this test's subject: arm the
		// surface so the failure comes from the identity store assembly.
		t.Setenv("BREAKWATER_ADMIN_TOKEN", "test")
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("assembly failure must surface as a returned error")
		}
	})

	t.Run("access log path", func(t *testing.T) {
		setEnv(t, baseEnv("127.0.0.1:0")) // DSN stays empty: static identity
		t.Setenv("BREAKWATER_ACCESS_LOG_PATH", filepath.Join(t.TempDir(), "missing-dir", "a.log"))
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("an unopenable access log must fail assembly")
		}
	})

	t.Run("dead redis", func(t *testing.T) {
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_REDIS_ADDR", "127.0.0.1:1")
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("dead redis: assembly failure must surface as a returned error")
		}
	})

	t.Run("busy port", func(t *testing.T) {
		// Occupy the port first: exercises the serve error path
		// (observation flush, then the returned error). The exact same
		// address form is reused — a bare ":port" could still bind on
		// the dual-stack wildcard.
		blocker := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
		defer blocker.Close()
		setEnv(t, baseEnv(blocker.Listener.Addr().String()))
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("a busy port must surface as a returned error")
		}
	})

	t.Run("malformed static identity", func(t *testing.T) {
		// Rejected by the identity parser, so the failure comes from the
		// store assembly rather than from any later step.
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_IDENTITY", "{not json")
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("malformed identity JSON must fail assembly")
		}
	})

	t.Run("invalid model binding", func(t *testing.T) {
		// A binding the adapter cannot resolve: caught when the upstream
		// adapters are built, after the identity store is in place.
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_UPSTREAMS", `[{"id":"mock","base_url":"http://127.0.0.1:1","models":["=broken"]}]`)
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("an unresolvable model binding must fail assembly")
		}
	})

	t.Run("fallback names an unknown model", func(t *testing.T) {
		// A typo in the fallback chain has to refuse the boot rather than
		// silently never fire. The upstream is pinned to concrete model
		// names: a wildcard binding serves every name, so it would make
		// the unknown model resolvable and the check would pass.
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_UPSTREAMS", `[{"id":"mock","base_url":"http://127.0.0.1:1","models":["m1"]}]`)
		t.Setenv("BREAKWATER_FALLBACKS", `{"m1":["nope"]}`)
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("a fallback naming an unconfigured model must fail assembly")
		}
	})

	t.Run("unknown routing strategy", func(t *testing.T) {
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_ROUTING_STRATEGY", "random")
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("an unknown routing strategy must fail assembly")
		}
	})
}

// TestRunRejectsBadConfiguration covers the run wrapper: an invalid
// environment is rejected before anything is assembled.
func TestRunRejectsBadConfiguration(t *testing.T) {
	setEnv(t, baseEnv("127.0.0.1:0"))
	t.Setenv("BREAKWATER_STREAM_TIMEOUT", "-5s")
	if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
		t.Fatal("run must reject an invalid configuration")
	}
}

// TestRunRejectsUnauthenticatedDeployment: the binary path refuses the
// open-proxy posture before anything listens — upstreams armed, no
// identity source, no opt-in.
func TestRunRejectsUnauthenticatedDeployment(t *testing.T) {
	setEnv(t, []string{
		"BREAKWATER_ADDR=127.0.0.1:0",
		`BREAKWATER_UPSTREAMS=[{"id":"mock","base_url":"http://127.0.0.1:1","models":["*"]}]`,
		"BREAKWATER_IDENTITY=",
	})
	if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
		t.Fatal("run must refuse upstreams without an identity source")
	}
}

func TestMergeReadiness(t *testing.T) {
	t.Parallel()
	if mergeReadiness(nil, nil) != nil {
		t.Fatal("all-nil probes must report ready (nil)")
	}

	okProbe := func() error { return nil }
	badProbe := func() error { return errors.New("redis down") }

	if err := mergeReadiness(nil, okProbe)(); err != nil {
		t.Fatalf("nil probes must drop out: %v", err)
	}
	if err := mergeReadiness(okProbe, badProbe)(); err == nil {
		t.Fatal("one failing probe must fail the merge")
	}
}

func TestStateValue(t *testing.T) {
	t.Parallel()
	if stateValue(circuit.StateClosed) != 0 ||
		stateValue(circuit.StateHalfOpen) != 1 ||
		stateValue(circuit.StateOpen) != 2 {
		t.Fatal("state gauge mapping drifted")
	}
}

func TestMetricsHandlerRenders(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	metricsHandler(obs.NewMetrics()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("content type = %q", ct)
	}
	if rec.Body.Len() == 0 {
		t.Fatal("exposition must not be empty")
	}
}

func TestBuildBindingsValidates(t *testing.T) {
	t.Parallel()
	if _, _, err := buildBindings([]config.Upstream{{ID: "broken"}}); err == nil {
		t.Fatal("upstream without base_url must fail binding")
	}
	bindings, _, err := buildBindings([]config.Upstream{{ID: "ok", BaseURL: "http://x", Models: []string{"*"}}})
	if err != nil || len(bindings) != 1 {
		t.Fatalf("bindings = %v err = %v", bindings, err)
	}
}

func TestNewAccessLogDisabledAndEnabled(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	disabled, err := newAccessLog(config.Config{}, logger)
	if err != nil || disabled.logger != nil || disabled.sink != nil {
		t.Fatalf("no path configured must disable the access log: %v", err)
	}
	disabled.close() // must be safe when disabled

	path := filepath.Join(t.TempDir(), "access.log")
	enabled, err := newAccessLog(config.Config{Obs: config.Obs{AccessLogPath: path}}, logger)
	if err != nil {
		t.Fatalf("open access log: %v", err)
	}
	if enabled.logger == nil {
		t.Fatal("configured path must enable the access log")
	}
	// An unopenable path is an error, never a silently unobserved gateway.
	if _, err := newAccessLog(config.Config{Obs: config.Obs{AccessLogPath: filepath.Join(t.TempDir(), "missing-dir", "a.log")}}, logger); err == nil {
		t.Fatal("unopenable path must fail assembly")
	}
	enabled.sink.Record(obs.Entry{Status: 200})
	// close drains the logger AND releases the file (Windows locks open
	// files, so TempDir cleanup would fail otherwise).
	enabled.close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("access log file missing: %v", err)
	}
}

func TestSeedBalancesProvisionsTenants(t *testing.T) {
	t.Parallel()
	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", RPM: 10, TPM: 1000, MaxTokens: 64, MonthlyQuota: 1000}},
		Tenants: []auth.StaticTenant{{ID: "t1", Name: "T1", Tier: "free", Keys: []string{"k1"}}},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	ledger := quota.NewMemory()
	seedBalances(context.Background(), identity, ledger, slog.New(slog.DiscardHandler))

	bal, err := ledger.Balance(context.Background(), "t1")
	if err != nil || bal != 1000 {
		t.Fatalf("balance = %d err = %v, want the tier monthly quota", bal, err)
	}
}

// TestServeReconcilerArmsAgainstLivePostgres (CI only): with Redis, a
// reachable identity database and an interval, serve() arms the
// reconcile worker over the database's tenants and starts cleanly —
// the assembly path a live deployment runs.
func TestServeReconcilerArmsAgainstLivePostgres(t *testing.T) {
	dsn := os.Getenv("BREAKWATER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("BREAKWATER_TEST_POSTGRES_DSN not set: reconcile arming needs a live PostgreSQL")
	}
	// The CI service starts an empty database, and serve arms the
	// reconciler by listing tenants from the identity schema: the
	// tables must exist before assembly. Apply the deploy schema —
	// the same file first boot applies, idempotent by construction —
	// statement by statement, after stripping the comment lines (a
	// bare semicolon split would otherwise execute comment-only
	// fragments).
	// nosemgrep: go_filesystem_rule-fileread -- path is a fixed literal inside the repository
	schema, err := os.ReadFile(filepath.Join("..", "..", "deploy", "schema.sql"))
	if err != nil {
		t.Fatalf("read deploy schema: %v", err)
	}
	var cleaned strings.Builder
	for _, line := range strings.Split(string(schema), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		cleaned.WriteString(line)
		cleaned.WriteByte('\n')
	}
	connCtx, cancelConn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelConn()
	conn, err := pgx.Connect(connCtx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	for _, stmt := range strings.Split(cleaned.String(), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		// nosemgrep: go_sql_rule-concat-sqli -- statements come from the repository's own deploy/schema.sql
		if _, err := conn.Exec(connCtx, stmt); err != nil {
			t.Fatalf("apply schema statement %q: %v", firstLine(stmt), err)
		}
	}
	_ = conn.Close(connCtx)

	mr := miniredis.RunT(t)
	setEnv(t, baseEnv("127.0.0.1:0"))
	t.Setenv("BREAKWATER_REDIS_ADDR", mr.Addr())
	t.Setenv("BREAKWATER_POSTGRES_DSN", dsn)
	t.Setenv("BREAKWATER_RECONCILE_INTERVAL", "1m")
	t.Setenv("BREAKWATER_ADMIN_TOKEN", "test")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := serveUntilReady(t, ctx, cfg, listener)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}
