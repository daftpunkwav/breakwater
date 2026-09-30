/**
 * @file executor
 * @description The relay engine: executes one client request against
 * candidate upstreams, owning the attempt loop, failover order, usage
 * extraction and the honest termination of streaming replies.
 *
 * Responsibilities:
 * - Turn one Job into exactly one client-visible response, under the
 *   retry policy and the process-wide retry budget
 * - Report breaker outcomes per attempt (the router pre-filters
 *   breaker-open upstreams; enforcement and accounting happen here)
 * - Nothing else: candidate ordering belongs to the router, transport
 *   to the adapters, wire formats to the protocol package
 *
 * Response-side shape: retry, circuit breaking and
 * failover live INSIDE the forward stage; the stages after it (settle,
 * cache write, observation) run once per client request, not per
 * attempt — which is why Execute returns a Result carrying the served
 * upstream and the usage instead of writing them anywhere itself.
 */
package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// maxResponseBytes bounds how much of a non-streaming upstream body is
// buffered. Beyond it the exchange fails closed: a body that size cannot
// be cached or replayed anyway.
const maxResponseBytes = 32 << 20

// Executor executes relay jobs. It is safe for concurrent use.
type Executor struct {
	policy     retry.Policy
	budget     *retry.Budget
	classifier retry.Classifier
	breaker    circuit.Breaker
	metrics    *obs.Metrics
	// observer, when set, receives one outcome per upstream attempt for
	// routing-layer performance tracking.
	observer UpstreamObserver
	// fatalHook, when set, receives one report per fatal upstream
	// condition observed on a completed error exchange (dead
	// credentials, exhausted quota), with the credential index the
	// exchange used (-1 when it carried none). The assembly wires it
	// to the credential ring's retirement and, once no credential is
	// left, the routing switch's auto-disable. It fires from the
	// attempt goroutine and must be safe for concurrent use.
	fatalHook func(upstreamID string, credentialIndex int, reason string)
	// streamTimeout bounds a committed stream's whole body; zero means
	// the client owns the stream's lifetime outright.
	streamTimeout time.Duration
	// streamIdleTimeout bounds upstream silence inside a committed
	// stream; zero disables the idle watchdog.
	streamIdleTimeout time.Duration
}

// Option customizes an Executor.
type Option func(*Executor)

// WithBreaker installs the circuit breaker consulted before every
// attempt; nil (the default) disables breaker accounting.
func WithBreaker(b circuit.Breaker) Option {
	return func(e *Executor) { e.breaker = b }
}

// WithClassifier overrides the standard retryability table.
func WithClassifier(c retry.Classifier) Option {
	return func(e *Executor) { e.classifier = c }
}

// WithMetrics installs the observation recorder; nil disables.
func WithMetrics(m *obs.Metrics) Option {
	return func(e *Executor) { e.metrics = m }
}

// UpstreamObserver receives one outcome per upstream exchange, for the
// routing layer's performance tracking. Failed reports an exchange
// that did not complete; a client disconnect is not a failure.
type UpstreamObserver interface {
	ObserveUpstream(upstreamID string, latency time.Duration, failed bool)
}

// WithUpstreamObserver installs the outcome observer; nil disables.
func WithUpstreamObserver(o UpstreamObserver) Option {
	return func(e *Executor) { e.observer = o }
}

// WithUpstreamFatalHook installs the fatal-condition hook; nil (the
// default) disables it. The reason is a machine-readable cause, e.g.
// upstream_auth_failure; the credential index names the credential the
// reported exchange used (-1 when it carried none).
func WithUpstreamFatalHook(fn func(upstreamID string, credentialIndex int, reason string)) Option {
	return func(e *Executor) { e.fatalHook = fn }
}

// WithStreamTimeout sets the ceiling of a committed stream's body: a
// stream that runs longer is terminated honestly through the error
// contract (upstream_timeout). Zero, the default, lets the client own
// the stream's lifetime.
func WithStreamTimeout(d time.Duration) Option {
	return func(e *Executor) { e.streamTimeout = d }
}

// WithStreamIdleTimeout sets the idle watchdog of a committed stream:
// an upstream that goes silent for the whole window loses the stream
// through the same honest error contract, however much total budget
// remains. Zero, the default, disables the watchdog.
func WithStreamIdleTimeout(d time.Duration) Option {
	return func(e *Executor) { e.streamIdleTimeout = d }
}

// New builds an Executor. A nil budget disables the global in-flight
// retry cap; the per-request MaxAttempts still applies. Pass a budget
// to bound retry amplification across the process.
func New(policy retry.Policy, budget *retry.Budget, opts ...Option) *Executor {
	e := &Executor{
		policy:     policy,
		budget:     budget,
		classifier: retry.DefaultClassifier{},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Job is one client request to execute.
type Job struct {
	// Model routes and meters the request.
	Model string
	// Stream selects the passthrough mode.
	Stream bool
	// Body is the neutral OpenAI request body, forwarded verbatim.
	Body []byte
	// RequestID is propagated to the upstream exchange as X-Request-Id
	// so one client-visible identifier correlates the whole path.
	RequestID string
	// Candidates are the upstream instances to try, priority ordered;
	// attempt n uses candidate min(n, len)-1, so failover walks the list
	// once and extra attempts re-hit the last fallback.
	Candidates []upstream.Upstream
	// Fallbacks are the client-facing models tried, in order, when
	// every candidate of the primary model is exhausted. Empty
	// disables the chain.
	Fallbacks []string
	// Resolve turns a fallback model into its candidate list at the
	// moment the chain reaches it; nil ignores Fallbacks.
	Resolve CandidateResolver
	// Wire presents the exchange in the client's format; nil selects
	// the canonical wire (openai-chat, byte passthrough).
	Wire protocol.Wire
	// Out is the client response writer.
	Out http.ResponseWriter
}

// AttemptTrace records one upstream attempt in the access trail: the
// candidate that served it, the credential it used, and the status the
// exchange completed with — zero when it never completed (transport
// failure, breaker denial, a time-to-first-byte cut).
type AttemptTrace struct {
	Upstream string
	// CredentialIndex is the ring position of the credential the
	// attempt used; -1 when the exchange carried none.
	CredentialIndex int
	Status          int
}

// Result reports what one execution did, for the stages that follow the
// forward stage (settlement, cache write, observation).
type Result struct {
	// Status is the status code sent (or intended, when the client was
	// already gone) to the client.
	Status int
	// UpstreamID names the upstream that produced the delivered reply.
	UpstreamID string
	Attempts   int
	Retries    int
	// Trail is the per-attempt record of the forward stage: one entry
	// per upstream attempt, in order. It is what the access log needs
	// to show a request's failover walk, not just its ending.
	Trail []AttemptTrace
	// Usage carries the token usage extracted from the reply;
	// UsageKnown is false when the reply carried none (settlement then
	// falls back to estimation).
	Usage      protocol.Usage
	UsageKnown bool
	// Streamed reports whether a streaming passthrough started;
	// StreamBytes is the byte count it delivered — the fallback metering
	// input when the stream ended without a usage report.
	Streamed    bool
	StreamBytes int64
	// Aborted reports a stream terminated honestly after bytes had
	// already reached the client: through the error event contract, or
	// with nothing further when the client had already walked away. The
	// cache stage treats any aborted stream as unreplayable.
	Aborted bool
	// ClientGone reports that the client disconnected before the
	// response completed.
	ClientGone bool
	// ErrorCode is the gateway-originated failure code when the gateway
	// itself rendered the error: no_upstream, circuit_open,
	// budget_exhausted, upstream_unreachable, or the in-stream abort
	// code of a broken stream. Upstream error passthroughs leave it
	// empty — their status code is the failure classification.
	ErrorCode string
}

// exchangeSnapshot captures one completed upstream error exchange for
// verbatim passthrough to the client once the attempt loop settles.
type exchangeSnapshot struct {
	status int
	header http.Header
	body   []byte
}

// run holds one request's mutable state across attempts. Methods on it
// are called only from the single goroutine executing the attempt loop.
type run struct {
	exec *Executor
	job  Job
	// ctx is the client request context; finish reads it to tell a
	// client disconnect apart from upstream failures.
	ctx  context.Context
	wire protocol.Wire

	attempts   int
	retries    int
	servedBy   string
	usage      protocol.Usage
	usageKnown bool
	streamed   bool
	// trail is the per-attempt record of this request's forward walk,
	// handed to the Result for the access log.
	trail []AttemptTrace

	success       *exchangeSnapshot
	lastFailed    *exchangeSnapshot
	lastFailedErr *retry.StatusError
	terminal      *exchangeSnapshot
	streamBytes   int64
	// rotated is the status error whose failure the stash convicted
	// down to its credential alone: the run's classifier layer calls
	// it retryable so the loop hands the request to the next
	// credential instead of ending it.
	rotated *retry.StatusError
	// gatewayCode records the gateway-originated failure code of the
	// branch that will render the final error; a mid-stream abort
	// records its in-stream code instead.
	gatewayCode string

	// batches is the fallback plan: the primary model's candidates
	// plus every fallback batch resolved so far, append-only.
	// batchIndex is the current batch, batchStart the attempt index
	// where it began, fallbackIndex the next unread position of the
	// Job's chain, and attempted the cycle guard over resolved models.
	// lastCand is the previous attempt's candidate, for the failover
	// metric.
	batches       []batch
	batchIndex    int
	batchStart    int
	fallbackIndex int
	attempted     map[string]bool
	lastCand      upstream.Upstream
	// excluded holds the credentials this request burned per upstream
	// id: the ones a completed exchange proved limited or broken. The
	// next exchange on the same upstream hands the list to the adapter,
	// which serves the request with a different credential.
	excluded map[string][]int
	// startedAt is the current attempt's first instant; the streaming
	// path reads it for the time-to-first-byte observation.
	startedAt time.Time
}

// Execute runs the job. Exactly one HTTP response is written to
// job.Out — either the delivered reply, the passthrough of the last
// upstream error, or a gateway error envelope.
func (e *Executor) Execute(ctx context.Context, job Job) Result {
	if len(job.Candidates) == 0 {
		wire := job.Wire
		if wire == nil {
			wire = protocol.WireFor("")
		}
		wire.RenderError(job.Out, http.StatusBadGateway, "no_upstream",
			"no upstream candidate available for model "+job.Model)
		return Result{Status: http.StatusBadGateway, ErrorCode: "no_upstream"}
	}

	wire := job.Wire
	if wire == nil {
		wire = protocol.WireFor("")
	}
	r := &run{
		exec: e, job: job, ctx: ctx, wire: wire,
		batches:    []batch{{model: job.Model, candidates: job.Candidates}},
		batchStart: 1,
		attempted:  map[string]bool{job.Model: true},
		excluded:   map[string][]int{},
	}
	err := retry.Execute(ctx, e.policy, e.budget, runClassifier{inner: e.classifier, run: r}, nil, r.attempt)
	return r.finish(err)
}

// runClassifier layers the request's own credential verdicts over the
// executor's static table: an exchange the stash convicted down to its
// credential alone is retryable for this request even when the table
// calls the status terminal — the next attempt serves the request with
// another credential instead of ending it.
type runClassifier struct {
	inner retry.Classifier
	run   *run
}

// Retryable implements Classifier, overriding the static table only
// for the one error the request's stash rotated on.
func (c runClassifier) Retryable(err error) bool {
	if err != nil && c.run.rotated != nil && errors.Is(err, c.run.rotated) {
		return true
	}
	return c.inner.Retryable(err)
}

// DelayHint forwards the inner classifier's hint so the loop's type
// assertion keeps working through the decorator; a classifier without
// one behaves as hint-less.
func (c runClassifier) DelayHint(err error) time.Duration {
	if dh, ok := c.inner.(interface{ DelayHint(error) time.Duration }); ok {
		return dh.DelayHint(err)
	}
	return 0
}

// attempt runs one upstream attempt: breaker grant, exchange, outcome
// report. It is the AttemptFunc of the retry loop.
func (r *run) attempt(attemptCtx context.Context, attempt int) error {
	r.attempts = attempt
	cand, model := r.target(attempt)
	if attempt > 1 {
		r.retries = attempt - 1
		r.exec.metrics.RetryScheduled(cand.ID())
		if r.lastCand != nil && r.lastCand.ID() != cand.ID() {
			r.exec.metrics.Failover(r.lastCand.ID(), cand.ID())
		}
	}
	r.lastCand = cand

	var perm circuit.Permission
	if r.exec.breaker != nil {
		p, ok := r.exec.breaker.Allow(attemptCtx, cand.ID())
		if !ok {
			r.trace(cand.ID(), -1, 0)
			return fmt.Errorf("relay: circuit open for upstream %s: %w", cand.ID(), errCircuitOpen)
		}
		perm = p
	}

	started := time.Now()
	r.startedAt = started
	outcome, err := r.exchange(attemptCtx, cand, model)
	if r.exec.observer != nil {
		// A client walking away cancels the exchange; that is nobody's
		// fault but the network's own and must not demote the upstream.
		// The verdict is the same clientFault rule the breaker
		// accounting applies — one story for a disconnect.
		failed := err != nil && !clientFault(r.ctx, err)
		r.exec.observer.ObserveUpstream(cand.ID(), time.Since(started), failed)
	}
	if perm != nil {
		perm.Report(outcome)
	}
	if err == nil {
		r.servedBy = cand.ID()
		return nil
	}
	// A Retry-After hint is one credential's or one upstream's own
	// recovery schedule; it must not delay a hand-off to a different
	// candidate or to a different credential of the same upstream. The
	// hint travels only when a further attempt will actually run and
	// re-hit the same credential: stripping on the final attempt would
	// clear a hint nobody can read again. No retry can read the hint
	// then, so leaving it is free — which is also why a committed error
	// skips the lookahead entirely: the loop ends on it without ever
	// consulting anything.
	if !errors.Is(err, retry.ErrCommitted) && r.exec.policy.MaxAttempts > attempt {
		next, _ := r.target(attempt + 1)
		if next.ID() != cand.ID() || r.credentialRotates(cand) {
			err = retry.StripRetryAfter(err)
		}
	}
	return err
}

// credentialRotates reports whether this upstream will serve its next
// exchange with a credential this request has not burned: the upstream
// holds a credential ring, this request already excluded at least one
// credential, and more alive credentials remain than the walk has
// burned — so the pick is guaranteed to land on a fresh one.
func (r *run) credentialRotates(cand upstream.Upstream) bool {
	burned := r.excluded[cand.ID()]
	if len(burned) == 0 {
		return false
	}
	pool, ok := cand.(upstream.CredentialPool)
	if !ok {
		return false
	}
	return pool.AliveCredentials() > len(burned)
}

// finish renders the client response for the loop's final error and
// assembles the Result. Exactly one of the branches fires.
func (r *run) finish(err error) Result {
	job := r.job

	switch {
	case err == nil && job.Stream:
		return Result{
			Status: http.StatusOK, UpstreamID: r.servedBy, Trail: r.trail,
			Attempts: r.attempts, Retries: r.retries,
			Usage: r.usage, UsageKnown: r.usageKnown,
			Streamed: true, StreamBytes: r.streamBytes,
		}
	case err == nil:
		snap := r.success
		r.wire.RenderSuccess(job.Out, snap.status, snap.header, snap.body)
		return Result{
			Status: snap.status, UpstreamID: r.servedBy, Trail: r.trail,
			Attempts: r.attempts, Retries: r.retries,
			Usage: r.usage, UsageKnown: r.usageKnown,
			StreamBytes: r.streamBytes,
		}

	case errors.Is(err, retry.ErrCommitted):
		// The abort sequence was already written by the stream pump.
		return Result{
			Status: http.StatusOK, UpstreamID: r.servedBy, Trail: r.trail,
			Attempts: r.attempts, Retries: r.retries,
			Usage: r.usage, UsageKnown: r.usageKnown,
			Streamed: true, StreamBytes: r.streamBytes,
			Aborted: true, ClientGone: r.clientGone(),
			ErrorCode: r.gatewayCode,
		}

	case r.clientGone():
		// The client is gone: writing anything would be noise. The
		// intended status is still reported for observation.
		return Result{Status: r.intendedStatus(err), ClientGone: true, Trail: r.trail,
			Attempts: r.attempts, Retries: r.retries, Usage: r.usage, UsageKnown: r.usageKnown, StreamBytes: r.streamBytes}

	case r.terminal != nil:
		r.wire.RenderUpstreamError(job.Out, r.terminal.status, r.terminal.header, r.terminal.body)
		return Result{Status: r.terminal.status, UpstreamID: r.servedBy, Trail: r.trail,
			Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes}

	case errors.Is(err, errCircuitOpen):
		r.wire.RenderError(job.Out, http.StatusServiceUnavailable, "circuit_open",
			"all upstream candidates are unavailable")
		return Result{Status: http.StatusServiceUnavailable, Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes,
			Trail: r.trail, ErrorCode: "circuit_open"}

	case errors.Is(err, retry.ErrBudgetExhausted):
		r.exec.metrics.RetryBudgetExhausted()
		r.wire.RenderError(job.Out, http.StatusServiceUnavailable, "budget_exhausted",
			"retry budget exhausted before an upstream answered")
		return Result{Status: http.StatusServiceUnavailable, Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes,
			Trail: r.trail, ErrorCode: "budget_exhausted"}

	case r.lastFailed != nil && errors.Is(err, r.lastFailedErr):
		r.wire.RenderUpstreamError(job.Out, r.lastFailed.status, r.lastFailed.header, r.lastFailed.body)
		return Result{Status: r.lastFailed.status, UpstreamID: r.servedBy, Trail: r.trail,
			Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes}

	default:
		// The transport error's text carries the upstream URL and
		// network internals; the client gets the stable code only, the
		// details stay with the server side (correlate via request id).
		r.wire.RenderError(job.Out, http.StatusBadGateway, "upstream_unreachable",
			"upstream did not answer")
		return Result{Status: http.StatusBadGateway, Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes,
			Trail: r.trail, ErrorCode: "upstream_unreachable"}
	}
}

// intendedStatus maps the final error to the status the client would
// have received, for the case where it disconnected first.
func (r *run) intendedStatus(err error) int {
	switch {
	case r.terminal != nil:
		return r.terminal.status
	case errors.Is(err, errCircuitOpen), errors.Is(err, retry.ErrBudgetExhausted):
		return http.StatusServiceUnavailable
	case r.lastFailed != nil && errors.Is(err, r.lastFailedErr):
		return r.lastFailed.status
	default:
		return http.StatusBadGateway
	}
}

// clientGone reports whether the client request context was cancelled —
// the one failure that is nobody's fault but the network's own.
func (r *run) clientGone() bool {
	return r.ctx != nil && errors.Is(r.ctx.Err(), context.Canceled)
}

// trace records one attempt in the access trail. It is called from the
// single goroutine running the attempt loop.
func (r *run) trace(upstream string, credentialIndex, status int) {
	r.trail = append(r.trail, AttemptTrace{Upstream: upstream, CredentialIndex: credentialIndex, Status: status})
}

// errCircuitOpen marks attempts denied by the breaker; the classifier
// treats it as retryable so the next attempt may reach another
// candidate.
var errCircuitOpen = errors.New("relay: circuit open")

// readSeedHeadroom is one Read's worth of spare capacity kept past the
// pre-sized hint, so a hint that matches the body never pushes the read
// into the growth chain near its end.
const readSeedHeadroom = 512

// readBounded reads the whole body, failing closed past limit. A
// positive sizeHint (the upstream's declared Content-Length, capped at
// the limit) pre-sizes the one buffer the read needs; the unseeded
// growth chain would otherwise reallocate logarithmically and copy a
// large body several times over on the way. A missing or lying hint
// falls back to that same growth — it costs throughput, never
// correctness.
func readBounded(body io.Reader, limit int64, sizeHint int64) ([]byte, error) {
	var buf bytes.Buffer
	if sizeHint > 0 && sizeHint <= limit {
		buf.Grow(int(sizeHint) + readSeedHeadroom)
	}
	if _, err := io.Copy(&buf, io.LimitReader(body, limit+1)); err != nil {
		return nil, err
	}
	if int64(buf.Len()) > limit {
		return nil, fmt.Errorf("relay: upstream body exceeds %d bytes", limit)
	}
	return buf.Bytes(), nil
}

// contentLengthHint extracts the body size a response declares, or -1
// when it declares none or an unusable one. The value is a read
// pre-sizing hint only.
func contentLengthHint(header http.Header) int64 {
	if header == nil {
		return -1
	}
	n, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// snapshot builds an exchangeSnapshot from a response and its buffered
// body. The body comes from readBounded's io.ReadAll — private to this
// goroutine — so it is held as-is; only the header is cloned, because
// the upstream Response contract lets the adapter keep mutating it.
func snapshot(status int, header http.Header, body []byte) *exchangeSnapshot {
	return &exchangeSnapshot{status: status, header: header.Clone(), body: body}
}
