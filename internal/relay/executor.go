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
 * Response-side shape (pipeline doc): retry, circuit breaking and
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
	// streamTimeout bounds a committed stream's whole body; zero means
	// the client owns the stream's lifetime outright.
	streamTimeout time.Duration
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

// WithStreamTimeout sets the ceiling of a committed stream's body: a
// stream that runs longer is terminated honestly through the error
// contract (upstream_timeout). Zero, the default, lets the client own
// the stream's lifetime.
func WithStreamTimeout(d time.Duration) Option {
	return func(e *Executor) { e.streamTimeout = d }
}

// New builds an Executor. A nil budget means retries are unbounded by
// the global cap (per-request MaxAttempts still applies); production
// assembly always passes one.
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
	// Wire presents the exchange in the client's format; nil selects
	// the canonical wire (openai-chat, byte passthrough).
	Wire protocol.Wire
	// Out is the client response writer.
	Out http.ResponseWriter
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

	success       *exchangeSnapshot
	lastFailed    *exchangeSnapshot
	lastFailedErr *retry.StatusError
	terminal      *exchangeSnapshot
	streamBytes   int64
	// gatewayCode records the gateway-originated failure code of the
	// branch that will render the final error; a mid-stream abort
	// records its in-stream code instead.
	gatewayCode string
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
	r := &run{exec: e, job: job, ctx: ctx, wire: wire}
	err := retry.Execute(ctx, e.policy, e.budget, e.classifier, nil, r.attempt)
	return r.finish(err)
}

// attempt runs one upstream attempt: breaker grant, exchange, outcome
// report. It is the AttemptFunc of the retry loop.
func (r *run) attempt(attemptCtx context.Context, attempt int) error {
	r.attempts = attempt
	if attempt > 1 {
		r.retries = attempt - 1
		r.exec.metrics.RetryScheduled(r.job.Candidates[min(attempt, len(r.job.Candidates))-1].ID())
		prev := r.job.Candidates[min(attempt-1, len(r.job.Candidates))-1]
		curr := r.job.Candidates[min(attempt, len(r.job.Candidates))-1]
		if prev.ID() != curr.ID() {
			r.exec.metrics.Failover(prev.ID(), curr.ID())
		}
	}
	cand := r.job.Candidates[min(attempt, len(r.job.Candidates))-1]

	var perm circuit.Permission
	if r.exec.breaker != nil {
		p, ok := r.exec.breaker.Allow(attemptCtx, cand.ID())
		if !ok {
			return fmt.Errorf("relay: circuit open for upstream %s: %w", cand.ID(), errCircuitOpen)
		}
		perm = p
	}

	started := time.Now()
	outcome, err := r.exchange(attemptCtx, cand)
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
	}
	return err
}

// finish renders the client response for the loop's final error and
// assembles the Result. Exactly one of the branches fires.
func (r *run) finish(err error) Result {
	job := r.job

	switch {
	case err == nil && job.Stream:
		return Result{
			Status: http.StatusOK, UpstreamID: r.servedBy,
			Attempts: r.attempts, Retries: r.retries,
			Usage: r.usage, UsageKnown: r.usageKnown,
			Streamed: true, StreamBytes: r.streamBytes,
		}
	case err == nil:
		snap := r.success
		r.wire.RenderSuccess(job.Out, snap.status, snap.header, snap.body)
		return Result{
			Status: snap.status, UpstreamID: r.servedBy,
			Attempts: r.attempts, Retries: r.retries,
			Usage: r.usage, UsageKnown: r.usageKnown,
			StreamBytes: r.streamBytes,
		}

	case errors.Is(err, retry.ErrCommitted):
		// The abort sequence was already written by the stream pump.
		return Result{
			Status: http.StatusOK, UpstreamID: r.servedBy,
			Attempts: r.attempts, Retries: r.retries,
			Usage: r.usage, UsageKnown: r.usageKnown,
			Streamed: true, StreamBytes: r.streamBytes,
			Aborted: true, ClientGone: r.clientGone(),
			ErrorCode: r.gatewayCode,
		}

	case r.clientGone():
		// The client is gone: writing anything would be noise. The
		// intended status is still reported for observation.
		return Result{Status: r.intendedStatus(err), ClientGone: true,
			Attempts: r.attempts, Retries: r.retries, Usage: r.usage, UsageKnown: r.usageKnown, StreamBytes: r.streamBytes}

	case r.terminal != nil:
		r.wire.RenderUpstreamError(job.Out, r.terminal.status, r.terminal.header, r.terminal.body)
		return Result{Status: r.terminal.status, UpstreamID: r.servedBy,
			Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes}

	case errors.Is(err, errCircuitOpen):
		r.wire.RenderError(job.Out, http.StatusServiceUnavailable, "circuit_open",
			"all upstream candidates are unavailable")
		return Result{Status: http.StatusServiceUnavailable, Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes,
			ErrorCode: "circuit_open"}

	case errors.Is(err, retry.ErrBudgetExhausted):
		r.exec.metrics.RetryBudgetExhausted()
		r.wire.RenderError(job.Out, http.StatusServiceUnavailable, "budget_exhausted",
			"retry budget exhausted before an upstream answered")
		return Result{Status: http.StatusServiceUnavailable, Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes,
			ErrorCode: "budget_exhausted"}

	case r.lastFailed != nil && errors.Is(err, r.lastFailedErr):
		r.wire.RenderUpstreamError(job.Out, r.lastFailed.status, r.lastFailed.header, r.lastFailed.body)
		return Result{Status: r.lastFailed.status, UpstreamID: r.servedBy,
			Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes}

	default:
		r.wire.RenderError(job.Out, http.StatusBadGateway, "upstream_unreachable",
			"upstream did not answer: "+err.Error())
		return Result{Status: http.StatusBadGateway, Attempts: r.attempts, Retries: r.retries, StreamBytes: r.streamBytes,
			ErrorCode: "upstream_unreachable"}
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

// errCircuitOpen marks attempts denied by the breaker; the classifier
// treats it as retryable so the next attempt may reach another
// candidate.
var errCircuitOpen = errors.New("relay: circuit open")

// readBounded reads the whole body, failing closed past limit.
func readBounded(body io.Reader, limit int64) ([]byte, error) {
	content, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > limit {
		return nil, fmt.Errorf("relay: upstream body exceeds %d bytes", limit)
	}
	return content, nil
}

// snapshot builds an exchangeSnapshot from a response and its buffered
// body; the header is cloned so later mutation cannot race the render.
func snapshot(status int, header http.Header, body []byte) *exchangeSnapshot {
	return &exchangeSnapshot{status: status, header: header.Clone(), body: bytes.Clone(body)}
}
