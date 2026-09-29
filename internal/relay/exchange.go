/**
 * @file exchange
 * @description One upstream attempt for the relay: the buffered mode
 * for non-streaming requests, the streaming mode for SSE.
 *
 * Responsibilities:
 * - Classify one completed exchange into success / retryable failure /
 *   terminal passthrough, and stash what the finish stage needs
 * - Streaming: commit the reply headers, pump chunks with passive usage
 *   scraping, and terminate honestly through the error event contract
 *   once bytes have reached the client
 * - Nothing else: the attempt loop and budgets live in the retry
 *   package, client rendering with the protocol wires
 */
package relay

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// exchange performs one upstream attempt and returns the breaker
// outcome alongside the loop error. The model is the batch's
// client-facing name — the fallback plan may have moved it off the
// Job's original model.
func (r *run) exchange(attemptCtx context.Context, cand upstream.Upstream, model string) (circuit.Outcome, error) {
	req := upstream.Request{
		Model: model,
		// The body still carries the Job's primary model name (it is the
		// client's own bytes); the adapter rewrites it when this batch's
		// model differs — the mechanism that makes a fallback hop a real
		// model switch.
		BodyModel: r.job.Model,
		Stream:    r.job.Stream,
		Body:      r.job.Body,
		RequestID: r.job.RequestID,
		// Credentials this request already burned on this upstream: the
		// adapter serves the exchange with a different one.
		ExcludedCredentials: r.excluded[cand.ID()],
	}
	if !r.job.Stream {
		resp, err := cand.Forward(attemptCtx, req)
		if err != nil {
			r.trace(cand.ID(), -1, 0)
			return transportOutcome(r.ctx, err), err
		}
		return r.exchangeBuffered(cand, resp)
	}

	// A streamed reply may legitimately run for minutes: the attempt
	// timeout must not bound its body. It bounds time-to-first-byte
	// instead (a timer on the lease); the body is bound by the client
	// context plus the optional stream ceiling. The overall deadline
	// keeps governing the attempt loop only — once a stream commits,
	// the client owns its lifetime.
	lease, fwdCtx := r.beginStream()
	resp, err := cand.Forward(fwdCtx, req)
	if err != nil {
		r.trace(cand.ID(), -1, 0)
		// end() is what stops the timers, so both flags are read after
		// it: a timer that fires between the read and the stop would
		// otherwise be read as not having fired, and the gateway's own
		// cut would be charged to the upstream.
		lease.end()
		if lease.ttftFired.Load() {
			// Classified as a timeout so the loop may retry or fail
			// over to the next candidate.
			return circuit.OutcomeServerFault, r.ttftTimeoutError()
		}
		if lease.ceilingFired.Load() {
			// The gateway's own stream ceiling cut the forward before
			// any byte arrived: a policy execution, not upstream
			// evidence, exactly like the post-commit case below. The
			// error wraps a deadline rather than the cancellation the
			// forward context actually produced, because the next
			// candidate gets a fresh ceiling and failing over to it is
			// a real option.
			return circuit.OutcomeGatewayTerminated, r.ceilingTimeoutError()
		}
		return transportOutcome(r.ctx, err), err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Read the error body while the lease is still alive: end
		// cancels the forward context, and a cancelled request context
		// takes the response body's readability with it — the upstream
		// error status would be lost to a generic read failure.
		body, readErr := readBounded(resp.Body, maxResponseBytes)
		_ = resp.Body.Close()
		lease.end()
		if readErr != nil {
			return circuit.OutcomeServerFault, fmt.Errorf("relay: upstream %s read body: %w", cand.ID(), readErr)
		}
		return r.stashUpstreamError(cand, resp, body)
	}
	lease.ttftPassed()
	if lease.ttftFired.Load() {
		// The timer fired as the headers landed: the forward context is
		// already cancelled, so the body dies on its first read. Fail
		// the attempt as the retryable timeout it is instead of
		// committing a stream that cannot deliver.
		_ = resp.Body.Close()
		lease.end()
		return circuit.OutcomeServerFault, r.ttftTimeoutError()
	}
	// First byte of a streamed reply: publish the TTFT the way the
	// observation surface expects it (buffered replies stay on the
	// end-to-end duration histogram).
	r.exec.metrics.ObserveTTFT(cand.ID(), time.Since(r.startedAt).Seconds())
	return r.exchangeStream(cand, resp, lease, model)
}

// exchangeBuffered handles a non-streaming exchange: the body is
// buffered before anything reaches the client, so any failure here is
// still retryable in principle.
func (r *run) exchangeBuffered(cand upstream.Upstream, resp *upstream.Response) (circuit.Outcome, error) {
	body, readErr := readBounded(resp.Body, maxResponseBytes)
	_ = resp.Body.Close()
	if readErr != nil {
		return circuit.OutcomeServerFault, fmt.Errorf("relay: upstream %s read body: %w", cand.ID(), readErr)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return r.stashUpstreamError(cand, resp, body)
	}

	r.success = snapshot(resp.StatusCode, resp.Header, body)
	if usage, ok := protocol.ParseUsage(body); ok {
		r.usage, r.usageKnown = usage, true
	}
	r.trace(cand.ID(), resp.CredentialIndex, resp.StatusCode)
	return circuit.OutcomeSuccess, nil
}

// exchangeStream handles a streaming exchange whose headers already
// arrived (the time-to-first-byte budget is spent). The commit point is
// the first byte written to the client: before it, failures loop back
// as retryable; after it, any failure terminates through the client
// format's honest termination and is reported as committed (no
// transparent retry).
func (r *run) exchangeStream(cand upstream.Upstream, resp *upstream.Response, lease *streamLease, model string) (circuit.Outcome, error) {
	// Commit: from here on the loop must never see a plain error. The
	// forwarded headers carry upstream-controlled values, so each is
	// set only when it is a legal header field value — the same rule
	// the buffered passthrough applies (internal/protocol). An unusable
	// Content-Type falls back to the SSE default.
	header := r.job.Out.Header()
	if ct := resp.Header.Get("Content-Type"); ct != "" && protocol.ValidHeaderValue(ct) {
		header.Set("Content-Type", ct)
	} else {
		header.Set("Content-Type", "text/event-stream")
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "" && protocol.ValidHeaderValue(cc) {
		header.Set("Cache-Control", cc)
	}
	r.job.Out.WriteHeader(http.StatusOK)
	if flusher, ok := r.job.Out.(http.Flusher); ok {
		flusher.Flush()
	}
	r.streamed = true
	// The delivered reply belongs to this candidate from the commit on —
	// including its aborted tail.
	r.servedBy = cand.ID()
	r.trace(cand.ID(), resp.CredentialIndex, resp.StatusCode)
	// From the commit on, upstream silence is bounded by the idle
	// watchdog (when configured): every body read re-arms it, so a
	// stream only dies when its upstream has stopped speaking.
	lease.armIdle(r.exec.streamIdleTimeout)
	body := &activityReader{reader: resp.Body, activity: lease.activity}

	var usage protocol.Usage
	var usageKnown bool
	var streamBytes int64
	var pumpErr error
	transcoder := r.wire.Stream()
	if transcoder != nil {
		usage, usageKnown, streamBytes, pumpErr = pumpTranscoded(r.job.Out, body, transcoder, model)
	} else {
		usage, usageKnown, streamBytes, pumpErr = pumpStream(r.job.Out, body)
	}
	_ = resp.Body.Close()
	if usageKnown {
		r.usage, r.usageKnown = usage, true
	}
	r.streamBytes = streamBytes

	if pumpErr == nil {
		lease.end()
		return circuit.OutcomeSuccess, nil
	}
	defer lease.end()

	// Mid-stream failure: honest termination —
	// one error frame in the client's format; chunks already sent stay
	// sent. The same transcoder instance closes the stream it opened:
	// a fresh one would carry a new object id no client frame introduced.
	if r.clientGone() {
		return circuit.OutcomeClientFault, fmt.Errorf("%w: %w", retry.ErrCommitted, pumpErr)
	}
	// The gateway's own ceiling or idle watchdog cut this stream: a
	// policy execution, not upstream evidence, and the same timeout the
	// client sees in the error frame either way.
	gatewayCut := lease.ceilingFired.Load() || lease.idleFired.Load()
	code := abortCode(pumpErr)
	if gatewayCut {
		code = protocol.CodeUpstreamTimeout
	}
	// The failure taxonomy reads the same code the client saw in the
	// stream's error frame.
	r.gatewayCode = string(code)
	// The abort frame stays code-level, never the raw pump error: a
	// mid-stream read failure is a net error whose text carries the
	// gateway's and the upstream's internal addresses, and the client
	// surface must not reflect network internals — the same rule the
	// transport-error branch of finish() holds. The classification the
	// code carries (reset vs timeout) is the client-visible truth.
	message := "upstream stream failed mid-flight"
	if code == protocol.CodeUpstreamTimeout {
		message = "upstream stream timed out mid-flight"
	}
	if transcoder != nil {
		_ = transcoder.Abort(r.job.Out, code, message)
	} else {
		_ = protocol.WriteAbort(r.job.Out, code, message)
	}
	if gatewayCut {
		// A healthy but slow upstream must not be voted out of rotation
		// by its own configuration; the breaker hears the neutral
		// verdict.
		return circuit.OutcomeGatewayTerminated, fmt.Errorf("%w: %w", retry.ErrCommitted, pumpErr)
	}
	return circuit.OutcomeServerFault, fmt.Errorf("%w: %w", retry.ErrCommitted, pumpErr)
}

// stashUpstreamError files a completed error exchange: retryable
// statuses wait as the passthrough candidate of the final attempt,
// client faults become the terminal passthrough immediately. The
// serving upstream is recorded: error passthroughs must still carry
// their upstream dimension for observation and negative caching.
//
// A credential-class failure (rate limit, dead credential, exhausted
// quota) convicts the credential that served the exchange, not the
// upstream: it is excluded for the rest of this request, and while
// another credential remains alive the exchange hands the request to
// it instead of ending the request. When none is left — or the
// upstream holds no ring — the verdict falls to the retryability
// table exactly as before.
func (r *run) stashUpstreamError(cand upstream.Upstream, resp *upstream.Response, body []byte) (circuit.Outcome, error) {
	r.servedBy = cand.ID()
	r.trace(cand.ID(), resp.CredentialIndex, resp.StatusCode)
	statusErr := retry.NewStatusError(resp.StatusCode)
	// The upstream's own wait request rides the error for the attempt
	// loop; the loop decides where it applies (same-credential
	// retries).
	statusErr.RetryAfter = retry.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	// A fatal condition is reported once per observation, whether or
	// not the exchange counts as retryable: quota exhaustion arrives
	// as a 429 and is retryable by the table, but it is still proof
	// the credential's budget is gone.
	reason := fatalUpstreamReason(resp.StatusCode, body)
	if reason != "" && r.exec.fatalHook != nil {
		r.exec.fatalHook(cand.ID(), resp.CredentialIndex, reason)
	}
	if resp.CredentialIndex >= 0 && (reason != "" || resp.StatusCode == http.StatusTooManyRequests) {
		r.excludeCredential(cand.ID(), resp.CredentialIndex)
	}
	retryable := r.exec.classifier.Retryable(statusErr)
	if reason != "" && credentialAlive(cand) > 0 {
		// The verdict is per-credential: while another credential
		// remains, the loop must run its next attempt on this upstream.
		// The static table calls 401-class statuses terminal, so the
		// request's own classifier layer carries the override.
		retryable = true
		r.rotated = statusErr
	}
	if retryable {
		r.lastFailed = snapshot(resp.StatusCode, resp.Header, body)
		r.lastFailedErr = statusErr
		return circuit.OutcomeServerFault, statusErr
	}
	r.terminal = snapshot(resp.StatusCode, resp.Header, body)
	return circuit.OutcomeClientFault, statusErr
}

// excludeCredential burns the credential a failed exchange used, so
// this request's next attempt on the same upstream picks a different
// one. The list is the request's own verdict; a concurrent request
// keeps using the credential until its own evidence convicts it.
func (r *run) excludeCredential(upstreamID string, index int) {
	if r.excluded == nil {
		r.excluded = map[string][]int{}
	}
	for _, got := range r.excluded[upstreamID] {
		if got == index {
			return
		}
	}
	r.excluded[upstreamID] = append(r.excluded[upstreamID], index)
}

// credentialAlive reports how many credentials the upstream can still
// serve with; zero for adapters without a credential ring, so a fatal
// exchange keeps its terminal verdict there.
func credentialAlive(cand upstream.Upstream) int {
	if pool, ok := cand.(upstream.CredentialPool); ok {
		return pool.AliveCredentials()
	}
	return 0
}

// clientFault reports whether a failed exchange was the client's own
// doing: a cancel-flavored error while the client request context is
// canceled. Breaker accounting and the routing observer apply this one
// verdict, so a disconnect is judged identically wherever it surfaces.
func clientFault(requestCtx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) && requestCtx != nil &&
		requestCtx.Err() != nil && errors.Is(requestCtx.Err(), context.Canceled)
}

// transportOutcome maps a failed exchange to the breaker accounting:
// a client disconnect is nobody's fault but the network's; every other
// failed exchange — timeout, reset, refused — indicts the upstream.
func transportOutcome(requestCtx context.Context, err error) circuit.Outcome {
	if clientFault(requestCtx, err) {
		return circuit.OutcomeClientFault
	}
	return circuit.OutcomeServerFault
}

// ttftTimeoutError renders an expired time-to-first-byte budget as the
// retryable timeout the attempt loop understands.
func (r *run) ttftTimeoutError() error {
	return fmt.Errorf("time-to-first-byte exceeded %v: %w", r.exec.policy.AttemptTimeout, context.DeadlineExceeded)
}

// ceilingTimeoutError renders a stream ceiling that fired before the
// first byte as the retryable timeout the attempt loop understands. A
// deadline rather than a cancellation, so the loop may fail over to a
// candidate whose own ceiling has not expired yet.
func (r *run) ceilingTimeoutError() error {
	return fmt.Errorf("stream ceiling %v exceeded before the first byte: %w", r.exec.streamTimeout, context.DeadlineExceeded)
}

// abortCode maps a mid-stream failure to its in-stream error code.
func abortCode(err error) protocol.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return protocol.CodeUpstreamTimeout
	}
	return protocol.CodeUpstreamReset
}
