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
 *   once bytes have reached the client (invariant I6)
 * - Nothing else: the attempt loop and budgets live in the retry
 *   package, client rendering with the protocol wires
 */
package relay

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// exchange performs one upstream attempt and returns the breaker
// outcome alongside the loop error.
func (r *run) exchange(attemptCtx context.Context, cand upstream.Upstream) (circuit.Outcome, error) {
	req := upstream.Request{
		Model:     r.job.Model,
		Stream:    r.job.Stream,
		Body:      r.job.Body,
		RequestID: r.job.RequestID,
	}
	if !r.job.Stream {
		resp, err := cand.Forward(attemptCtx, req)
		if err != nil {
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
		lease.end()
		if lease.ttftFired.Load() {
			// Classified as a timeout so the loop may retry or fail
			// over to the next candidate.
			return circuit.OutcomeServerFault, r.ttftTimeoutError()
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
	return r.exchangeStream(cand, resp, lease)
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
	return circuit.OutcomeSuccess, nil
}

// exchangeStream handles a streaming exchange whose headers already
// arrived (the time-to-first-byte budget is spent). The commit point is
// the first byte written to the client: before it, failures loop back
// as retryable; after it, any failure terminates through the client
// format's honest termination and is reported as committed (no
// transparent retry).
func (r *run) exchangeStream(cand upstream.Upstream, resp *upstream.Response, lease *streamLease) (circuit.Outcome, error) {
	// Commit: from here on the loop must never see a plain error.
	header := r.job.Out.Header()
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		header.Set("Content-Type", ct)
	} else {
		header.Set("Content-Type", "text/event-stream")
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "" {
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

	var usage protocol.Usage
	var usageKnown bool
	var streamBytes int64
	var pumpErr error
	transcoder := r.wire.Stream()
	if transcoder != nil {
		usage, usageKnown, streamBytes, pumpErr = pumpTranscoded(r.job.Out, resp.Body, transcoder, r.job.Model)
	} else {
		usage, usageKnown, streamBytes, pumpErr = pumpStream(r.job.Out, resp.Body)
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

	// Mid-stream failure: honest termination per the frozen contract —
	// one error frame in the client's format; chunks already sent stay
	// sent. The same transcoder instance closes the stream it opened:
	// a fresh one would carry a new object id no client frame introduced.
	if r.clientGone() {
		return circuit.OutcomeClientFault, fmt.Errorf("%w: %w", retry.ErrCommitted, pumpErr)
	}
	code := abortCode(pumpErr)
	if lease.ceilingFired.Load() {
		code = protocol.CodeUpstreamTimeout
	}
	message := "upstream stream failed mid-flight: " + pumpErr.Error()
	if transcoder != nil {
		_ = transcoder.Abort(r.job.Out, code, message)
	} else {
		_ = protocol.WriteAbort(r.job.Out, code, message)
	}
	return circuit.OutcomeServerFault, fmt.Errorf("%w: %w", retry.ErrCommitted, pumpErr)
}

// stashUpstreamError files a completed error exchange: retryable
// statuses wait as the passthrough candidate of the final attempt,
// client faults become the terminal passthrough immediately. The
// serving upstream is recorded: error passthroughs must still carry
// their upstream dimension for observation and negative caching.
func (r *run) stashUpstreamError(cand upstream.Upstream, resp *upstream.Response, body []byte) (circuit.Outcome, error) {
	r.servedBy = cand.ID()
	statusErr := retry.NewStatusError(resp.StatusCode)
	if r.exec.classifier.Retryable(statusErr) {
		r.lastFailed = snapshot(resp.StatusCode, resp.Header, body)
		r.lastFailedErr = statusErr
		return circuit.OutcomeServerFault, statusErr
	}
	r.terminal = snapshot(resp.StatusCode, resp.Header, body)
	return circuit.OutcomeClientFault, statusErr
}

// transportOutcome maps a failed exchange to the breaker accounting:
// a client disconnect is nobody's fault but the network's; every other
// failed exchange — timeout, reset, refused — indicts the upstream.
func transportOutcome(requestCtx context.Context, err error) circuit.Outcome {
	if errors.Is(err, context.Canceled) && requestCtx.Err() != nil &&
		errors.Is(requestCtx.Err(), context.Canceled) {
		return circuit.OutcomeClientFault
	}
	return circuit.OutcomeServerFault
}

// ttftTimeoutError renders an expired time-to-first-byte budget as the
// retryable timeout the attempt loop understands.
func (r *run) ttftTimeoutError() error {
	return fmt.Errorf("time-to-first-byte exceeded %v: %w", r.exec.policy.AttemptTimeout, context.DeadlineExceeded)
}

// abortCode maps a mid-stream failure to its frozen in-stream error
// code.
func abortCode(err error) protocol.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return protocol.CodeUpstreamTimeout
	}
	return protocol.CodeUpstreamReset
}
