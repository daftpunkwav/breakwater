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
 *   package, client rendering in render.go
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
	resp, err := cand.Forward(attemptCtx, upstream.Request{
		Model:  r.job.Model,
		Stream: r.job.Stream,
		Body:   r.job.Body,
	})
	if err != nil {
		return transportOutcome(r.ctx, err), err
	}
	if r.job.Stream {
		return r.exchangeStream(attemptCtx, cand, resp)
	}
	return r.exchangeBuffered(cand, resp)
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
		return r.stashUpstreamError(resp, body)
	}

	r.success = snapshot(resp.StatusCode, resp.Header, body)
	if usage, ok := protocol.ParseUsage(body); ok {
		r.usage, r.usageKnown = usage, true
	}
	return circuit.OutcomeSuccess, nil
}

// exchangeStream handles a streaming exchange. The commit point is the
// first byte written to the client: before it, failures loop back as
// retryable; after it, any failure terminates through the SSE error
// event contract and is reported as committed (no transparent retry).
func (r *run) exchangeStream(attemptCtx context.Context, cand upstream.Upstream, resp *upstream.Response) (circuit.Outcome, error) {
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, readErr := readBounded(resp.Body, maxResponseBytes)
		_ = resp.Body.Close()
		if readErr != nil {
			return circuit.OutcomeServerFault, fmt.Errorf("relay: upstream %s read body: %w", cand.ID(), readErr)
		}
		return r.stashUpstreamError(resp, body)
	}

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

	usage, usageKnown, pumpErr := pumpStream(r.job.Out, resp.Body)
	_ = resp.Body.Close()
	if usageKnown {
		r.usage, r.usageKnown = usage, true
	}

	if pumpErr == nil {
		return circuit.OutcomeSuccess, nil
	}

	// Mid-stream failure: honest termination per the frozen contract —
	// one error event, then [DONE]; chunks already sent stay sent.
	r.aborted = true
	if r.clientGone() {
		return circuit.OutcomeClientFault, fmt.Errorf("%w: %w", retry.ErrCommitted, pumpErr)
	}
	code := abortCode(pumpErr)
	_ = protocol.WriteAbort(r.job.Out, code, "upstream stream failed mid-flight: "+pumpErr.Error())
	return circuit.OutcomeServerFault, fmt.Errorf("%w: %w", retry.ErrCommitted, pumpErr)
}

// stashUpstreamError files a completed error exchange: retryable
// statuses wait as the passthrough candidate of the final attempt,
// client faults become the terminal passthrough immediately.
func (r *run) stashUpstreamError(resp *upstream.Response, body []byte) (circuit.Outcome, error) {
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

// abortCode maps a mid-stream failure to its frozen in-stream error
// code.
func abortCode(err error) protocol.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return protocol.CodeUpstreamTimeout
	}
	return protocol.CodeUpstreamReset
}
