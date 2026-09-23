/**
 * @file handler
 * @description HTTP surface of the mock LLM upstream.
 *
 * Responsibilities:
 * - Route requests to the completion endpoint and the health probe
 * - Apply fault injection: per request via X-Mockllm-* headers, process
 *   wide via Options (default delay, error rate)
 *
 * The mock is the fault injector for the gateway's experiments; it must
 * stay OpenAI-compatible on the happy path so the gateway under test
 * needs no special-casing.
 */
package mockllm

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"time"
)

// maxRequestBytes caps the accepted request body size.
const maxRequestBytes = 1 << 20

// Options configures process-wide fault behavior.
type Options struct {
	// DefaultDelay is injected into every request; per-request delay adds
	// on top.
	DefaultDelay time.Duration
	// ErrorRate is the fraction of requests answered with 500 (0..1).
	// Used for chaos runs that cannot rewrite client headers.
	ErrorRate float64
}

// Handler is the HTTP surface of the mock upstream.
type Handler struct {
	opts Options
}

// New builds a Handler, clamping invalid option values into safe ranges.
func New(opts Options) *Handler {
	if opts.DefaultDelay < 0 {
		opts.DefaultDelay = 0
	}
	if opts.ErrorRate < 0 {
		opts.ErrorRate = 0
	}
	if opts.ErrorRate > 1 {
		opts.ErrorRate = 1
	}
	return &Handler{opts: opts}
}

// ServeHTTP routes one request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "health probe requires GET")
			return
		}
		handleHealth(w, r)
	case "/v1/chat/completions":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "completions require POST")
			return
		}
		h.handleCompletion(w, r)
	default:
		writeError(w, http.StatusNotFound, "not_found", "unknown path: "+r.URL.Path)
	}
}

// handleCompletion applies fault directives and renders the response.
func (h *Handler) handleCompletion(w http.ResponseWriter, r *http.Request) {
	faults, err := parseFaults(r.Header)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_fault_directive", err.Error())
		return
	}

	if err := sleepContext(r.Context(), h.opts.DefaultDelay+faults.Delay); err != nil {
		// Client disconnected during the injected delay.
		return
	}

	if h.shouldFail() {
		writeError(w, http.StatusInternalServerError, "injected_failure", "mock upstream injected failure")
		return
	}
	if faults.Status != 0 {
		writeError(w, faults.Status, "injected_status", "mock upstream injected status")
		return
	}

	body := http.MaxBytesReader(w, r.Body, maxRequestBytes)
	var req completionRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the accepted size")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body")
		return
	}
	if req.Model == "" || len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "model and messages are required")
		return
	}

	if req.Stream {
		_ = writeStream(r.Context(), w, req, faults)
		return
	}
	_ = writeCompletion(w, req, faults)
}

// shouldFail rolls the process-wide error rate.
func (h *Handler) shouldFail() bool {
	return h.opts.ErrorRate > 0 && rand.Float64() < h.opts.ErrorRate
}
