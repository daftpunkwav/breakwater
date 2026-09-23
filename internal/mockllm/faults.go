/**
 * @file faults
 * @description Per-request fault injection directives for the mock upstream.
 *
 * Responsibilities:
 * - Parse and validate the X-Mockllm-* header interface
 * - Nothing else: response rendering lives in completion.go, routing in
 *   handler.go
 *
 * Directives are strict: an unknown value or a malformed number is a
 * client error (HTTP 400), never silently ignored, so experiments fail
 * loudly instead of producing meaningless data.
 */
package mockllm

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// StreamMode controls how a streaming response behaves.
type StreamMode string

const (
	// StreamModeNormal emits all chunks then closes cleanly with [DONE].
	StreamModeNormal StreamMode = "normal"
	// StreamModeSlow delays between chunks.
	StreamModeSlow StreamMode = "slow"
	// StreamModeAbort emits a few chunks then terminates the connection
	// without [DONE], simulating a mid-stream upstream failure.
	StreamModeAbort StreamMode = "abort"
)

// Fault injection request headers.
const (
	headerDelay         = "X-Mockllm-Delay-Ms"
	headerStatus        = "X-Mockllm-Status"
	headerStreamMode    = "X-Mockllm-Stream-Mode"
	headerChunkDelay    = "X-Mockllm-Chunk-Delay-Ms"
	headerOmitUsage     = "X-Mockllm-Omit-Usage"
	headerCompletionTok = "X-Mockllm-Completion-Tokens"
)

// defaultChunkDelay applies when slow mode is requested without an
// explicit inter-chunk delay.
const defaultChunkDelay = 100 * time.Millisecond

// Faults carries the fault injection directives of one request.
type Faults struct {
	// Delay is injected before the first response byte.
	Delay time.Duration
	// Status overrides the response with an error status (400..599).
	Status int
	// StreamMode applies to streaming responses only.
	StreamMode StreamMode
	// ChunkDelay applies between chunks in slow mode.
	ChunkDelay time.Duration
	// OmitUsage drops the usage field (settlement-without-usage runs).
	OmitUsage bool
	// CompletionTokens sizes the generated completion; zero means default.
	CompletionTokens int
}

// completionTokens resolves the requested completion length.
func (f Faults) completionTokens() int {
	if f.CompletionTokens > 0 {
		return f.CompletionTokens
	}
	return defaultCompletionTokens
}

// abortChunkIndex returns the content chunk index after which the stream
// is terminated abruptly, or -1 when the mode is not abort.
func (f Faults) abortChunkIndex(total int) int {
	if f.StreamMode != StreamModeAbort || total <= 0 {
		return -1
	}
	n := min(2, total)
	return n - 1
}

// parseFaults reads and validates directives from request headers.
func parseFaults(header http.Header) (Faults, error) {
	f := Faults{StreamMode: StreamModeNormal}

	delay, err := headerDuration(header, headerDelay)
	if err != nil {
		return f, err
	}
	f.Delay = delay

	chunkDelay, err := headerDuration(header, headerChunkDelay)
	if err != nil {
		return f, err
	}
	f.ChunkDelay = chunkDelay

	if raw := strings.TrimSpace(header.Get(headerStatus)); raw != "" {
		status, convErr := strconv.Atoi(raw)
		if convErr != nil {
			return f, fmt.Errorf("parse %s: %w", headerStatus, convErr)
		}
		if status < 400 || status > 599 {
			return f, fmt.Errorf("%s must be an error status (400..599), got %d", headerStatus, status)
		}
		f.Status = status
	}

	if raw := strings.TrimSpace(header.Get(headerStreamMode)); raw != "" {
		mode := StreamMode(raw)
		switch mode {
		case StreamModeNormal, StreamModeSlow, StreamModeAbort:
			f.StreamMode = mode
		default:
			return f, fmt.Errorf("%s must be %q, %q or %q, got %q",
				headerStreamMode, StreamModeNormal, StreamModeSlow, StreamModeAbort, raw)
		}
	}

	omit, err := headerBool(header, headerOmitUsage)
	if err != nil {
		return f, err
	}
	f.OmitUsage = omit

	if raw := strings.TrimSpace(header.Get(headerCompletionTok)); raw != "" {
		tokens, convErr := strconv.Atoi(raw)
		if convErr != nil {
			return f, fmt.Errorf("parse %s: %w", headerCompletionTok, convErr)
		}
		if tokens <= 0 || tokens > maxCompletionTokens {
			return f, fmt.Errorf("%s must be within 1..%d, got %d", headerCompletionTok, maxCompletionTokens, tokens)
		}
		f.CompletionTokens = tokens
	}

	if f.StreamMode == StreamModeSlow && f.ChunkDelay == 0 {
		f.ChunkDelay = defaultChunkDelay
	}
	return f, nil
}

func headerDuration(header http.Header, name string) (time.Duration, error) {
	raw := strings.TrimSpace(header.Get(name))
	if raw == "" {
		return 0, nil
	}
	ms, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if ms < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %d", name, ms)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func headerBool(header http.Header, name string) (bool, error) {
	raw := strings.TrimSpace(header.Get(name))
	if raw == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}
	return b, nil
}
