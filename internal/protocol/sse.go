/**
 * @file sse
 * @description SSE wire codec for the gateway's streaming contract: the
 * in-stream error event and the [DONE] terminator (frozen public
 * protocol, locked by contract tests).
 *
 * Responsibilities:
 * - Encode SSE events with the exact byte layout clients rely on
 * - Terminate aborted streams honestly: exactly one error event followed
 *   by the [DONE] sentinel; chunks already sent are never replayed
 * - Nothing else: chunk payload types (OpenAI schema) land in this
 *   package with the forwarding implementation; response headers and
 *   flushing are the handler's business
 *
 * The error code set is closed. Adding a code is a protocol change and
 * must update the contract tests together with this enum.
 */
package protocol

import (
	"encoding/json"
	"fmt"
	"io"
)

// ErrorType is the fixed type marker of in-stream gateway errors.
const ErrorType = "gateway_error"

// Code enumerates the in-stream error codes of the frozen contract.
type Code string

const (
	// CodeUpstreamReset reports the upstream connection broke mid-stream.
	CodeUpstreamReset Code = "upstream_reset"
	// CodeUpstreamTimeout reports the upstream missed its deadline
	// mid-stream.
	CodeUpstreamTimeout Code = "upstream_timeout"
	// CodeBudgetExhausted reports the retry and failover budget ran out
	// before any upstream produced a usable reply.
	CodeBudgetExhausted Code = "budget_exhausted"
)

// doneSentinel terminates every stream, successful or aborted.
const doneSentinel = "data: [DONE]\n\n"

// StreamError is the envelope of the in-stream error event.
type StreamError struct {
	Error StreamErrorBody `json:"error"`
}

// StreamErrorBody is the error payload: message, fixed type, frozen code.
type StreamErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    Code   `json:"code"`
}

// NewStreamError builds a stream error with the fixed gateway type.
func NewStreamError(code Code, message string) StreamError {
	return StreamError{Error: StreamErrorBody{
		Message: message,
		Type:    ErrorType,
		Code:    code,
	}}
}

// WriteData writes one nameless SSE data event carrying a JSON payload;
// chunk streaming uses this shape.
func WriteData(w io.Writer, payload any) error {
	if err := writeEventData(w, "", payload); err != nil {
		return err
	}
	return nil
}

// WriteAbort terminates a stream after a mid-flight failure per the
// frozen contract: exactly one error event followed by the [DONE]
// sentinel, so SDKs walk their standard completion path instead of
// hanging on a half-open stream.
func WriteAbort(w io.Writer, code Code, message string) error {
	if err := writeEventData(w, "error", NewStreamError(code, message)); err != nil {
		return err
	}
	return WriteDone(w)
}

// WriteDone writes the successful stream terminator.
func WriteDone(w io.Writer) error {
	if _, err := io.WriteString(w, doneSentinel); err != nil {
		return fmt.Errorf("write done sentinel: %w", err)
	}
	return nil
}

// writeEventData encodes one SSE event: an optional event name line, one
// data line with the JSON payload, and the separating blank line.
func writeEventData(w io.Writer, event string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode sse payload: %w", err)
	}
	if event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
			return fmt.Errorf("write %s event: %w", event, err)
		}
		return nil
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return fmt.Errorf("write data event: %w", err)
	}
	return nil
}
