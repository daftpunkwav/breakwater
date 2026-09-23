/**
 * @file completion
 * @description OpenAI-compatible response rendering for the mock upstream.
 *
 * Responsibilities:
 * - Render non-streaming JSON responses and SSE chunk streams
 * - Render the OpenAI-style error envelope
 * - Keep completion lengths fixed and deterministic so load test runs
 *   stay comparable
 *
 * One token is approximated as one whitespace-separated word; content is
 * drawn from a fixed vocabulary so byte length is deterministic.
 */
package mockllm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// defaultCompletionTokens sizes completions when the request does not pin
// a length via header.
const defaultCompletionTokens = 32

// maxCompletionTokens bounds generated completion size so a hostile
// header cannot balloon memory.
const maxCompletionTokens = 100_000

// errNoFlusher guards the streaming path: without http.Flusher support
// chunk delivery cannot be guaranteed.
var errNoFlusher = fmt.Errorf("mockllm: response writer does not support flushing")

// mockWords is cycled to build fixed-length completion content.
var mockWords = strings.Fields("the quick brown fox jumps over the lazy dog")

var completionSeq atomic.Uint64

// completionRequest is the subset of the OpenAI chat completion schema
// the mock upstream consumes.
type completionRequest struct {
	Model    string        `json:"model"`
	Stream   bool          `json:"stream"`
	Messages []chatMessage `json:"messages"`
}

// chatMessage is one conversation entry.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// completionResponse mirrors the OpenAI non-streaming response schema.
type completionResponse struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Created int64              `json:"created"`
	Model   string             `json:"model"`
	Choices []completionChoice `json:"choices"`
	Usage   *usage             `json:"usage,omitempty"`
}

// completionChoice is one non-streaming choice.
type completionChoice struct {
	Index        int     `json:"index"`
	Message      message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// message is the assistant reply envelope.
type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatChunk mirrors the OpenAI streaming chunk schema.
type chatChunk struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []chatChunkChoice `json:"choices"`
	Usage   *usage            `json:"usage,omitempty"`
}

// chatChunkChoice is one streaming choice.
type chatChunkChoice struct {
	Index        int            `json:"index"`
	Delta        chatChunkDelta `json:"delta"`
	FinishReason *string        `json:"finish_reason"`
}

// chatChunkDelta carries the incremental content of a chunk.
type chatChunkDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// usage reports token accounting.
type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// errorEnvelope mirrors the OpenAI error response schema.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

// errorBody is the OpenAI error payload.
type errorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

// promptTokens counts prompt words across all message contents.
func promptTokens(req completionRequest) int {
	n := 0
	for _, m := range req.Messages {
		n += len(strings.Fields(m.Content))
	}
	if n == 0 {
		n = 1
	}
	return n
}

// buildContent renders completion content of exactly tokens words.
func buildContent(tokens int) string {
	words := make([]string, tokens)
	for i := range words {
		words[i] = mockWords[i%len(mockWords)]
	}
	return strings.Join(words, " ")
}

func nextCompletionID() string {
	return fmt.Sprintf("chatcmpl-mock-%06d", completionSeq.Add(1))
}

// writeError renders the OpenAI-style error envelope.
func writeError(w http.ResponseWriter, status int, code, text string) {
	errType := "mock_error"
	if status >= 400 && status < 500 {
		errType = "invalid_request_error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{
		Error: errorBody{Message: text, Type: errType, Code: code},
	})
}

// writeCompletion renders a non-streaming chat completion response.
func writeCompletion(w http.ResponseWriter, req completionRequest, faults Faults) error {
	prompt := promptTokens(req)
	tokens := faults.completionTokens()
	resp := completionResponse{
		ID:      nextCompletionID(),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []completionChoice{{
			Index:        0,
			Message:      message{Role: "assistant", Content: buildContent(tokens)},
			FinishReason: "stop",
		}},
		Usage: &usage{
			PromptTokens:     prompt,
			CompletionTokens: tokens,
			TotalTokens:      prompt + tokens,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(resp)
}

// writeStream renders an SSE chunk stream honoring the injected stream
// mode. The context belongs to the incoming request so client disconnects
// cut generation short.
func writeStream(ctx context.Context, w http.ResponseWriter, req completionRequest, faults Faults) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return errNoFlusher
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	prompt := promptTokens(req)
	tokens := faults.completionTokens()
	id := nextCompletionID()
	created := time.Now().Unix()

	emit := func(payload any) error {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	roleChunk := chunkWithDelta(id, created, req.Model, chatChunkDelta{Role: "assistant"}, nil)
	if err := emit(roleChunk); err != nil {
		return err
	}

	words := strings.Split(buildContent(tokens), " ")
	for i, word := range words {
		if err := sleepContext(ctx, faults.ChunkDelay); err != nil {
			return err
		}
		delta := chatChunkDelta{Content: word}
		if i < len(words)-1 {
			delta.Content += " "
		}
		if err := emit(chunkWithDelta(id, created, req.Model, delta, nil)); err != nil {
			return err
		}
		if i == faults.abortChunkIndex(tokens) {
			// Terminate the connection without [DONE]: the client sees a
			// truncated stream, which is exactly the injected failure.
			panic(http.ErrAbortHandler)
		}
	}

	stop := "stop"
	if err := emit(chunkWithDelta(id, created, req.Model, chatChunkDelta{}, &stop)); err != nil {
		return err
	}
	if !faults.OmitUsage {
		final := chatChunk{
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   req.Model,
			Choices: []chatChunkChoice{},
			Usage: &usage{
				PromptTokens:     prompt,
				CompletionTokens: tokens,
				TotalTokens:      prompt + tokens,
			},
		}
		if err := emit(final); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprint(w, "data: [DONE]\n\n"); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// chunkWithDelta builds one content or terminal chunk.
func chunkWithDelta(id string, created int64, model string, delta chatChunkDelta, finish *string) chatChunk {
	return chatChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: created,
		Model:   model,
		Choices: []chatChunkChoice{{Index: 0, Delta: delta, FinishReason: finish}},
	}
}

// sleepContext waits for d, returning early when the context is cancelled.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
