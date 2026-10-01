/**
 * @file prompttext_test
 * @description The affinity key's rendering: content in message
 * order, empty contents dropped, one canonical join.
 */
package pipeline

import (
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

func TestPromptTextJoinsMessageContents(t *testing.T) {
	req := protocol.ChatRequest{Messages: []protocol.ChatMessage{
		{Role: "system", Content: "be brief"},
		{Role: "tool", Content: ""}, // dropped, no placeholder split
		{Role: "user", Content: "hello"},
	}}
	if got, want := PromptText(req), "be brief\nhello"; got != want {
		t.Fatalf("PromptText = %q, want %q", got, want)
	}
	if got := PromptText(protocol.ChatRequest{}); got != "" {
		t.Fatalf("PromptText of no messages = %q, want empty", got)
	}
}
