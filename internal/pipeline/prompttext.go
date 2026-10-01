/**
 * @file prompttext
 * @description The prompt rendering one consumer needs beyond the
 * token estimate: the affinity key.
 *
 * Responsibilities:
 * - Render the canonical prompt as one string for prefix matching
 * - Nothing else: the affinity index owns matching and recording;
 *   estimation keeps its own message scan
 */
package pipeline

import (
	"strings"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// PromptText renders the canonical prompt as one string: the message
// contents in order, joined with newlines. Roles and pass-through
// fields are not part of the key — an upstream prompt cache warms on
// content, and two requests sharing their leading content share the
// expensive prefill regardless of metadata. Empty contents are
// dropped so they cannot split the join.
func PromptText(req protocol.ChatRequest) string {
	parts := make([]string, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Content != "" {
			parts = append(parts, m.Content)
		}
	}
	return strings.Join(parts, "\n")
}
