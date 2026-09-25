/**
 * @file modelmap
 * @description Client-to-provider model rewrites for one upstream.
 *
 * Responsibilities:
 * - Parse the configured "client=real" model bindings into a lookup
 * - Rewrite the top-level "model" member of a canonical request body
 * - Nothing else: which names a client may use belongs to the router;
 *   the rewrite itself is a provider quirk and therefore adapter work
 *
 * The rewrite is what lets one client-facing model name span providers
 * with different naming: the gateway routes on the client name, and
 * each adapter forwards the name its provider actually serves. When a
 * failover crosses upstreams, each attempt rewrites to its own target,
 * which is exactly a model switch inside one request.
 */
package upstream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// ParseModelMap extracts client-to-provider rewrites from a configured
// model list. A plain entry forwards the client name verbatim; a
// "client=real" entry forwards the provider-real name under the client
// name. Malformed entries are ignored: configuration validation
// rejects them before an adapter is ever built, and the wildcard
// "*" never carries a rewrite.
func ParseModelMap(models []string) map[string]string {
	rewrites := make(map[string]string)
	for _, entry := range models {
		client, real, ok := strings.Cut(entry, "=")
		if !ok || client == "" || real == "" || client == "*" {
			continue
		}
		rewrites[client] = real
	}
	return rewrites
}

// rewriteModelBody replaces the top-level "model" member of a JSON
// object. Every other member keeps its value exactly; the re-encode
// only reorders keys (Go maps serialize sorted) and disables HTML
// escaping so message text survives byte-for-byte in value. Bodies
// without a rewrite need no round-trip and keep the verbatim
// passthrough.
func rewriteModelBody(body []byte, model string) ([]byte, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, fmt.Errorf("upstream: rewrite model: %w", err)
	}
	name, err := json.Marshal(model)
	if err != nil {
		return nil, fmt.Errorf("upstream: encode model name: %w", err)
	}
	top["model"] = name
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(top); err != nil {
		return nil, fmt.Errorf("upstream: rewrite model: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
