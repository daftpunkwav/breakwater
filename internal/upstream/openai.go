/**
 * @file openai
 * @description Adapter for OpenAI-compatible chat completion endpoints:
 * the mock upstream, OpenAI and DeepSeek all speak this wire format.
 *
 * Responsibilities:
 * - Translate one neutral Forward call into exactly one HTTP exchange
 * - Nothing else: retry, circuit breaking and failover belong to the
 *   layers above; response body handling belongs to the caller
 *
 * The result convention of the port holds here: any completed exchange
 * — including 4xx and 5xx — is a non-nil Response; a nil Response means
 * the exchange did not complete.
 */
package upstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// completionPath is appended to the endpoint base of every adapter
// target. The mock upstream and both real providers serve chat
// completions under this path.
const completionPath = "/v1/chat/completions"

// OpenAIConfig configures one OpenAI-compatible upstream.
type OpenAIConfig struct {
	// ID names the upstream for routing and breaker state.
	ID string
	// BaseURL is the scheme and host of the provider, without a
	// trailing slash (e.g. http://127.0.0.1:8090).
	BaseURL string
	// APIKeys are the bearer tokens rotated behind the upstream, in
	// ring order; empty means the upstream needs no credential (the
	// mock upstream). The config layer merges api_key and api_keys
	// into this list.
	APIKeys []string
	// ProbeURL is the health endpoint consulted by Probe.
	ProbeURL string
	// ModelMap rewrites client-facing model names to the names this
	// provider actually serves; requests for an unmapped name pass
	// through unchanged. See ParseModelMap.
	ModelMap map[string]string
	// Transport overrides the shared tuned transport; nil selects the
	// package default. Tests inject single-purpose transports here.
	Transport *http.Transport
}

// OpenAI is one OpenAI-compatible upstream adapter.
type OpenAI struct {
	id       string
	baseURL  string
	ring     *credentialRing
	probe    string
	modelMap map[string]string
	client   *http.Client
}

// NewOpenAI builds an adapter. It returns an error for a missing ID or
// BaseURL; the config layer owns the deeper URL validation (scheme,
// host), so a malformed base_url never reaches this constructor.
func NewOpenAI(cfg OpenAIConfig) (*OpenAI, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("upstream: openai adapter needs an id")
	}
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("upstream: openai adapter %s needs a base url", cfg.ID)
	}
	transport := cfg.Transport
	if transport == nil {
		transport = DefaultTransport()
	}
	return &OpenAI{
		id:       cfg.ID,
		baseURL:  cfg.BaseURL,
		ring:     newCredentialRing(cfg.APIKeys),
		probe:    cfg.ProbeURL,
		modelMap: cfg.ModelMap,
		client:   &http.Client{Transport: transport},
	}, nil
}

// ID implements Upstream.
func (o *OpenAI) ID() string { return o.id }

// AliveCredentials implements CredentialPool.
func (o *OpenAI) AliveCredentials() int { return o.ring.alive() }

// RetireCredential implements CredentialPool.
func (o *OpenAI) RetireCredential(index int) bool { return o.ring.retire(index) }

// ReviveCredentials implements CredentialPool.
func (o *OpenAI) ReviveCredentials() { o.ring.revive() }

// bodyModel resolves the model name the forwarded body must carry and
// reports whether that differs from what it carries now. A mapped name
// forwards its provider-real name, an unmapped one forwards itself; the
// current carrier is the body's own model (BodyModel, defaulting to
// Model) — a fallback hop advanced the batch model without re-encoding
// the client body, so the rewrite applies whenever the two diverge.
func (o *OpenAI) bodyModel(req Request) (string, bool) {
	target := req.Model
	if real, ok := o.modelMap[req.Model]; ok {
		target = real
	}
	carrier := req.Model
	if req.BodyModel != "" {
		carrier = req.BodyModel
	}
	return target, target != carrier
}

// Forward implements Upstream: one POST exchange with the neutral body,
// rewritten to carry the model this attempt serves (mapped through the
// model map) whenever the body still names a different one. The exchange
// rides the next eligible credential of the ring; a request whose earlier
// attempt burned a credential hands the exclusion list in and the burned
// one is skipped. Attempt timeout and cancellation are owned by the
// caller through ctx.
func (o *OpenAI) Forward(ctx context.Context, req Request) (*Response, error) {
	body := req.Body
	if target, differs := o.bodyModel(req); differs {
		rewritten, err := rewriteModelBody(body, target)
		if err != nil {
			return nil, fmt.Errorf("upstream %s: %w", o.id, err)
		}
		body = rewritten
	}
	index := -1
	if len(o.ring.keys) > 0 {
		index = o.ring.pick(req.ExcludedCredentials)
		if index < 0 {
			return nil, fmt.Errorf("upstream %s: no credential available", o.id)
		}
	}
	target := o.baseURL + completionPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("upstream %s: build request: %w", o.id, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Stream {
		httpReq.Header.Set("Accept", "text/event-stream")
	}
	if index >= 0 {
		httpReq.Header.Set("Authorization", "Bearer "+o.ring.keys[index])
	}
	if req.RequestID != "" {
		httpReq.Header.Set("X-Request-Id", req.RequestID)
	}

	httpResp, err := o.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("upstream %s: exchange: %w", o.id, err)
	}
	return &Response{
		StatusCode:      httpResp.StatusCode,
		Header:          httpResp.Header.Clone(),
		Body:            httpResp.Body,
		CredentialIndex: index,
	}, nil
}

// Probe implements Upstream: the configured health endpoint answers 2xx.
func (o *OpenAI) Probe(ctx context.Context) error {
	if o.probe == "" {
		return fmt.Errorf("upstream %s: no probe url configured", o.id)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, o.probe, nil)
	if err != nil {
		return fmt.Errorf("upstream %s: build probe: %w", o.id, err)
	}
	httpResp, err := o.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("upstream %s: probe: %w", o.id, err)
	}
	defer func() { _ = httpResp.Body.Close() }()
	if _, err := io.Copy(io.Discard, io.LimitReader(httpResp.Body, 1<<10)); err != nil {
		return fmt.Errorf("upstream %s: probe body: %w", o.id, err)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode > 299 {
		return fmt.Errorf("upstream %s: probe status %d", o.id, httpResp.StatusCode)
	}
	return nil
}
