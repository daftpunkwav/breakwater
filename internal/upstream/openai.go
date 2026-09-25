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
	// APIKey is sent as a bearer token when non-empty; the mock
	// upstream does not require one.
	APIKey string
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
	apiKey   string
	probe    string
	modelMap map[string]string
	client   *http.Client
}

// NewOpenAI builds an adapter. It returns an error for a missing ID or
// BaseURL so misconfiguration fails at assembly, not per request.
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
		apiKey:   cfg.APIKey,
		probe:    cfg.ProbeURL,
		modelMap: cfg.ModelMap,
		client:   &http.Client{Transport: transport},
	}, nil
}

// ID implements Upstream.
func (o *OpenAI) ID() string { return o.id }

// Forward implements Upstream: one POST exchange with the neutral body
// passed through verbatim, after a model rewrite when the client-facing
// name maps to a provider-real one. Attempt timeout and cancellation
// are owned by the caller through ctx.
func (o *OpenAI) Forward(ctx context.Context, req Request) (*Response, error) {
	body := req.Body
	if real, ok := o.modelMap[req.Model]; ok && real != req.Model {
		rewritten, err := rewriteModelBody(body, real)
		if err != nil {
			return nil, fmt.Errorf("upstream %s: %w", o.id, err)
		}
		body = rewritten
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
	if o.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+o.apiKey)
	}
	if req.RequestID != "" {
		httpReq.Header.Set("X-Request-Id", req.RequestID)
	}

	httpResp, err := o.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("upstream %s: exchange: %w", o.id, err)
	}
	return &Response{
		StatusCode: httpResp.StatusCode,
		Header:     httpResp.Header.Clone(),
		Body:       httpResp.Body,
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
