/**
 * @file control
 * @description Runtime traffic switches over models and upstreams,
 * operated through the admin API.
 *
 * Responsibilities:
 * - Track which models and upstreams an operator has disabled
 * - Answer eligibility queries for the router's candidate selection
 * - Nothing else: the switch holds no routing policy of its own and
 *   knows nothing about breakers; disabling is an operator action, not
 *   a health reaction
 *
 * Disabled state lives in memory and resets on restart — the same
 * lifetime class as breaker state. Unknown names are fail-open
 * (enabled) so a switch installed on a subset of the config never
 * locks traffic out by accident, while setter calls on unknown names
 * fail closed (error) so an operator typo cannot silently no-op.
 */
package router

import (
	"errors"
	"fmt"
	"sync"
)

// ErrUnknownModel reports a SetModel call for a model no configured
// upstream serves.
var ErrUnknownModel = errors.New("router: unknown model")

// ErrUnknownUpstream reports a SetUpstream call for an unconfigured
// upstream id.
var ErrUnknownUpstream = errors.New("router: unknown upstream")

// ErrDisabled reports that the model is disabled by an operator.
// Callers map it to 403: the refusal is deliberate, not a health
// condition (503) and not a configuration gap (404).
var ErrDisabled = errors.New("router: model is disabled by an operator")

// Switch is the runtime traffic control. It is safe for concurrent
// use: operators write it through the admin API while request
// goroutines read it on every candidate selection.
type Switch struct {
	mu                sync.RWMutex
	disabledModels    map[string]bool
	disabledUpstreams map[string]bool
	knownModels       map[string]struct{}
	knownUpstreams    map[string]struct{}
}

// NewSwitch builds a switch over the configured model names and
// upstream ids. The known sets let the admin surface reject typos
// instead of silently toggling a name nobody serves.
func NewSwitch(knownModels, knownUpstreams []string) *Switch {
	s := &Switch{
		disabledModels:    make(map[string]bool),
		disabledUpstreams: make(map[string]bool),
		knownModels:       make(map[string]struct{}, len(knownModels)),
		knownUpstreams:    make(map[string]struct{}, len(knownUpstreams)),
	}
	for _, m := range knownModels {
		s.knownModels[m] = struct{}{}
	}
	for _, u := range knownUpstreams {
		s.knownUpstreams[u] = struct{}{}
	}
	return s
}

// SetModel disables or re-enables a model. An unknown model is an
// operator error, not a toggle.
func (s *Switch) SetModel(model string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.knownModels[model]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownModel, model)
	}
	if enabled {
		delete(s.disabledModels, model)
	} else {
		s.disabledModels[model] = true
	}
	return nil
}

// SetUpstream disables or re-enables an upstream. An unknown upstream
// is an operator error, not a toggle.
func (s *Switch) SetUpstream(id string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.knownUpstreams[id]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownUpstream, id)
	}
	if enabled {
		delete(s.disabledUpstreams, id)
	} else {
		s.disabledUpstreams[id] = true
	}
	return nil
}

// ModelEnabled reports whether the model accepts traffic. Unknown
// models are enabled: the switch must not become a second model
// registry that shadows the router's.
func (s *Switch) ModelEnabled(model string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.disabledModels[model]
}

// UpstreamEnabled reports whether the upstream accepts traffic.
// Unknown upstreams are enabled, for the same reason.
func (s *Switch) UpstreamEnabled(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.disabledUpstreams[id]
}

// SwitchView is the wire form of the switch state: every known model
// and upstream with its current eligibility.
type SwitchView struct {
	Models    map[string]bool `json:"models"`
	Upstreams map[string]bool `json:"upstreams"`
}

// View snapshots the full switch state for the admin surface.
func (s *Switch) View() SwitchView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	models := make(map[string]bool, len(s.knownModels))
	for m := range s.knownModels {
		models[m] = !s.disabledModels[m]
	}
	upstreams := make(map[string]bool, len(s.knownUpstreams))
	for u := range s.knownUpstreams {
		upstreams[u] = !s.disabledUpstreams[u]
	}
	return SwitchView{Models: models, Upstreams: upstreams}
}
