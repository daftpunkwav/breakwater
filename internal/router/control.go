/**
 * @file control
 * @description Runtime traffic switches over models and upstreams,
 * operated through the admin API — plus the automatic upstream
 * disables the system records when an upstream proves fatally broken
 * (dead credentials, exhausted quota).
 *
 * Responsibilities:
 * - Track which models and upstreams an operator has disabled
 * - Track which upstreams the system has auto-disabled, with the
 *   reason and the moment of the decision
 * - Answer eligibility queries for the router's candidate selection
 * - Nothing else: the switch holds no routing policy of its own and
 *   knows nothing about breakers; a health reaction never overwrites
 *   an operator decision and vice versa
 *
 * Disabled state lives in memory and resets on restart — the same
 * lifetime class as breaker state. Two disable channels are kept
 * apart: a manual disable is an operator action that
 * only an operator lifts; an auto disable is a system reaction to a
 * fatal upstream condition that the recovery prober lifts when the
 * upstream answers a health probe again. An operator touching the
 * upstream clears both — human intent wins. Unknown names are
 * fail-open (enabled) so a switch installed on a subset of the config
 * never locks traffic out by accident, while setter calls on unknown
 * names fail closed (error) so an operator typo cannot silently no-op.
 */
package router

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrUnknownModel reports a SetModel call for a model no configured
// upstream serves.
var ErrUnknownModel = errors.New("router: unknown model")

// ErrUnknownUpstream reports an upstream switch call for an
// unconfigured upstream id.
var ErrUnknownUpstream = errors.New("router: unknown upstream")

// ErrDisabled reports that the model is disabled by an operator.
// Callers map it to 403: the refusal is deliberate, not a health
// condition (503) and not a configuration gap (404).
var ErrDisabled = errors.New("router: model is disabled by an operator")

// autoDisable records why and when the system took an upstream out of
// rotation on its own.
type autoDisable struct {
	reason string
	since  time.Time
}

// Switch is the runtime traffic control. It is safe for concurrent
// use: operators write it through the admin API while request
// goroutines read it on every candidate selection, and the relay's
// fatal-error hook auto-disables upstreams mid-flight.
type Switch struct {
	mu                sync.RWMutex
	disabledModels    map[string]bool
	disabledUpstreams map[string]bool
	autoDisabled      map[string]autoDisable
	knownModels       map[string]struct{}
	knownUpstreams    map[string]struct{}
	// wildcardModels reports that a binding serves every model via the
	// "*" wildcard. A concrete name is then toggleable on demand: the
	// switch cannot know the wildcard's full model vocabulary up front,
	// so the typo protection only applies when no wildcard exists.
	wildcardModels bool
}

// SwitchOption customizes a Switch.
type SwitchOption func(*Switch)

// WithWildcardModels marks the deployment as wildcard-served: any
// concrete model name may be disabled even though the config never
// enumerated it.
func WithWildcardModels(wildcard bool) SwitchOption {
	return func(s *Switch) { s.wildcardModels = wildcard }
}

// NewSwitch builds a switch over the configured model names and
// upstream ids. The known sets let the admin surface reject typos
// instead of silently toggling a name nobody serves.
func NewSwitch(knownModels, knownUpstreams []string, opts ...SwitchOption) *Switch {
	s := &Switch{
		disabledModels:    make(map[string]bool),
		disabledUpstreams: make(map[string]bool),
		autoDisabled:      make(map[string]autoDisable),
		knownModels:       make(map[string]struct{}, len(knownModels)),
		knownUpstreams:    make(map[string]struct{}, len(knownUpstreams)),
	}
	for _, m := range knownModels {
		s.knownModels[m] = struct{}{}
	}
	for _, u := range knownUpstreams {
		s.knownUpstreams[u] = struct{}{}
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SetModel disables or re-enables a model. An unknown model is an
// operator error, not a toggle — unless the deployment serves a
// wildcard, in which case concrete names arrive unenumerated and the
// toggle lands on the named model the wildcard serves.
func (s *Switch) SetModel(model string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.knownModels[model]; !ok && !s.wildcardModels {
		return fmt.Errorf("%w: %q", ErrUnknownModel, model)
	}
	if enabled {
		delete(s.disabledModels, model)
	} else {
		s.disabledModels[model] = true
	}
	return nil
}

// SetUpstream disables or re-enables an upstream by operator action.
// Enabling clears both disable channels — an operator's explicit
// enable always wins over any recorded auto disable. An unknown
// upstream is an operator error, not a toggle.
func (s *Switch) SetUpstream(id string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.knownUpstreams[id]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownUpstream, id)
	}
	if enabled {
		delete(s.disabledUpstreams, id)
		delete(s.autoDisabled, id)
	} else {
		delete(s.autoDisabled, id)
		s.disabledUpstreams[id] = true
	}
	return nil
}

// AutoDisableUpstream takes an upstream out of rotation because a
// fatal upstream condition was observed (bad credentials, exhausted
// quota). The first call records the reason and reports true; further
// calls only refresh nothing and report false, so callers can count
// transitions exactly. An already manually disabled upstream is left
// as it is (the manual state subsumes the auto one) and reports false.
// An unknown upstream is an error.
func (s *Switch) AutoDisableUpstream(id, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.knownUpstreams[id]; !ok {
		return false, fmt.Errorf("%w: %q", ErrUnknownUpstream, id)
	}
	if s.disabledUpstreams[id] {
		return false, nil
	}
	if _, ok := s.autoDisabled[id]; ok {
		return false, nil
	}
	s.autoDisabled[id] = autoDisable{reason: reason, since: time.Now().UTC()}
	return true, nil
}

// AutoEnableUpstream lifts an auto disable, restoring the upstream to
// rotation. An unknown upstream is an error; a manually disabled
// upstream stays disabled — recovery only ever touches the system's
// own decision.
func (s *Switch) AutoEnableUpstream(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.knownUpstreams[id]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownUpstream, id)
	}
	delete(s.autoDisabled, id)
	return nil
}

// AutoDisabledIDs lists the currently auto-disabled upstreams in
// sorted order, for the recovery prober's iteration.
func (s *Switch) AutoDisabledIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.autoDisabled))
	for id := range s.autoDisabled {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ModelEnabled reports whether the model accepts traffic. Unknown
// models are enabled: the switch must not become a second model
// registry that shadows the router's.
func (s *Switch) ModelEnabled(model string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.disabledModels[model]
}

// UpstreamEnabled reports whether the upstream accepts traffic:
// neither operator-disabled nor auto-disabled. Unknown upstreams are
// enabled, for the same reason.
func (s *Switch) UpstreamEnabled(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, auto := s.autoDisabled[id]
	return !s.disabledUpstreams[id] && !auto
}

// AutoDisableInfo is the wire form of one auto disable.
type AutoDisableInfo struct {
	// Reason is the machine-readable cause, e.g. upstream_auth_failure.
	Reason string `json:"reason"`
	// Since is the UTC moment the upstream was taken out of rotation.
	Since time.Time `json:"since"`
}

// SwitchView is the wire form of the switch state: every known model
// and upstream with its current eligibility, plus the auto disables
// with their causes.
type SwitchView struct {
	Models    map[string]bool            `json:"models"`
	Upstreams map[string]bool            `json:"upstreams"`
	Auto      map[string]AutoDisableInfo `json:"auto_disabled"`
}

// View snapshots the full switch state for the admin surface. Models
// disabled while unenumerated (wildcard deployments toggle concrete
// names on demand) appear as the disabled entries they are — a
// disabled model must be visible to the operator even though no
// binding ever named it.
func (s *Switch) View() SwitchView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	models := make(map[string]bool, len(s.knownModels)+len(s.disabledModels))
	for m := range s.knownModels {
		models[m] = !s.disabledModels[m]
	}
	for m := range s.disabledModels {
		if _, ok := s.knownModels[m]; !ok {
			models[m] = false
		}
	}
	upstreams := make(map[string]bool, len(s.knownUpstreams))
	auto := make(map[string]AutoDisableInfo, len(s.autoDisabled))
	for u := range s.knownUpstreams {
		upstreams[u] = !s.disabledUpstreams[u]
		if a, ok := s.autoDisabled[u]; ok {
			upstreams[u] = false
			auto[u] = AutoDisableInfo{Reason: a.reason, Since: a.since}
		}
	}
	return SwitchView{Models: models, Upstreams: upstreams, Auto: auto}
}
