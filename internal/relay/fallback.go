/**
 * @file fallback
 * @description The fallback plan: model batches resolved lazily as the
 * attempt loop exhausts them.
 *
 * Responsibilities:
 * - Track which model's candidates the attempt loop is currently
 *   walking, and advance to the next fallback model when they run out
 * - Nothing else: what the fallback chain contains is the Job's and
 *   the assembly's decision; resolution goes through the Job's
 *   resolver so the relay stays decoupled from the router
 *
 * The machinery is deterministic per attempt index and caches resolved
 * batches, so the retry-strip lookahead may evaluate the next target
 * before the loop actually reaches it without changing what that
 * attempt will see.
 */
package relay

import (
	"context"

	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// batch is one model's candidate list within the plan.
type batch struct {
	model      string
	candidates []upstream.Upstream
}

// target returns the candidate and model serving attempt n, advancing
// through the fallback plan as batches exhaust. When no fallback is
// left, extra attempts clamp to the last candidate of the last batch —
// the standing rule for attempts beyond the candidate list.
func (r *requestRun) target(attempt int) (upstream.Upstream, string) {
	offset := attempt - r.batchStart
	for offset >= len(r.batches[r.batchIndex].candidates) {
		if !r.advance(attempt) {
			offset = len(r.batches[r.batchIndex].candidates) - 1
			break
		}
		offset = attempt - r.batchStart
	}
	b := r.batches[r.batchIndex]
	return b.candidates[offset], b.model
}

// advance resolves the next unattempted fallback model into a batch
// and makes it current. It reports false when the chain is drained.
// Resolver rejections (unknown model, disabled, all candidates
// ineligible) skip that fallback: the chain is a preference list, not
// a contract that every entry can serve.
func (r *requestRun) advance(attempt int) bool {
	if r.job.Resolve == nil {
		return false
	}
	for r.fallbackIndex < len(r.job.Fallbacks) {
		model := r.job.Fallbacks[r.fallbackIndex]
		r.fallbackIndex++
		if r.attempted[model] {
			continue
		}
		r.attempted[model] = true
		cands, err := r.job.Resolve(r.ctx, model)
		if err != nil || len(cands) == 0 {
			continue
		}
		r.batches = append(r.batches, batch{model: model, candidates: cands})
		r.batchIndex = len(r.batches) - 1
		r.batchStart = attempt
		return true
	}
	return false
}

// CandidateResolver is the Job's resolution port: one fallback model
// in, its priority-ordered candidates out. An error or an empty list
// means "this model cannot serve right now" and the chain moves on.
type CandidateResolver func(ctx context.Context, model string) ([]upstream.Upstream, error)
