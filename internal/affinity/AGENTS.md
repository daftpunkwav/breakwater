# internal/affinity/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- `NewIndex` returns nil for a non-positive TTL. A nil index is valid
  at every call site.
- The index stores model names, prompt text, and upstream ids. Prompt
  text comes from `pipeline.PromptText`.
- Cut the prompt into fixed 128-byte chunks from offset zero. Hash each
  chunk with FNV-1a 64. Prompts longer than 512 chunks keep the head.
- Compare only upstream ids the router already returned. A promotion
  adds no excluded upstream.
- Move the longest eligible match to the head. Leave the remaining
  router order unchanged. Ties at one depth follow router position.
- Record the pick, including a miss. A miss records the current head.
  A failed exchange leaves the recorded entry in place.
- Skip a record that does not fit the per-model node ceiling.
- Enforce the TTL with the write-path sweep. Prune children before
  parents.
- The inference handler applies the index after router eligibility, on
  the primary model and on fallback models.
