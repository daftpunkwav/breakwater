# internal/affinity/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- `NewIndex` returns nil for a non-positive TTL. A nil index is valid
  at every call site.
- Callers pass prompt text from `pipeline.PromptText`. The index stores
  FNV-1a 64 chunk hashes, the model name as the map key, and upstream
  ids on trie nodes. It does not store prompt text.
- Cut the prompt into fixed 128-byte chunks from offset zero. Hash each
  chunk with FNV-1a 64. Prompts longer than 512 chunks keep the head.
- `Pick` compares only upstream ids the caller passes. Ties at one
  depth follow the caller's order. The index does not reorder the
  candidate slice. `internal/server` swaps the chosen index with the
  head.
- Record the pick, including a miss. A miss records the current head.
  A failed exchange leaves the recorded entry in place.
- `maxNodes` is 8192 per model. When the next new node would exceed it,
  `record` returns and keeps the shorter prefix already written.
- Enforce the TTL with the write-path sweep. Prune children before
  parents.
- The inference handler applies the index after router eligibility, on
  the primary model and on fallback models.
