# affinity/

> Language: **English** | [简体中文](README.zh.md)

Prompt-prefix affinity: remembers which upstream recently served which
prompt prefixes and promotes the upstream holding the longest match to the
head of the candidate list. A provider's prompt cache warms per prefix, so
repeating a prefix on the upstream that already holds it skips the prefill
work the first request paid for. Off by default — `NewIndex` returns nil
for a non-positive TTL, and a nil index is valid at every call site, so
disabled affinity needs no branch.

## Files

| File | Role |
| --- | --- |
| `affinity.go` | `Index`: one prefix trie per model; `Pick` matches the request's chunk chain and records the routing decision (misses included — an index that only recorded matches would never seed) |

## Mechanism

- The prompt text (see `pipeline.PromptText`) is cut into fixed 128-byte
  chunks starting at position zero, each hashed independently (FNV-1a 64).
  Fixed chunks need no per-node text and no insert-time edge splitting; two
  prompts align on the same boundaries, and the trailing short slice hashes
  as-is, so a conversation continuing past a recorded request's end loses
  at most one chunk of affinity. Prompts longer than 512 chunks use their
  head, where cache reuse concentrates.
- A trie node holds the upstream IDs routed through that prefix. Matching
  walks the chain keeping the eligible ID with the lowest router position
  at each depth; the deepest hit wins, and the walk stops at the first
  depth whose node is missing or holds none of the eligible IDs — a dead
  or excluded upstream can never pin traffic because only IDs the router
  already cleared are ever compared.
- The decision is recorded at pick time for the candidate that will lead
  the attempt: a miss records the head, so the next identical request can
  match. A record is the routing decision's echo, not a completion report;
  a failed exchange leaves its entry in place and the next eligibility
  filter is what retires it.
- Memory is bounded twice: a per-model node ceiling (a record that cannot
  fit is skipped, degrading to plain strategy order) and a freshness TTL
  enforced by a throttled sweep on the write path — the same discipline
  the cache module's expiry uses. Pruning walks children first, so a
  long-shared prefix survives while only its tail extensions are stale.

## Scope rules

- The index sees strings only: model name, prompt text, upstream IDs. The
  handler composes it after the router's eligibility gates and applies it
  to fallback models too — the chain walks the same discipline as the
  primary.
- A promotion overrides the strategy's head because cache warmth dominates
  the latency differences the strategy measures; everything after the head
  keeps the router's order, so failover is untouched.
- Ties at one depth resolve by router position, not at random: with two or
  three upstreams the deterministic choice is also the measurable one.
- Hash collisions merge two chunk chains and can only route a request to
  an upstream that serves the model anyway — never a correctness issue.
