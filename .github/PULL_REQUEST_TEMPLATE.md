<!-- The title doubles as the squash-merge commit message: keep it a
Conventional Commit — type(scope): subject. One PR does one thing; merge
requirements live in the branch ruleset (2 approvals, threads resolved,
up to date with main). -->

## What

<!-- What changed, in operator-visible terms. -->

## Why

<!-- The problem or driver; link the issue if one exists (`Closes #123`). -->

## How

<!-- What a reviewer should know: key decisions, alternatives considered
and rejected, boundaries touched (compose topology, image layout, config
surface). Delete this section if the diff is self-explanatory. -->

## Verification

<!-- What ran and what it proved. CI runs the same gates; local runs catch
failures before the push. State skipped checks honestly. -->
- [ ] Tests added or updated — a bug fix ships a regression test that fails
      before the fix and passes after it
- [ ] `go vet ./...`
- [ ] `go test -race -cover -timeout 8m ./...` (coverage floor ≥ 95% per
      package, enforced in CI)
- [ ] `go tool govulncheck ./...`
- [ ] Deploy-facing changes: image builds and the compose smoke re-run in CI
      (`docker compose build && docker compose up` locally if they touch
      Dockerfiles or the compose files)
- [ ] Anything the tests cannot reach was verified manually (describe below)

<!-- Manual steps, before/after output. Delete if empty. -->

## Compatibility impact

<!-- Breaking changes, config/env surface, migration notes — or "None."
State it explicitly. -->

## Security and supply chain

<!-- Does the change touch authentication or trust boundaries, go.mod/go.sum,
or GitHub workflows? security.yml scans on every PR regardless of paths —
use this section to give the reviewer context the scans cannot infer.
Otherwise write "N/A". -->

## Reviewer notes

<!-- Non-obvious trade-offs, known follow-ups, areas that deserve extra
scrutiny. Delete if empty. -->
