# docs/ agent rules

## Pairing

- Each document has an English file and a `*.zh.md` pair. Update both
  in the same change. Keep the section order aligned.

## Evidence

- `BENCHMARK.md` and `CHAOS-REPORT.md` record executed runs. Leave an
  unmeasured cell empty.
- Figures in those files come from `loadtest/` runs.
- Test placement, naming, and the invariant index live in `TESTING.md`.
- Local provider wiring lives in `DEPLOY-LOCAL.md`.
