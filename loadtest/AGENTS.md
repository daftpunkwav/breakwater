# loadtest/ agent rules

Scenario table: [README.md](README.md). Recorded numbers:
[../docs/BENCHMARK.md](../docs/BENCHMARK.md) and
[../docs/CHAOS-REPORT.md](../docs/CHAOS-REPORT.md).

- One script targets one property of the gateway.
- Scripts talk to the gateway over HTTP. Do not import Go packages
  from this directory.
- Keep the fault procedure for `chaos-upstream.js`, `retry-storm.js`,
  and `redis-kill.js` in the script header. Those runs restart mockllm
  or Redis.
- Read `breakwater_*` counters from `GET /metrics`. Name the counters
  the script checks in the file header.
- Default `API_KEY` is `bw-local-t1`, except `quota-race.js`, which
  defaults to `bw-local-t2`. Those keys are in `deploy/seed.sql`.
- `quota-race.js` sends `ADMIN_TOKEN` on its teardown query. Set it
  when the gateway has `BREAKWATER_ADMIN_TOKEN`.
