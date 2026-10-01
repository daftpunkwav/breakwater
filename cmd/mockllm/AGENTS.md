# cmd/mockllm/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Flag table:
[../README.md](../README.md). Handler rules:
[../../internal/mockllm/AGENTS.md](../../internal/mockllm/AGENTS.md).

- This directory is the CLI wrapper. Fault behavior stays in
  `internal/mockllm`.
- Flags: `-addr` (default `:8090`), `-default-delay` (default `0`),
  `-error-rate` (default `0`, range 0..1).
- Serve through `httpserver.Run` with a 10s shutdown grace.
- Do not assemble governance, identity, or the gateway pipeline here.
