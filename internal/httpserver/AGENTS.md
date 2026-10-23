# internal/httpserver/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package row:
[../README.md](../README.md).

- Import only the standard library.
- This package owns the `http.Server` lifecycle for every binary.
  Routing, handlers, configuration, and governance stay elsewhere.
- `Run` serves until `ctx` is cancelled, then drains for
  `ShutdownGrace`.
- A nil handler returns an error. A non-positive `ShutdownGrace`
  becomes 10s. Configured gateway runs never hit that path:
  `internal/config` refuses a non-positive grace.
- `ReadHeaderTimeout` is 10s. `ReadTimeout` is 30s. `IdleTimeout` is
  120s. Leave `WriteTimeout` unset.
- The response tee arms the connection's write deadline (30s) before
  every write and flush, so one blocked write gives up while a long
  stream keeps streaming. The deadline is re-armed per write, never
  set once.
- `ErrDrainTimeout` means requests were still in flight when the drain
  window expired. Callers log it and exit zero. Every other `Shutdown`
  error is a failure.
- A caller-supplied `Listener` is owned by `Run` after the call. The
  caller does not close it and does not reuse it.
- The response tee copies bytes. It does not classify gateway outcomes.
- A detached tee absorbs its client's write failures and reports the
  write as a full success — the shared work it backs must survive a
  gone reader. An ordinary tee surfaces write failures.
