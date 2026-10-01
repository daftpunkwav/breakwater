# internal/relay/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- This package turns one client request into one client-visible
  response. Candidate order belongs to `internal/router`. Transport
  belongs to `internal/upstream`. Wire codecs belong to
  `internal/protocol`.
- Retry, circuit breaking, and failover stay inside `Execute`.
  Settlement, cache write, and observation run once per client
  request, after `Execute` returns. `Execute` does not settle quota.
- Finish with exactly one of: the delivered reply, a verbatim
  passthrough of the last upstream error, or a gateway envelope.
- After the first byte is written to the client, do not start another
  attempt. Terminate with one error frame in the client format. Bytes
  already sent stay sent. Return `retry.ErrCommitted` to the loop.
- A client disconnect is `clientFault` for breaker accounting and the
  routing observer.
- Honor `Retry-After` only for the next attempt on the same credential
  and upstream. A different candidate, or another credential of the
  same upstream, does not wait on that hint.
- Fatal classification is `insufficient_quota`, 401, and
  credential-class 403, reported with the credential that served the
  exchange. A request-scoped 403 is not fatal. Retire the credential
  first. Auto-disable the upstream only when its credential ring is
  empty.
- Record every attempt on `Result.Trail`, finished or not: upstream,
  credential, and status.
- `readBounded` fails the attempt when the body exceeds
  `maxResponseBytes` (32 MiB). It does not truncate. The stream commit
  point is the first byte written to the client.
- A streaming attempt uses the attempt timeout until the first byte,
  then the optional stream ceiling and the optional idle watchdog.
  Each body read re-arms the idle watchdog.
- `pumpStream` returns `errStreamTruncated` when it saw a data line
  and never saw `[DONE]`. EOF with no data line is success.
- `pumpTranscoded` returns `errStreamTruncated` when EOF arrives
  without `[DONE]`.
- A healthy attempt slower than the slow-call threshold is reported
  as `OutcomeSlow`.
