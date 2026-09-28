# retry/

> 语言：**简体中文** | [English](README.md)

有界的 attempt loop：一个客户端请求在三个上限下分解为若干上游
attempt——数量上受 `MaxAttempts`，单个 attempt 受 `AttemptTimeout`，
全部加总受 `OverallDeadline`。本包拥有 loop 的形态、进程级在途预算
（retry storm containment）与可重试性分类器。它不选择候选
（[`internal/router`](../router/)）、不记账 breaker 状态
（[`internal/circuit`](../circuit/)）、也不渲染客户端可见的错误——
客户端响应属于 relay 的 forward stage；loop 只决定"没有剩余 attempt"。
relay 的 executor（[executor.go](../relay/executor.go)）对每个请求
驱动一次 `Execute`，并把每个 attempt 的结果折叠为一个
`circuit.Outcome`。

## Files

| File | Role |
| --- | --- |
| `retry.go` | 契约：`Policy`（三个上限加上 backoff 形状）、`Classifier`、`Budget`——固定在途上限，或随当前在途请求伸缩的份额预算（百分比加下限） |
| `loop.go` | `Execute`：attempt loop——首个 attempt 之后受预算门控、full-jitter 指数 backoff、上游 `Retry-After` 提示带小幅上偏抖动地等待（绝不低于提示、至多一半再多）、`ErrBudgetExhausted` 包装触发错误 |
| `classifier.go` | `DefaultClassifier` 可重试性表；`StatusError`；`ErrCommitted`；`ParseRetryAfter`（整数、小数或 HTTP-date 形式，上限 60s）；`StripRetryAfter` |

## Tests

`retry_after_test.go` 覆盖 hint 解析及其对计算 backoff 的优先；
`backoff_policy_test.go` 与 `backoff_classifier_test.go` 覆盖 ceiling
计算与分类表；`attempt_deadline_test.go` 与
`classifier_transport_test.go` 覆盖 deadline 分层与传输层失败用例。

## Invariants

- first-byte 边界属于 loop，不属于分类器：字节到达客户端之后的失败是
  `ErrCommitted`，原样返回——不分类、不再尝试；重试等于重放半途的
  回复。
- 首个 attempt 之后的每一次重试都必须获取进程级 `Budget`；预算耗尽时
  以 `ErrBudgetExhausted` 快速失败，并包装触发该次重试的 attempt
  错误。`Budget` 是有意设计为具体类型——单一进程内计数器，没有可
  预见的共享后端。
- `Retry-After` hint 只对"下一个 attempt 打同一个上游"替换计算出的
  backoff；relay 在 failover 到不同候选前调用 `StripRetryAfter`——
  一个上游的恢复时间表说明不了另一个的。
- 客户端取消（`context.Canceled`）永不重试；超时与连接级传输失败会
  重试，于是一个死上游变成一次 failover 而非一个失败请求。429 与
  5xx 可重试，其余 4xx 是客户端的问题。
- `MaxAttempts < 1` 钳位为 1——零意味着一次，绝不是"无限重试"；
  零值的 timeout 与 deadline 意味着不设限。

配置：`BREAKWATER_RETRY_MAX_ATTEMPTS`（3）、`_ATTEMPT_TIMEOUT`
（30s）、`_OVERALL_DEADLINE`（60s）、`_BACKOFF_INITIAL`（100ms）、
`_BACKOFF_MAX`（2s）与 `BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT`（64）。
