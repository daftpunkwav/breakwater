# 测试

> 语言：**简体中文** | [English](TESTING.md)

每个包的测试与其代码同置（`foo_test.go` 与 `foo.go` 并排，同包或 `_test`
包）——这是 Go 的惯例，刻意保留：没有独立的测试树，没有会漂移的平行层级。

## 测试分类

| 类别 | 位置 | 内容 |
| ---- | ----- | ---- |
| Unit | `internal/<pkg>/*_test.go`，同包 | 一个机制一个文件：状态机、Lua 台账语义（经 miniredis）、token 桶、限额合并层、transcoder、codec、helper。分支需要处做白盒。 |
| Functional | `internal/server/chain_test.go`、`inference_formats_test.go`、`completions_test.go`、`model_authz_cache_test.go` | 走真实路由的完整 pipeline 场景：八个验收场景（key 拒绝、限流、失败零结算、quota 耗尽、经 governance 的流式、tier 拒绝、客户端断连结算与 request-id 回显）、三种 client format、诚实流终结、缓存退款记账、tier 授权与缓存的相互作用。 |
| Integration | `*_integration_test.go`，环境变量门控 | 真实 PostgreSQL 与 Redis（CI workflow 以 service container 提供两者）。本地未设置 `BREAKWATER_TEST_POSTGRES_DSN` / `BREAKWATER_TEST_REDIS_ADDR` 时跳过。 |
| Concurrency / race | 遍布各包，`-race` 是 CI 默认 | 不变量证据：并发 quota 消耗对账归零、stampede 回源恰好一次、breaker probe 槽位绝不双授、并发闸绝不越顶、日志丢弃仍被计数。 |
| Benchmark | `*_bench_test.go` | SSE 泵、singleflight stampede 与并发闸锁基线（`go test -bench`）。 |

## 命名与内聚

- 一个测试文件覆盖一个内聚主题；文件名陈述该主题
  （`stream_termination_test.go`、`load_validation_test.go`）。无关的测试
  绝不共用文件，无论文件多小。
- 共享 fixture 放在名字准确的自有文件中（`testupstream_test.go`、
  `teststore_test.go`）。
- 测试断言可观察行为（状态码、响应体、余额、指标），不断言内部调用图。

## 不变量映射

下表每个机制陈述它保证的性质、拥有它的代码，以及性质被破坏时会失败的测试。
测试名就是索引——直接运行任一条即可检验该性质。

| 性质 | 拥有者 | 守护测试 |
| -------- | ----- | ------------- |
| 被拒请求绝不抵达 upstream | `internal/server`、`internal/pipeline` | `TestChainRejectsMissingAndUnknownKeys`、`TestChainRateLimitsWithRetryAfter`、`TestChainQuotaExhaustionIsPaymentRequired`、`TestChainDeniesModelOutsideTier` |
| 同一 cache key 的并发冷请求恰好一次上游回源 | `internal/cache` | `TestFlightSingleFetchUnderStampede`、`TestCacheMiddlewareConcurrentColdStartsFetchOnce` |
| panic 的回源释放其 waiter 而非卡死它们 | `internal/cache` | `TestFlightPanicReleasesWaiters` |
| 请求绝不超出 attempt 上限，重试绝不超出全局 in-flight 预算 | `internal/retry` | `TestExecuteCapsAttempts`、`TestBudgetCapsInFlightRetries`、`TestBudgetReleaseAbsorbsImbalance` |
| 第一个响应字节之后，loop 绝不重试 | `internal/retry`、`internal/relay` | `TestExecuteNeverClassifiesCommittedErrors`、`TestMessagesRouteStreamAbortTerminatesHonestly` |
| 打开的 breaker 拒绝一切调用；half-open 恰放行一个 probe 并回收被弃的 | `internal/circuit` | `TestBreakerConcurrentProbesExactlyOne`、`TestBreakerReclaimsAbandonedProbe` |
| ratio 守卫按失败占比拒绝越来越多的调用，但绝不完全切断流量——每个强制放行间隔仍有一个调用通过 | `internal/circuit` | `TestRatioDenialTracksFailureShare`、`TestRatioForcePassAdmitsOnePerInterval` |
| slow-call 熔断只在采样窗口的慢占比达标时开路（样本不足永不开路，故障算最强的慢证据），健康探测在清空后的窗口上关闭它 | `internal/circuit` | `TestSlowWindowNeedsSamples`、`TestSlowShareOpensTheBreaker`、`TestSlowProbeClosesOnAnEmptiedWindow` |
| 健康的 attempt 越过慢阈值即上报为慢——每个策略都按成功级证据吸收；流式以首字节时刻度量 | `internal/circuit`、`internal/relay` | `TestConsecutiveAbsorbsSlowAsHealth`、`TestSlowBufferedAttemptReportsSlow`、`TestSlowStreamTTFTReportsSlow` |
| 彻底宕掉的上游其恢复探测按翻倍阶梯拉开间隔；一次健康应答即忘记阶梯 | `cmd/breakwater` | `TestProbeBackoffLadder`、`TestRecoveryBackoffZeroKeepsFixedPace` |
| 对账轮次不会活过自己的节拍；卡死的 store 在边界处中止该轮 | `internal/quota` | `TestStartReconcilerBoundsAWedgedRound` |
| 断掉的流保留已交付字节，以一个错误事件加 `[DONE]` 收尾 | `internal/relay`、`internal/protocol` | `TestWriteAbortContract`、`TestWriteAbortAllCodes`、`TestMessagesRouteStreamAbortTerminatesHonestly` |
| 余额绝不透支；扣减减去退款与余额对账 | `internal/quota` | `TestMemoryConcurrentDrainReconciles`、`TestRedisConcurrentDrainReconciles` |
| 每笔预留都有 lease 记录；被弃的 lease 被回收 | `internal/quota` | `TestStartSweeperReclaimsUntilCancelled` |
| 流中途离开的客户端仍按其消耗结算 | `internal/quota`、`internal/server` | `TestChainClientDisconnectCancelsUpstreamAndSettlesByUsage` |
| 观测缓冲满时丢弃并计数，绝不阻塞 handler | `internal/obs`、`internal/insights` | `TestLoggerDropsOldestUnderPressure`、`TestRecordQueuesAndCountsDrops` |
| 失败的批量写入被计数，而不是静默重试 | `internal/insights` | `TestCopyIntoDropsOnFailure` |
| 观测队列在进程退出前排空 | `cmd/breakwater`、`internal/obs`、`internal/insights` | `TestLoggerCloseDrains`、`TestWriteLoopFlushesOnClose` |

## 覆盖率

门槛是每包 ≥95% 语句覆盖，CI 的 coverage job 强制执行。该 job 提供 Redis 与
PostgreSQL，因此数据库门控的集成测试在那里运行——这正是写门槛时针对的
环境。本地缺少这两个服务时 `go test -cover ./...` 读数更低，因为整段 SQL
路径在 `BREAKWATER_TEST_POSTGRES_DSN` 与 `BREAKWATER_TEST_REDIS_ADDR` 之后：

| 包 | 本地、无数据库 | 双服务齐备 |
| ------- | ------------------- | ------------------ |
| `internal/auth` | 90.3% | ~99% |
| `internal/insights` | 87.2% | ~99% |
| `cmd/breakwater` | 92.3% | ~96% |

其余所有包在本地、无任何服务的情况下即 ≥95%。即便双数据库齐备，
`cmd/breakwater` 未覆盖的仍是一组防御性 error return——任何合法配置都无法
抵达：upstream adapter 与 router 本就由已校验的配置构建，它们的错误分支是
为未来其他构建方式预留的。这些如实披露，而不是用怎么跑都过的测试伪装。

本地数字用以下命令验证：

    go test -cover ./...
