# limiter/

> 语言：**简体中文** | [English](README.md)

时间窗吞吐保护：每租户的 RPM/TPM token bucket，外加每租户并发闸门。它是纯
机制——租户仅以 key 形式出现，上限值由管线从 [`internal/auth`](../auth/)
产出的身份快照解析后传入。它不负责余额账本（[`internal/quota`](../quota/)
的事）；两个阶段共享本包阶段计算的那一次 token 估算值。阶段顺序：auth →
model authorization → concurrency → limiter → quota → cache；超限请求在任何
上游被触碰之前即以 `429` 与 `Retry-After` 拒绝。

`Limiter` 有两个后端：`Memory`（连续补充的 bucket，用于测试与内存退化形
态）和 `Redis`（同一语义经 Lua 在 Redis `TIME` 上原子执行）。并发闸门按设
计就是进程内计数信号量。

## Files

| File | Role |
| --- | --- |
| `limiter.go` | 契约：`Limiter`（`Allow`/`Refund`）、`Limits`（零值即关闭该上限）、`Decision`（`Allowed`、`RetryAfter`） |
| `memory.go` | `Memory`：初始满格的连续补充 bucket；`Refund` 只会把桶抬向容量 |
| `redis.go` | `Redis`：`bw:limiter:`（或 `<namespace>:bw:limiter:`）下的 key，脚本经 `go:embed` 内嵌 |
| `tokenbucket.lua` | 原子的补充-检查-扣减；Redis 时钟；容量 0 即跳过该维度；key 120s 过期 |
| `refund.lua` | 先补充到当前时刻，再把未消耗的 token 退回 TPM 桶，绝不超过容量 |
| `concurrency.go` | `Concurrency`：手写计数信号量，release 幂等 |
| `middleware.go` | 429 限流阶段：单次 body 读取、token 估算、后端故障即 fail-closed、调用后只退不加 |
| `concurrencymiddleware.go` | 429 并发阶段：`concurrency_limit_exceeded`，先于任何限流预留执行 |

## Tests

桶补充、边界情况与中间件管线为单元测试；`redis_integration_test.go` 需要
Redis（`BREAKWATER_TEST_REDIS_ADDR`）。

## Invariants

- 被拒绝的请求绝不触碰上游、绝不消耗额度：并发闸门先于限流预留，限流预留
  先于 quota lease。
- 后端故障 fail-closed：中间件以 `503 governance_unavailable` 拒绝，而不是
  放行未计量的流量。fail-closed 策略属于管线层，不属于后端。
- 只退不加：调用后修正只返还未消耗的部分；退款只能归还预留，绝不能凭空制
  造余量。客户端已离开则退款丢失——这是安全的方向。
- Redis 脚本的时钟取自 Redis `TIME`，绝不用应用时钟，补充因此不依赖共享的
  应用时钟。
- 并发闸门的槽位是进程内的；多实例部署不得将其读作全局上限。
