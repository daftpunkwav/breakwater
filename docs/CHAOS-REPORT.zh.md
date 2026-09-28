# Chaos 报告

> 语言：**简体中文** | [English](CHAOS-REPORT.md)

> 状态：以下故障注入步骤已定稿并脚本化；时间线由执行这些步骤填入。这里
> 没有任何内容是纸上模拟——空小节表示"尚无在案执行"。

## 方法

故障注入面向运行中的 stack，绝不在网关内部 mock。每次实验记录：破坏了
什么、何时、客户端看到了什么（状态码与延迟），以及 `/metrics` 上的恢复
过程。

## 实验

### 1. Upstream 完全故障 → breaker 时间线

- 注入：`mockllm -error-rate 1.0`
- 观察：连续失败打开 breaker；客户端感知的失败延迟从 timeout 量级塌缩为
  快速 503；cooldown 之后恰好放行一个 probe；恢复时状态走 half-open →
  closed。
- 证据：`breakwater_circuit_open_total`、
  `breakwater_circuit_half_open_total`、`breakwater_circuit_state`，加上
  k6 `failure_latency` 趋势（`loadtest/chaos-upstream.js`）。

| 字段 | 值 |
| ----- | ----- |
| 执行于 | _待填_ |
| 打开耗时（自首次失败起） | |
| open 期失败 P99 | |
| 恢复（open → closed） | |

### 2. 流中途被切断 → 诚实终结

- 注入：上游请求路径上的 `X-Mockllm-Stream-Mode: abort`。
- 观察：客户端保留已交付的 chunk，随后收到恰好一帧 `event: error` 加
  `data: [DONE]`；`breakwater_sse_stream_aborted_total` 计数器对每个被中断
  的流递增一次。

| 字段 | 值 |
| ----- | ----- |
| 执行于 | _待填_ |
| 观察到的 abort code | |

### 3. 负载下杀掉 Redis → fail-closed 降级

- 注入：运行中停掉 Redis（`loadtest/redis-kill.js`）。
- 观察：无法限流的请求被 503 拒绝（fail-closed，绝不静默放行），进程内
  响应缓存全程照常命中——它从不依赖 Redis；readiness 翻转为 503；Redis
  恢复后网关不经重启即恢复服务。Quota lease 存活在 Redis 中并在恢复后
  对账。

| 字段 | 值 |
| ----- | ----- |
| 执行于 | _待填_ |
| 故障期拒绝数 | |
| 重启后恢复时间 | |
| 恢复后余额漂移（必须为 0） | |

### 4. 进程死于 reserve 与 settle 之间

- 注入：在 reserve 已可见于 Redis、结算尚未发生时对网关 `SIGKILL`
  （脚本化运行，配一个很长的 completion 流）。
- 观察：lease 保持 RESERVED 直至自身 TTL 到期——默认回收视野是请求预算
  加一分钟，所以重启那一刻没有任何东西被回收。此后 sweeper 把孤儿 lease
  标为 EXPIRED 并分批退款，余额恒等式从下一个快照区间起恢复对账。

| 字段 | 值 |
| ----- | ----- |
| 执行于 | _待填_ |
| 回收的 lease | |
| 退款正确性 | |
