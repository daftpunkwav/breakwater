# loadtest

> 语言：**简体中文** | [English](README.md)

针对 breakwater 网关的 k6 负载测试与 chaos 场景。每个场景瞄准系统的一条
特定不变量，产出发布在 `docs/` 下的证据。

## 前置条件

- 已武装 governance 的网关（identity + Redis 或 memory 模式）
- mock upstream：`go run ./cmd/mockllm -addr 127.0.0.1:8090`
- PATH 上的 [k6](https://k6.io)

公共环境变量：`BASE_URL`（默认 `http://127.0.0.1:8080`）、`API_KEY`（一个已
seed 的 key；各脚本默认 `bw-local-t1`，`quota-race.js` 用 `bw-local-t2`）、
`RATE`、`DURATION`。`quota-race.js` 额外读取 `ADMIN_TOKEN` 用于 teardown
查询——只要网关以 `BREAKWATER_ADMIN_TOKEN` 运行，它就是必需的。

## 场景

| 场景           | 命令                             | 验证目标                                                       |
| -------------- | -------------------------------- | -------------------------------------------------------------- |
| baseline       | `k6 run baseline.js`             | 纯转发 QPS/P99，网关自身开销                                    |
| cache-hit      | `k6 run cache-hit.js`            | 命中率、命中与回源延迟对比、每 key 恰一次冷回源                  |
| rate-limit     | `k6 run rate-limit.js`           | 100% 超限请求得到 429 + `Retry-After`，上游硬上限                |
| quota-race     | `k6 run quota-race.js`           | 并发消耗对账归零、零差错                                        |
| chaos-upstream | 见文件头                          | breaker 时间线、有界的失败延迟                                  |
| retry-storm    | 见文件头                          | 有界的重试放大                                                  |
| redis-kill     | 见文件头                          | fail-closed 降级与恢复                                          |

`chaos-upstream`、`retry-storm` 与 `redis-kill` 需要在实验中途以特定故障配置
重启 mock upstream 或 Redis；确切步骤写在各文件的头部。

## 读结果

网关侧计数器来自抓取端点：

    curl -s $BASE_URL/metrics

各场景断言的计数器是 `internal/obs/metrics.go` 注册的 `breakwater_*` 指标
族；每个场景自己的文件头写明它读取哪些。发布的数字汇入 `docs/BENCHMARK.md`
与 `docs/CHAOS-REPORT.md`。
