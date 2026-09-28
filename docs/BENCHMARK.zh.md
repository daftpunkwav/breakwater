# 基准测试

> 语言：**简体中文** | [English](BENCHMARK.md)

> 状态：方法论与场景集已定稿；下表数字由在真实硬件上运行 loadtest 场景
> 填入。本页没有任何数字是估计值——空单元格表示"尚无在案测量"，绝不是
> "假定"。

## 方法

- 负载生成器：k6（`loadtest/`），constant-arrival-rate executor，使施加的
  负载与系统响应时间无关。
- Upstream：`mockllm`，默认 completion 长度 32 词，使各次运行之间的字节量
  可比。mock 也接受 `X-Mockllm-Completion-Tokens`，但网关不向 upstream 转发
  任何客户端 header，所以经网关的运行始终使用默认值。
- 机器与进程布局、Go 版本、Redis/PostgreSQL 版本随每次运行记录在下方。
- 延迟是端到端客户端侧的（k6 `http_req_duration`）；网关内部指标事后从
  `/metrics` 读取，绝不混入。

## 环境（每次运行）

| 字段         | 值 |
| ------------ | ----- |
| 日期         | _待填_ |
| 机器         | _待填_ |
| Go           | _待填_ |
| Redis        | _待填_ |
| 拓扑         | gateway 与 mockllm 同机 / 分离 |

## 场景

| 场景          | 施加速率      | 吞吐       | P50 | P99 | 错误画像 |
| ------------- | ------------ | ---------- | --- | --- | ------------- |
| baseline      | _待填_       |            |     |     |               |
| cache-hit     |              |            |     |     | 命中率：      |
| rate-limit    |              |            |     |     | 429 占比、上游上限： |
| quota-race    |              |            |     |     | 对账漂移（必须为 0）： |
| chaos-upstream|              |            |     |     | breaker 时间线： |
| retry-storm   |              |            |     |     | 放大系数：     |
| redis-kill    |              |            |     |     | fail-closed 行为： |

## 网关自身损耗

验收目标：upstream 零延迟时，网关的 P99 转发开销应保持在个位数毫秒。

| 运行 | mockllm 直连 P99 | 经网关 | 差值 |
| --- | ------------------ | --------------- | ----- |
|     |                    |                 |       |

## 复现

上表每个数字来自 `loadtest/` 中的一条命令（chaos 变体见各文件头）。环境
旋钮：`BASE_URL`、`API_KEY`、`RATE`、`DURATION`。
