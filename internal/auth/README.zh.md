# auth/

> 语言：**简体中文** | [English](README.md)

治理管线的身份层：API key、tenant、role、tier 与分层限额模型。它只负责
key 解析（`Store`）与身份管理（`AdminStore`）；不负责执行——RPM/TPM 桶与
并发闸门在 [`internal/limiter`](../limiter/)，余额账本在
[`internal/quota`](../quota/)。它是推理管线的第一阶段：auth → model
authorization → concurrency → limiter → quota → cache。

`Store` 有两个实现：`Static`（`BREAKWATER_IDENTITY` JSON，用于本地开发与
证据运行）和 `PGStore`（PostgreSQL，`BREAKWATER_POSTGRES_DSN` 部署的 system of
record）。组合根会将其中任一包进进程内 `CachedStore` LRU，使稳态解析不落
分布式 I/O。只有 `PGStore` 实现 `AdminStore`（即 `/admin/users` 与 `/admin/keys`
管理面）；static 模式是配置，不是管理面。

## Files

| File | Role |
| --- | --- |
| `auth.go` | 契约：`Tenant`、`Tier`、`Store`、`ErrUnauthorized`、`AllowsModel`（deny 优先于 allow；空 allow 列表一律拒绝） |
| `limits.go` | `LimitOverride` 与 `MergeTier`：用户层、key 层覆盖合并为生效 tier |
| `static.go` | 基于配置 JSON 的 `Static` store；构建时即哈希 key；`Tenants()`/`TenantByID()` 供余额播种使用 |
| `pg.go` | `PGStore`：按 `key_hash` 一次 join `api_keys`/`tenants`/`tiers`，仅 `status = 'active'` |
| `lru.go` | `CachedStore`：LRU + TTL 装饰；缓存正向结果与确定性 `ErrUnauthorized` 负结果，绝不缓存瞬时故障 |
| `admin.go` | `AdminStore` 端口、`MaxKeysPerUser`（5）、`GenerateKey`（`bw-` 前缀，明文只在签发时出现一次） |
| `pgadmin.go` | PostgreSQL 版 `AdminStore`：用户/key 生命周期、覆盖写入、事务化签发 key |

## Tests

合并语义、LRU 行为与 static 身份为单元测试；pg 与 pgadmin 的集成测试需要
PostgreSQL（`BREAKWATER_TEST_POSTGRES_DSN`）。

## Invariants

- key 只以 HMAC-SHA256 哈希存储与查找，HMAC 密钥为 `BREAKWATER_KEY_PEPPER`
  （数据库与静态集一致）；明文不落任何持久化存储——它只出现在 `CreateKey`
  响应中一次，其余时间仅存在于进程内存（含解析缓存）。更换 pepper 会使所有
  已持久化的 key_hash 失效。
- store 返回合并后的快照（`MergeTier`）；治理层永不重复合并。标量取最近
  一层设置值（key 覆盖 user 覆盖 tier）；`denied_models` 只做并集；
  `allowed_models` 只做交集。损坏的 overrides 文档会使解析失败，而不是被
  静默跳过。
- 撤销延迟等于缓存正向 TTL（组合根为 60s，负向 5s），这是公开契约的一部
  分；瞬时后端故障永不入缓存。
- `Role` 只是注记，不是执行边界：管理面由 `BREAKWATER_ADMIN_TOKEN` 把守，
  两种 role 的推理治理完全相同。
- static 模式下 `Tenant.KeyID` 为空；按 key 归因与按 key 覆盖是数据库部署
  才有的能力。
