# Webhook 投递中枢（webhook-hub）

替业务系统向外部合作方投递 Webhook 的多租户中枢。零第三方依赖（Python 3.11 标准库 + SQLite），一条命令启动完整运行环境。

## 一条命令启动

```bash
./run.sh            # 或 make run / python3 -m hub --demo-receiver-port 9100
```

启动后即包含完整环境：

- **控制面 API**：`http://127.0.0.1:8080`（租户、端点、密钥、事件、重放）
- **投递 worker**：随 hub 进程内运行（每端点一个调度线程）
- **模拟合作方接收端**：`http://127.0.0.1:9100`（可远程注入超时/限流/宕机）

容器化部署：`docker compose up --build`（或 `make docker`）。

跑端到端场景演示（顺序性 → 密钥轮换 → 退避隔离 → 幂等重放）：

```bash
make demo           # 需先 ./run.sh
make test           # 17 个单元/端到端测试
```

## 架构

```
业务系统 ──POST /v1/endpoints/{id}/events──▶ API (api.py)
                                                │
                                          SQLite (store.py)
                                          事件入队即盖章当前密钥版本
                                                │
                              每端点一个 EndpointRunner (dispatcher.py)
                              ├─ CapacityGate：租户配置的并行上限
                              ├─ per-object_key FIFO：同一业务对象
                              │   只允许一个在途事件（队头阻塞）
                              └─ CircuitBreaker：端点级独立退避
                                                │
                                          Deliverer (deliverer.py)
                                   HMAC-SHA256 签名 + HTTP POST + 超时
                                                │
                                          外部合作方接收端
```

## 语义保证

**同一业务对象保序，无关对象并发**
事件携带 `object_key`（业务对象 ID）。调度器只对每个 `object_key` 的最老 pending 事件放行，且同一 key 最多一个在途；失败时该 key 队头阻塞直到重试成功或进入死信，绝不让新事件超车。不同 key 之间完全并行，总量受端点并行上限约束。

**端点级独立退避**
每个端点独立的熔断器：超时 / 连接失败 / 5xx / 429 都会按 `base * 2^n`（带抖动）退避该端点，429/503 的 `Retry-After` 会被遵守。退避只暂停该端点的调度线程，其他租户、其他端点不受影响。连续成功后熔断器复位。

**密钥轮换与端点迁移**
事件入队时盖章当前活跃密钥版本（`key_version`）。轮换后：已排队事件仍用旧版本签名（旧密钥保留为 `rotated` 状态，可继续验签），新事件只走新版本。合作方在迁移窗口内同时接受 v1+v2 即可平滑切换。

**人工重放不重复确认**
- 已送达事件重放：直接返回原始回执，不再发送（hub 侧不产生第二次确认）；
- 失败事件重放：以**同一个事件 ID** 重新入队，接收端凭 `X-Webhook-Id` 幂等去重（接收端先占位再处理，超时重试也不会重复处理）；
- 投递中事件重放：返回 409。

**摄入幂等**
客户端可传 `id` 或 `idempotency_key`，重复提交返回已存在的事件（200），不会重复入队。

**崩溃恢复**
事件状态持久化在 SQLite；重启时 `delivering` 状态的事件自动回到 `pending` 重新调度，不丢单。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/tenants` | 创建租户 |
| POST | `/v1/tenants/{tid}/endpoints` | 创建端点（返回一次性 `signing_secret`，v1） |
| GET / PATCH | `/v1/endpoints/{eid}` | 查看（含熔断器与队列状态）/ 调整 `max_concurrency`、`status` |
| POST | `/v1/endpoints/{eid}/keys/rotate` | 轮换签名密钥（返回新版本密钥） |
| GET | `/v1/endpoints/{eid}/keys` | 密钥版本列表 |
| POST | `/v1/endpoints/{eid}/events` | 投递事件（`object_key` 必填，支持 `id`/`idempotency_key`/`max_attempts`） |
| GET | `/v1/endpoints/{eid}/events` | 事件列表（可按 `status` 过滤） |
| GET | `/v1/events/{id}` | 事件详情 + 投递尝试记录 |
| POST | `/v1/events/{id}/replay` | 人工重放（幂等） |
| GET | `/healthz` | 健康检查 |

### 投递请求头

```
X-Webhook-Id: evt_...                  # 幂等键（同时作为 Idempotency-Key）
X-Webhook-Event: order.updated
X-Webhook-Object-Key: order-123        # 业务对象
X-Webhook-Timestamp: 1726500000
X-Webhook-Key-Version: 2
X-Webhook-Signature: v2=<hmac_sha256_hex(secret_v2, "{ts}.{body}")>
```

接收端验签：用对应版本的密钥对 `"<timestamp>.<原始请求体>"` 做 HMAC-SHA256，与签名头比对。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `HUB_HOST` / `HUB_PORT` | `127.0.0.1` / `8080` | API 监听地址 |
| `HUB_DB` | `data/hub.db` | SQLite 路径 |
| `HUB_DELIVERY_TIMEOUT_MS` | `5000` | 单次投递超时 |
| `HUB_BASE_BACKOFF_MS` / `HUB_MAX_BACKOFF_MS` | `500` / `30000` | 退避基数 / 上限 |
| `HUB_MAX_ATTEMPTS` | `8` | 默认最大尝试次数（之后进死信，可人工重放） |
| `HUB_DEFAULT_MAX_CONCURRENCY` | `4` | 端点默认并行上限 |

## 模拟接收端（演示/联调用）

```bash
POST /__config  {"mode":"ok","latency_ms":0,"retry_after":1,
                 "path_modes":{"/a":"limited"},          # 按路径注入故障
                 "secrets":{"/a":{"1":"...","2":"..."}}} # 验签密钥（支持多版本重叠）
GET  /__receipts / __stats / __order_violations
POST /__reset
```

故障模式：`ok` / `slow`（延迟，触发超时重试）/ `flaky`（500）/ `limited`（429 + Retry-After）/ `down`（直接断连，模拟下线）。

## 设计取舍

- **零依赖**：标准库 `sqlite3` + 多线程即可满足当前语义；要水平扩展时，把 `store.py` 换成 Postgres（`SELECT ... FOR UPDATE SKIP LOCKED` 做队列）、调度器换成多副本 + 端点级租约即可，语义不变。
- **队头阻塞 vs 跳过**：同一 `object_key` 失败的事件会阻塞该 key 后续事件（保序优先）；达到 `max_attempts` 后进死信、放行后续事件，人工重放会把它插回原位置（按 `seq` 重新成为队头），顺序语义不被重放破坏。
- **每端点一线程 + 投递线程池**：端点数量级在千以内时足够简单可靠；更大规模可按端点 hash 分片到固定数量的调度线程。
