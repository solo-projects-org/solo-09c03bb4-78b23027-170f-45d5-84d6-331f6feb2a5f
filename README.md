# 逐笔成交供数 API（FastAPI + SQLite）

盘中风控断线恢复场景：接收交易所逐笔消息（成交 / 更正 / 撤销），按分钟聚合成 K 线柱，
风控侧首次全量快照 + 游标，之后用游标续拉变动分钟，保证不重、不漏、不看到半提交状态。

## 快速开始

```bash
docker compose up --build
# 打开 Swagger UI: http://localhost:8000/docs
```

在 Swagger 上可直接 `POST /v1/trades` 提交逐笔消息、用 `/v1/symbols/{symbol}/bars`
拿快照和游标、再用 `/v1/symbols/{symbol}/changes?cursor=...` 续拉变动。

本地开发（无 Docker）：

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --port 8000          # 默认库文件 ./feed.db，可用 FEED_DB_PATH 覆盖
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/trades` | 受理一条逐笔消息（成交/更正/撤销） |
| GET | `/v1/symbols/{symbol}/bars` | 风控首次查询：该股票全部分钟柱 + 游标 |
| GET | `/v1/symbols/{symbol}/changes?cursor=N&limit=500` | 用游标续拉变动分钟 |
| GET | `/v1/symbols/{symbol}/changes/{seq}` | **按变更序号溯源**：当时该分钟构成 + 开收盘来源 + 后来差异 |
| GET | `/v1/symbols/{symbol}/trades/{trade_no}/changes?limit=500` | **按交易号**列出它触发的全部变更（各条均可溯源） |
| GET | `/v1/trades/{symbol}/{trade_no}` | 查看某笔逐笔当前状态（调试） |

逐笔消息：

```json
{
  "symbol": "600519", "trade_no": "000123", "revision": 1,
  "op": "trade",                       // trade | correction | cancel
  "trade_time_utc_ms": 1700000000000,  // 成交时刻，UTC 毫秒
  "price_cents": 168500,               // 价格，整数，单位分
  "qty": 100                           // 股数
}
```

## 语义规则

**修订号（按 股票代码+交易号 维度）**

* `revision > 已存`：受理（允许跳号）；`cancel` 打撤销标记，`trade/correction` 覆盖内容
* `revision == 已存` 且内容相同：`duplicate`，幂等成功，不产生任何变更
* `revision == 已存` 但内容不同：**409 拒绝**
* `revision < 已存`：`stale_ignored`，迟到旧修订只忽略、**绝不回退**新状态
* 对未知逐笔的 `cancel` 记为墓碑，防止迟到的旧成交"复活"

**分钟柱口径**：开/收盘价 = 该分钟按 `(成交时刻, 交易号)` 排序的第一/最后一笔价格
（纯数字交易号按数值比较）；高/低 = 最大/最小价；另含总股数与笔数。

**游标续拉**

* 快照返回的 `cursor` 与返回的分钟柱是同一读事务里的一致点
* 跨分钟更正在同一事务里交付**两个分钟**的变化（旧分钟更新或删除 + 新分钟上线）
* 某分钟最后一笔被撤销/改走时，交付 `type=delete`、`bar=null` 的**删除标记**
* 游标超过已提交水位返回 409，提示重新快照

**一致性**：每次写入把"逐笔状态 + 分钟柱重算 + 变更日志"放进**同一个 SQLite 事务**
（`BEGIN IMMEDIATE`），失败整体回滚；读取只在单事务里读已提交数据；
WAL + `synchronous=FULL`，进程重启后已提交游标不会丢、不会跳号、不留半更新。

## 分钟构成溯源

对启用本功能后产生的每条 `upsert` / `delete` 变更，提交事务里**额外**冻结：

* 该变更由哪条消息产生（交易号、修订号、op、成交时刻、价格、股数、服务端提交时刻）；
* 该分钟**提交前**与**提交后**的有效逐笔构成（交易号、修订、成交时刻、价格、股数），
  以及开/收盘价分别由哪笔成交提供。

历史构成是**冻结副本**（`journal_trades` 独立存行），不引用 `trades`，
因此后续任何更正/撤销都不会改写它。`delete` 变更的 `historical_trades`
取"提交前"构成，即被删除掉的那些成交；`composition_after_commit` 为空列表。

查询 `GET /v1/symbols/{symbol}/changes/{seq}` 返回：

| 字段 | 含义 |
|---|---|
| `bar` / `type` | 该变更提交时的分钟柱（`delete` 为 null） |
| `caused_by` | 触发本变更的消息及其该次提交的修订/时刻/价格/股数 |
| `historical_trades` | 当时的逐笔构成（冻结） |
| `composition_before_commit` / `composition_after_commit` | 提交前/后两份冻结构成 |
| `open_trade_no` / `close_trade_no` | 当时开盘价/收盘价的来源交易号 |
| `current_trades` / `current_open_trade_no` / `current_close_trade_no` | 现行同一分钟的构成与来源 |
| `diff.removed` | 历史有、现在已不在：`cancelled`（当前状态确认已撤销）或 `moved_to_other_minute`（仍有效、已移至别的分钟） |
| `diff.appeared` | 历史没有、现在该分钟出现 |
| `diff.changed` | 同一笔仍在但修订/时刻/价格/股数改过，逐字段给历史值与现行值 |
| `related_changes` | 同一次提交产生的另一条变更（跨分钟更正的两条各自可追） |
| `historical_available` | 本功能启用前写入的老变更没有历史，为 `false`（绝不按现行状态倒推） |

`GET /v1/symbols/{symbol}/trades/{trade_no}/changes` 按交易号列出全部相关变更（倒序），
适合从"某笔成交"出发追它自己引发过的每次分钟变化。

一致性：每次溯源查询在**一个读事务**内取历史构成、现行构成与现行逐笔状态，
与并发更正要么完全在其之前、要么完全在其之后，历史与现行不会跨提交混读。
历史构成、变更日志（含溯源行）与逐笔状态同一事务提交，写入失败或重启不会缺一份。

开关：`FEED_TRACE_ENABLED=0` 只停止为新写入留痕；已留痕的历史继续可查。
默认开启。老库升级无需迁移：溯源表 `CREATE TABLE IF NOT EXISTS` 自动建立。

## 自测

```bash
.venv/bin/pip install pytest httpx
python -m pytest tests/ -v        # 39 个关键场景用例
```

覆盖：OHLCV 口径与同毫秒按交易号决胜、幂等重送、同修订冲突 409、迟到旧修订不回退、
跳号受理、同分钟/跨分钟更正（两分钟交付）、最后一笔撤销的删除标记、游标续拉流程、
游标超前 409、**重启后状态与游标连续**、**写入中途失败整体回滚**（monkeypatch 注入故障）、
参数校验、未知逐笔撤销墓碑；溯源侧：**历史构成不被后续更正覆盖**、开收盘来源、
消失/出现/改值分类、撤销与移出确认、delete 留痕、**跨分钟更正两条变更各自可追且互相引用**、
按交易号追查、同修订冲突/幂等重送不产生溯源、**读事务内历史与现行同一快照**
（另开写连接中途提交更正）、**失败写入不留下孤立溯源**、重启后溯源仍在、老日志如实标记不可用、
开关只影响新写入、OpenAPI 暴露端点。

## 限制与待办

* SQLite 单写者：容器固定单 worker；多实例水平扩展需换外部数据库（接口与事务边界不变）
* 未做鉴权/限流、数据保留期清理、批量提交接口
* 溯源表按变更条数增长（每条约 1 行溯源 + 分钟构成副本），尚无归档策略
* 未实现完整历史重放：溯源只能回答"某条已提交变更当时是什么"，无法重建未留痕的老日志
* 本环境无 Docker 守护进程，镜像未实际构建；Dockerfile/compose 经静态检查，
  同一入口（`uvicorn app.main:app`）已在本地用 venv 完整跑通上述场景（含启动后 HTTP 实测
  `/v1/trades`、`/v1/symbols/{symbol}/changes/{seq}`、`/trades/{trade_no}/changes`、
  `/openapi.json` 及重启后溯源仍在）
