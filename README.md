# Data Quality Gate API（数据质量闸门）

基于 **Python + FastAPI + SQLAlchemy + SQLite** 的数据质量闸门服务，使用 **Docker Compose** 一键启动。
用户提交带 `schema_version` 的批次数据与质量规则，服务执行字段约束、跨记录唯一性与批次级阈值校验，
生成带失败原因的校验报告；**仅当批次通过时**才登记为可供下游读取的批次版本。

## 一、快速开始

```bash
docker compose up --build -d
# 服务地址 http://localhost:8000，交互式文档 http://localhost:8000/docs
curl http://localhost:8000/health
```

数据持久化在 Docker 卷 `dqg-data`（容器内 `/data/dqg.db`），容器重启后已登记批次与幂等记录不丢失。

本地无 Docker 时可直接运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 二、接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/batches/submit` | 提交批次进行质量校验（幂等） |
| GET | `/batches` | 列出所有**已登记**（通过）的批次版本 |
| GET | `/batches/{batch_id}` | 下游读取已登记批次明细；失败/未登记返回 404 |
| GET | `/submissions/{batch_id}` | 查询提交状态与完整校验报告（含失败原因） |
| GET | `/submissions/{batch_id}/report` | 仅查询校验报告 |
| GET | `/submissions/{batch_id}/rules` | 查询该提交实际使用的规则快照 |
| GET | `/catalog/rules` | 查询支持的规则类型目录与参数说明 |
| GET | `/catalog/samples` | 查询示例提交载荷（通过 / 失败各一个） |

## 三、规则模型

规则随每次提交内联传入，分三类：

1. **字段约束 `field_rules`**：按字段配置，支持
   `required`、`type`（string/integer/number/boolean）、`min`、`max`、
   `min_length`、`max_length`、`in`（枚举）、`regex`（正则）。
2. **跨记录唯一性 `unique_rules`**：按一个或多个字段的组合在整批内去重；
   任一组成字段为 `null` 的记录跳过唯一性判定。
3. **批次级阈值 `batch_rules`**：
   - `max_failure_rate`（必填，0~1）：失败记录占比上限，超过即整批失败；
   - `min_record_count` / `max_record_count`：批次记录数上下限。

任一规则不满足则批次状态为 `FAILED`，报告中同时返回全部字段失败、唯一性冲突与批次阈值失败及中文原因。

## 四、关键语义

- **幂等**：以 `batch_id` 为幂等键。重复提交相同请求体返回与首次完全一致的报告（`replayed=true`），
  不会重复登记；相同 `batch_id` 但请求体（schema/records/rules）不一致返回 `409`。
- **失败不留可读批次**：`FAILED` 批次只保留提交元数据与报告供排查，不写入 `batches`/`batch_records`，
  下游读取返回 404；`PASSED` 批次才在同一事务内登记批次与全部明细。
- **schema 版本**：每个批次显式携带 `schema_version`，登记与查询均按版本记录。

## 四之二、下游读取采用流式输出

`GET /batches/{batch_id}` 不再先把整批明细读进内存再序列化，而是**按页读库、边读边编码边发送**：

| 关注点 | 实现 |
| --- | --- |
| 首段先到 | 先发批次头 `{batch_id,...,"records":[`，再发首页明细（`DQG_STREAM_FIRST_SEGMENT_RECORDS`，默认 20 行），不等整批读完 |
| 内存不随行数增长 | 每页只取 `DQG_STREAM_PAGE_SIZE`（默认 500）行；按记录逐条编码，攒到 `DQG_STREAM_CHUNK_BYTES`（默认 64 KiB）即发送并丢弃 |
| 不长期占用读事务 | 每页用独立 Session，keyset 分页（`record_index > cursor`），读完立即关闭；向下游阻塞发送期间不持有 SQLite 读事务，其他批次可继续登记，WAL 可检查点回收 |
| 失败不交付“缺行的完整 JSON” | 每条记录校验 `record_index` 从 0 连续递增、总数等于登记时的 `record_count`；读库或编码异常会中止连接，**不会**补发收尾的 `]}`，客户端只能得到传输错误/不完整 JSON |
| 客户端断开释放资源 | 生成器被取消/关闭时清理缓冲；每页数据库连接已在工作线程内关闭 |
| 兼容性 | 输出与原先 `RegisteredBatchOut` 的默认编码**逐字节一致**（紧凑分隔符、`ensure_ascii=False`、字段顺序与取值不变）；未登记批次仍在响应开始前返回 404 |

可调环境变量：`DQG_STREAM_PAGE_SIZE`、`DQG_STREAM_CHUNK_BYTES`、
`DQG_STREAM_FIRST_SEGMENT_RECORDS`、`DQG_DEMO_ENABLED`（均为演示/调优用，
`Dockerfile` 与 `docker-compose.yml` 已给出默认值）。

### 在 /docs 中演示

`docker compose up --build -d` 后打开 <http://localhost:8000/docs>，使用 **demo** 分组：

1. `POST /demo/batches?records=20000` 生成并登记一个大批次，返回三个演示 URL；
2. `GET /demo/batches/{batch_id}/slow?delay_ms=300`：慢速接收（每个 chunk 前等待），
   期间可并行调用 `POST /demo/batches` 登记新批次、`POST /demo/db/checkpoint` 观察 WAL 可回收；
3. `GET /demo/batches/{batch_id}/fail?after_records=30`：输出若干条后中止，验证拿不到完整 JSON；
4. `GET /demo/batches/{batch_id}/head?bytes=200`：只读取响应前缀即关闭，证明首段无需等整批；
5. `GET /demo/metrics`：查看页/块/内存上界等运行指标。

## 五、请求示例

```bash
curl -X POST http://localhost:8000/batches/submit \
  -H "Content-Type: application/json" \
  -d "$(curl -s http://localhost:8000/catalog/samples \
      | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["examples"][0]["payload"]))')"
```

## 六、目录结构

```
app/
  main.py        # FastAPI 入口（lifespan 建表；DQG_DEMO_ENABLED 时挂载 demo 路由）
  config.py      # 环境变量配置（含流式分页/块大小）
  database.py    # SQLAlchemy 引擎/会话/SQLite PRAGMA
  models.py      # ORM：submissions / batches / batch_records
  schemas.py     # Pydantic 请求与报告模型
  validation.py  # 纯函数校验引擎（三类规则）
  services.py    # 幂等提交、事务登记、批次元数据与按键集分页查询
  streaming.py   # 流式读取与增量 JSON 编码（首段、常量内存、短读事务、完整性校验）
  routers.py     # HTTP 路由
  demo.py        # 演示路由：大批次、慢速接收、中途失败、WAL 检查点、指标
data/
  rule_catalog.json     # 规则目录
  sample_payloads.json  # 示例数据
tests/test_api.py         # 接口自测（pytest + TestClient）
tests/test_streaming.py   # 流式语义自测（结构一致、首段、内存、短事务、失败/断开）
tests/test_demo.py        # 演示路由自测
tests/test_concurrency.py # 并发幂等自测
```

## 七、自测

```bash
pip install -r requirements.txt pytest httpx
pytest tests/ -v
```

`tests/test_streaming.py` 覆盖：与旧编码逐字节一致、字段/下标顺序、空批次、
首段先于后续页发出、峰值内存不随行数线性增长、每页短事务、
慢速下游期间 WAL 可截断检查点且其他批次可登记、中途失败不补收尾、
落库缺行被检出、客户端断开清理。
