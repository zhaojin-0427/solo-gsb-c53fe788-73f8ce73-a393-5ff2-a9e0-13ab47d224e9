# 增量构建裁决台

面向大型单仓库的增量构建裁决服务：导入任务图后，按「定义 + 源文件摘要 +
所读上游产物摘要」判定每个任务**可复用 / 待上游 / 可执行**，并给出最短失效
原因链；执行结果经输入指纹核对后原子落库，过期结果直接拒绝。

- 后端：Python + FastAPI，状态全部存 SQLite（WAL），重启后裁决一致
- 前端：浏览器原生 JavaScript 单页，轮询增量计划（仅返回变动项）
- 规模：导入 10 万任务 / 20 万依赖后，1000 次「领取 → 上报 → 增量计划查询」
  总计约 4 秒（预算 10 秒），单次上报影响 ≤10 项

## 启动（Docker Compose，一键）

```bash
docker compose up --build
```

然后打开构建裁决页面：**http://localhost:8000**

页面打开后点击「导入演示数据」即可看到裁决结果；状态经命名卷
`adjudicator-data` 持久化，容器重启后裁决不变。

## 本地开发启动

```bash
pip install -r requirements.txt
uvicorn app.main:app --port 8000
# 打开 http://127.0.0.1:8000
```

数据库文件默认写在 `./data/build.db`（可用环境变量 `ADJUDICATOR_DB` 覆盖）。

## 页面说明

- 顶栏实时统计三类任务数量：可复用 / 待上游 / 可执行
- 主列表按变更（seq 倒序）展示任务裁决及失效原因，可按状态过滤、按 ID 搜索
- 点击任意任务，右侧展示**最短失效原因链**（一路追到根因：定义变更或
  源文件变更）
- 页面每秒轮询 `GET /api/plan?since=<seq>`，只拉取变动项

## 裁决规则

| 状态 | 含义 |
|---|---|
| `reusable` 可复用 | 定义、源文件摘要、上次读取的全部上游产物摘要均与当前一致 |
| `waiting` 待上游 | 至少一个上游尚未就绪（自身也在等待或可执行），先等待 |
| `executable` 可执行 | 输入已变化（自身定义/源文件变更，或上游产物摘要变更）且上游全部就绪 |

失效原因链：`definition_changed` / `source_changed`（根因，链长 0）→
`upstream_changed`（上游产物摘要变了）/ `upstream_waiting`（上游未就绪），
逐跳指向责任上游，取链长最短者。

关键语义：

- **上游重跑但产物摘要不变 → 下游仍然复用**（按产物摘要比对，而非按是否重跑）
- 上报携带**领取时的输入指纹**，服务端重算当前指纹，不一致即判过期，
  拒绝且不改任何状态
- 上报的**实际读取依赖**会原子替换该任务的动态依赖；若引用缺失任务或
  形成依赖环，整次上报回滚
- 全部状态在 SQLite 中事务化更新，进程重启后裁决结果一致

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/import` | 全量导入任务图（替换式），原子生效；环或缺失引用整体拒绝 |
| `GET` | `/api/plan?since=<seq>` | 增量计划：仅返回 seq 之后的变动项 + 当前计数 |
| `POST` | `/api/tasks/{id}/claim` | 领取可执行任务，返回输入指纹 |
| `POST` | `/api/results` | 上报执行结果（指纹 + 实际读取依赖及摘要 + 新产物摘要） |
| `GET` | `/api/tasks/{id}` | 任务详情（含依赖与被依赖） |
| `GET` | `/api/tasks/{id}/chain` | 最短失效原因链 |
| `POST` | `/api/demo` | 导入内置演示图 |
| `GET` | `/api/docs` | Swagger 交互文档 |

### 导入格式

```json
{
  "tasks": [
    {
      "id": "compile_a",
      "definition": {"cmd": "gcc ..."},        // 本轮定义（任意 JSON）
      "prev_definition": {"cmd": "gcc ..."},   // 上次执行时的定义
      "source_digest": "sha256:...",           // 本轮源文件摘要
      "prev_source_digest": "sha256:...",      // 上次执行时的源文件摘要
      "artifact_digest": "sha256:...",         // 自身产物摘要（上次产出）
      "static_deps":  [{"upstream": "gen_h", "read_digest": "sha256:..."}],
      "dynamic_deps": [{"upstream": "lib_b", "read_digest": "sha256:..."}]
    }
  ]
}
```

`read_digest` 为上次执行时**实际读到的上游产物摘要**；`dynamic_deps` 是上次
执行中发现的动态前置任务。

### 上报格式

```json
{
  "task_id": "compile_a",
  "input_fingerprint": "领取时 /claim 返回的指纹",
  "reads": [{"upstream": "lib_b", "digest": "sha256:..."}],
  "artifact_digest": "sha256:..."
}
```

响应：`accepted`（已原子更新依赖与计划）/ `stale`（输入已过期，未改状态）/
`cycle`、`missing_dependency`（整次回滚）等。

## 规模验证

```bash
# 服务启动后执行：导入 10 万任务 / 20 万依赖，随后 1000 轮 领取+上报+增量计划
python3 scripts/loadtest.py
```

实测（本仓库提交环境）：导入约 8 秒；1000 轮总计约 4.2 秒
（领取 0.9s / 上报 2.3s / 计划查询 1.0s），低于 10 秒预算。

生成同样规模的导入文件（便于 curl 手工导入）：

```bash
python3 scripts/gen_import.py import.json
curl -X POST --data @import.json -H 'Content-Type: application/json' \
     http://127.0.0.1:8000/api/import
```

## 测试

```bash
python3 -m pytest tests/ -q
```

覆盖：全匹配复用、源文件变更置为可执行、下游等待、上游重跑产物不变仍复用、
产物变更使下游失效、过期上报拒绝且零副作用、环/缺失引用整次回滚、动态依赖
替换、重启后裁决一致、增量计划仅返回变动项、最短原因链选择。
