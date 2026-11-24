# 工业传感器时序采集与降采样平台

面向工业传感器高频时序数据的测点接入、边缘缓冲上送、单位标定、时间对齐、降采样归并与分层归档的采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 测点台账 | `sensor_point` | 测点档案 | 测点编号、所属产线、传感器型号 |
| 边缘缓冲 | `edge_buffer` | 缓冲分片 | 分片编号、测点编号、起始时刻 |
| 批量上送 | `upload_batch` | 上送批次 | 批次编号、分片范围、测点数量 |
| 单位标定 | `unit_calibrate` | 标定记录 | 标定编号、测点编号、原始单位 |
| 时间对齐 | `time_align` | 对齐规则 | 规则编号、对齐粒度、时区基准 |
| 断点填补 | `gap_fill` | 填补区间 | 区间编号、测点编号、缺失起点 |
| 尖峰剔除 | `spike_filter` | 异常采样 | 异常编号、测点编号、采样时刻 |
| 降采样 | `downsample` | 降采样任务 | 任务编号、源序列、源采样频率 |
| 窗口归并 | `rollup_window` | 归并窗口 | 窗口编号、测点编号、窗口类型 |
| 序列索引 | `series_index` | 索引分区 | 索引编号、测点编号、时间分区 |
| 冷热分层 | `tier_storage` | 分层策略 | 策略编号、时间边界、热区范围 |
| 采集心跳 | `device_health` | 心跳记录 | 心跳编号、采集终端、上报时刻 |
| 接入水位 | `ingest_watermark` | 水位标记 | 水位编号、测点编号、事件时间水位 |
| 区间重采 | `range_replay` | 重采任务 | 重采编号、测点编号、起始时刻 |
| 采集血缘 | `series_lineage` | 血缘链路 | 链路编号、测点编号、上送批次 |
| 序列导出 | `series_export` | 导出任务 | 导出编号、测点范围、时间范围 |
| 时序质量 | `series_quality` | 质量报告 | 报告编号、测点编号、统计窗口 |
| 序列快照 | `series_snapshot` | 版本快照 | 快照编号、口径说明、测点范围 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA

## 降采样窗口时间迁移（v2）

旧版降采样产物只有窗口序号（`window_index`），新版产物同时记录窗口开始/结束时刻。
迁移在不破坏旧归档的前提下增加一层**时间区间索引**，查询可以逐步从“按旧序号”
切到“按时间区间”，也可以随时回滚。

### 数据产物

| 产物 | 位置 | 用途 |
| --- | --- | --- |
| 时间区间索引 | `data/series_index/<索引编号>_interval.jsonl` | 带 `window_start`/`window_end`，并保留 `window_id`/`window_index` |
| 旧序号索引 | `data/series_index/<索引编号>_legacy.jsonl` | 覆盖全部旧产物（含只有条数的），按 `window_id#window_index` 回读 |
| 构建清单 | `data/series_index/<索引编号>_manifest.json` | 覆盖率、时间粒度、旧窗口标识、待补证项、查询口径 |
| 兼容产物 | `data/series_index/<索引编号>_index.jsonl` | 下游（冷热分层/导出）沿用的旧文件名，内容为时间区间记录 |
| 查询请求日志 | `data/range_queries/<请求编号>.jsonl` | 受理、补证、确认、重放的全过程 |

### 时基判定规则

- **新版产物**自带 `window_start`/`window_end`，直接进入时间区间索引；
- **旧任务**只有在登记了「采集开始时刻」+「窗口宽度」时才可推算区间并进入时间区间索引；
- **只有条数**（或其他无法解析的证据）的旧任务不进入时间区间索引，
  只保留旧序号回读，并在清单的 `pending_evidence` 里列明原因；
- 归并产物的 `window_start` 是**记录序号**，不能当时间；只有归并时由成员降采样
  推导出的 `window_start_time`/`window_end_time` 才可入索引；
- 同序号但时基/粒度不同的旧窗口按 `window_id` 分组，绝不按序号合并。

### 接口

```bash
# 构建（沿用通用资源接口的动作，或直接调用服务）
curl -X POST -H 'content-type: application/json' \
  -d '{"action":"确认构建"}' http://127.0.0.1:8000/api/series_index/1/actions

# 构建清单 / 时间区间查询 / 旧序号回读
curl http://127.0.0.1:8000/api/series_index/IDX-DEMO/manifest
curl -X POST -H 'content-type: application/json' -d '{"start":"2026-09-01T10:00:00Z","end":"2026-09-01T10:20:00Z"}' \
  http://127.0.0.1:8000/api/series_index/IDX-DEMO/query/time
curl -X POST -H 'content-type: application/json' -d '{"window_id":"DOWN-DEMO-B","start_ordinal":1,"end_ordinal":12}' \
  http://127.0.0.1:8000/api/series_index/IDX-DEMO/query/ordinals

# 切换 / 回滚（切换前会核对归并历史依赖仍在索引内）
curl -X POST -H 'content-type: application/json' -d '{"mode":"时间区间"}' \
  http://127.0.0.1:8000/api/series_index/IDX-DEMO/switch
curl -X POST http://127.0.0.1:8000/api/series_index/IDX-DEMO/rollback

# 查询受理 -> 补证 -> 确认；以及当场重放
curl -X POST http://127.0.0.1:8000/api/range_query/1/accept
curl -X POST -H 'content-type: application/json' \
  -d '{"window_id":"DOWN-DEMO-A","anchor":"2026-09-01T10:00:00Z","window_width":"60s"}' \
  http://127.0.0.1:8000/api/range_query/1/verify
curl -X POST http://127.0.0.1:8000/api/range_query/1/confirm
curl -X POST -H 'content-type: application/json' -d '{"metric":"均值"}' \
  http://127.0.0.1:8000/api/range_query/1/replay
```

### 一键迁移验证

```bash
curl -X POST http://127.0.0.1:8000/api/series_index/migration/seed-demo
```

该接口用**两份序号相同、粒度不同（60s/30s）的旧产物 + 一份新版产物**构造
`SENS-DEMO` 测点，依次执行构建、切换到时间区间、按时间查询和回滚，并返回：

- 三类产物各自可定位的实际窗口（`build` / `query`）；
- 回滚后仍可按旧序号读到的分组（`rollback.ordinal_groups`）；
- 缺乏时间依据、只能按旧序号复盘的历史范围（`rollback.ranges_without_time_basis`）。

前端模块列表中新增「区间查询」（`/range_query`），用于查看受理、补证与交付状态。
