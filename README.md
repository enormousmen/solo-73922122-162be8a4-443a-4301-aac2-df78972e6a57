# 订单表批次号在线迁移演示（Flask + PostgreSQL）

订单系统发版场景：给 `orders`（订单表）和 `order_items`（明细表）
**一起**加上可空的批次号列 `batch_no VARCHAR(64)`。页面支持值班员
制造锁冲突、提交/撤销迁移、验证等锁期间普通读写不受影响，以及
失败回滚后两表结构照旧。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- `db`：postgres:16-alpine，带健康检查；
- `web`：Flask 应用，容器入口先等数据库就绪、建库建表并写入样例
  订单（2 笔订单、3 行明细），再启动服务。

停止并清空数据：

```bash
docker compose down -v
```

## 页面操作路径

1. **① 演示读事务**：选择 `orders` 或 `order_items`，打开一个不提交、
   不回滚的读事务（持有表级 `AccessShareLock`），需要时手动结束。
2. **② 加列迁移申请**：提交后状态进入"等待锁中"；可勾选故障注入；
   等待中可撤销（前端有二次确认，撤销后即使释锁也不会执行）。
3. **③ 列生效情况**：实时显示两表 `batch_no` 是否存在。
4. **④ 试写订单/明细**：等锁期间也可写入和查询，页面显示耗时；
   迁移完成后可同时写入订单与明细的批次号。
5. **⑤ 锁队列采样实况（多窗口共享）**：单个 Web 进程内只有一个采样
   器，所有窗口共用同一份 `pg_locks` 采样；面板显示采样状态、实际采样
   次数、成功/失败/丢弃计数与上次开始/成功时间。外部连接（psql / 其他
   服务）在两表上持续持有的锁会带“外部/其他会话”标记显示。等锁期间只应
   看到演示事务**已授予**的锁，不应出现 `granted=false` 的 ALTER 排队记录。
6. **⑥ 样例数据**与事件日志：页面每秒自动刷新。

## 核心设计：为什么等锁期间普通读写不被堵住

PostgreSQL 中 `ALTER TABLE ... ADD COLUMN` 需要 `ACCESS EXCLUSIVE`
锁。若直接发起而表上有未结束事务，ALTER 会进入**服务器端锁等待队列**，
后续到达该表的普通读写会排在 ALTER 后面一起被阻塞。

本项目的迁移线程因此**不进入服务器端锁队列**（见 `app/migration.py`）：

1. 每 0.5s 用独立连接查 `pg_locks`，判断两表上是否存在与
   `ACCESS EXCLUSIVE` 冲突的已授予表级锁（含读事务的
   `AccessShareLock`，共 7 种模式）；
2. 无冲突时才在**同一个数据库事务**里执行两条 `ALTER TABLE`，
   并设置 `lock_timeout=250ms` 兜底；
3. 兜底超时（SQLSTATE `55P03`）则回滚本轮、重新进入等待循环；
4. 两张表的 ALTER 同事务提交：任一失败整体回滚，保证两表结构一致；
5. 撤销通过 `threading.Event` 置位，等待循环与执行前双重检查，
   确认撤销后永不执行。

## 共享锁采样器：多窗口为什么看到同一份结果

实现在 `app/lock_sampler.py`，进程级单例 `lock_sampler`
（`app/__init__.py` 在 `/api/state` 中调用其非阻塞 `poll()`）。

- **多窗口共享**：浏览器开多少个窗口都只请求同一个 Flask 进程，
  `/api/state` 不直接查 `pg_locks`，而是立即返回单例缓存，因此各窗口
  看到的 `sample_count / sampled_at / server_lock_queue` 完全一致。
- **采样节奏**：任意两轮采样的**开始时刻**至少相隔
  `MIN_SAMPLE_INTERVAL_SECONDS = 1.0s`；只有一个后台采样线程串行执行，
  且 `run_sample_round()` 有 `_in_flight` 单飞保护——上一轮没结束绝不
  发起下一轮。
- **查状态不等采样**：`poll()` 只置位“有请求”事件并返回缓存快照，
  不做数据库 IO、不 join 采样线程。
- **待更新 + 迟到采样栅栏**：演示读事务 open/close（以及 reset）成功后
  调用 `note_mutation()`，令 `generation += 1`、旧列表立即标
  `pending_update=true`。每轮采样开始时记下 generation，结果返回时若
  generation 已变，说明该轮在操作之前就已启动，结果**整体丢弃**
  （`discarded_count += 1`），既不刷新锁列表，也不改新鲜度标记；随后
  自动补一轮“操作之后启动”的采样，成功后清除待更新。open/close 的 HTTP
  响应体本身就带 `pending_update=true` 与操作瞬间的旧列表。
- **失败保留与自愈**：采样抛错时保留上一次成功的列表，置
  `expired=true / status="stale"`（从未成功过则 `status="error"`），
  后台按 1s 节奏继续重试；之后任意一轮 generation 匹配的采样成功，
  立即用新列表替换并清除 `expired / last_error`。
- **外部连接锁可见**：采样 SQL 不过滤 `pid`，外部 psql / 其他服务只要在
  `orders` / `order_items` 上持有锁（含 `AccessShareLock`、
  `RowExclusiveLock` 等）都会出现；前端用 PID 与本页演示事务区分来源。
- **迁移仍用实时锁信息**：`app/migration.py` 没有改用这份最多 1s 旧的
  缓存，仍是每 0.5s 用独立连接实时查询冲突锁来决定何时加列。

`/api/state` 中与采样相关的主要字段（同时以 `lock_sampler` / `sampler`
/ `lock_sample` 对象挂载，并在顶层给了近义别名）：

| 字段 | 含义 |
| --- | --- |
| `status` / `sample_status` | `loading` / `fresh` / `pending` / `stale` / `error` |
| `status_label` | 中文状态（首次采样中 / 正常 / 待更新 / 已过期 / 采样失败） |
| `pending_update`（等同 `pending`/`needs_update`/`dirty`） | 操作已发生，等待操作后启动的采样刷新 |
| `expired`（等同 `stale`/`is_stale`） | 最近采样失败，展示的是上次成功的过期列表 |
| `sample_count`（等同 `actual_sample_count` 等） | **实际发起的采样轮数**，不是 HTTP 请求数 |
| `success_count` / `failure_count` / `discarded_count` | 成功 / 失败 / 被 generation 丢弃的轮数 |
| `in_flight` / `sampling` | 当前是否有一轮采样正在执行 |
| `last_started_at` / `sampled_at` / `last_error` / `generation` | 节奏、新鲜度与栅栏代号 |
| `server_lock_queue` / `locks` | 最近一次被采纳的锁行列表 |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/` | 演示页面（多窗口共享同一服务端采样） |
| GET  | `/api/state` | 迁移状态、两列是否生效、演示事务、**共享锁采样快照** |
| GET  | `/api/locks`（别名 `/api/sampler`、`/api/sample`、`/api/lock-sample`、`/api/lock-sampler`） | 只取共享锁采样状态与锁列表，非阻塞 |
| POST | `/api/demo-tx/open` | 在任一表打开保持不结束的读事务；响应即标 `pending_update` |
| POST | `/api/demo-tx/close` | 手动结束演示事务（释放锁）；响应即标 `pending_update` |
| POST | `/api/migration/submit` | 提交加列申请（`inject_failure` 可故障注入） |
| POST | `/api/migration/cancel` | 撤销等待中的申请 |
| GET/POST | `/api/orders` | 查询订单明细 / 试写订单+明细 |
| GET  | `/api/columns` | 两表 batch_no 列是否存在 |
| POST | `/api/reset` | 重建两表与样例数据（迁移进行中或有演示事务时拒绝） |

## 测试

```bash
# 进程内模式（Flask test client，走完整 WSGI 栈）
PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres \
PGDATABASE=orders_demo python tests/e2e_test.py

# 或对已运行的服务发真实 HTTP
BASE_URL=http://localhost:8000 python tests/e2e_test.py

# 采样器时序单测（假查询，不需要数据库）：迟到采样栅栏 / 过期保留 /
# 1 秒节奏 / 单飞 / 失败自愈，30 项断言
python tests/sampler_test.py

# 多窗口 + 外部连接锁 + 采样/释锁交错的真实 HTTP 自测（22 项断言，
# 需要 compose 服务与已发布的 5432 端口）
BASE_URL=http://localhost:8000 python tests/multi_window_test.py
```

原有 e2e 覆盖 8 个场景、27 项断言：初始无列与样例数据、等锁状态、
等锁期间普通读写毫秒级完成且服务器端无排队 DDL、撤销后释锁也不执行、
释锁自动续跑两列同生效、批次号可写可读、故障注入后两表结构照旧、
重置恢复。

### 本次交付前的实测结果

开发沙箱内没有 Docker daemon，因此用等价的真实进程方式自测：
用户态 PostgreSQL 16.2（unix socket `/tmp/pgdata`）+ 实际
`python -m app` / `app.run` HTTP 服务 + Flask test client。

- `tests/e2e_test.py` 进程内：**27/27 通过**；对真实 HTTP 服务连跑
  **3 次均 27/27**。期间顺手修了一个既有竞态：`/api/state` 原来先查
  列、后取迁移状态，可能返回 `state=succeeded` 但 `columns=false`；
  改为先取迁移快照再查列后稳定通过。
- `tests/sampler_test.py`：**30/30 通过**（含默认常量确为 1.0s、
  0.2s 缩比节奏下所有开始间隔达标、慢查询最大并发为 1、`poll()`
  耗时 < 1ms、迟到采样被丢弃、失败过期与自愈）。
- `tests/multi_window_test.py` 对真实 HTTP：**22/22 通过**。8 窗口 ×
  12 次共 96 个并发突发请求只出现 1～2 个采样序号（断言 ≤3）、多数响应
  共享同一 `(sample_count, sampled_at)`；3.2s 轮询采样增量为 3；外部
  psycopg2 连接在两表持续持有的锁在 2s 高频刷新中稳定显示（断言 ≥10 次，
  实测接近 40 次满采样），释放后消失；open/close 均观察到
  `pending_update` 与随后的自动刷新；`/api/state` 最大延迟约 30ms。
- 另跑了未入库的真实 PG 交错脚本 **18/18**：用“先真实查询捕获锁状态、
  再卡门延迟返回”的查询桩，分别构造“含旧锁的晚到样本跨过释锁”和
  “不含新锁的晚到样本跨过开锁”，确认均被 generation 丢弃、列表不被
  回刷，操作后的新采样才刷新；以及 API 级故障注入下保留列表→过期→
  恢复 fresh。
- 前端内联 JS 通过 `node --check`，并用 jsdom 对 fresh/pending/stale/
  error/loading 五种渲染做了 **11/11** DOM 断言；所有
  `getElementById` 目标与按钮函数均存在。

### 尚未在本环境完成的验证 / 已知边界

- 沙箱内**没有 docker / docker compose / docker.sock**，因此
  `docker compose up --build` 本身没有在本机实际跑过；已校验
  `docker-compose.yml` YAML 合法，Dockerfile 未改且
  `COPY app ./app` 会包含新增的 `app/lock_sampler.py`、`app/sampler.py`，
  依赖仍只有 Flask 与 psycopg2-binary（本地用锁定版本 Flask 3.1.3 /
  psycopg2-binary 2.9.13 实测）。**首次交付环境建议实跑一次
  `docker compose up --build` 做最终确认。**
- 采样状态保存在单进程内存；compose 只启动 1 个 web 副本，符合
  “单个 Web 进程”前提。若以后改成多 worker / 多副本，需要把采样器
  下沉到 PostgreSQL 或独立共享服务。
- 采样列表最长有约 1s 的展示延迟（这是题目要求的最小采样间隔）；
  迁移判定不使用该缓存，仍为 0.5s 实时探测 + 250ms lock_timeout。

## 目录结构

```
app/
  __init__.py        Flask 路由、演示事务管理、把共享采样挂到 /api/state
  __main__.py        python -m app 入口
  db.py              连接、建表、样例数据、订单读写
  lock_sampler.py    进程级共享 pg_locks 采样器（1s 节奏/单飞/栅栏/过期保留）
  sampler.py         名称兼容别名，重导出 lock_sampler
  migration.py       迁移状态机与应用层等锁/重试/撤销（仍实时查锁）
  templates/index.html  单页界面（采样状态/次数/待更新/过期提示）
tests/e2e_test.py          原有端到端测试（27 项）
tests/sampler_test.py      采样器时序单测（30 项，无需数据库）
tests/multi_window_test.py 多窗口/外部锁/交错的 HTTP 自测（22 项）
Dockerfile / docker-compose.yml / docker-entrypoint.sh
```

## 已知边界

- 这是演示环境：迁移状态保存在进程内存，重启后通过检查列是否存在
  恢复为"已完成/空闲"；生产场景应使用持久化的迁移记录表。
- 探测间隔 0.5s、兜底 `lock_timeout` 250ms，是为演示把影响压到
  亚秒级；高并发生产环境建议再评估参数。
- `ADD COLUMN ... 可空无默认值` 在 PG 11+ 本身是快速元数据操作，
  本演示重点是锁等待行为与原子性，不涉及大表重写。
