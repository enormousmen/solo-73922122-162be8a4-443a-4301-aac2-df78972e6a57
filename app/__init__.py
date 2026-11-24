"""Flask 迁移演示页面。

场景：订单系统发版，需要给 orders / order_items 两表一起加可空批次号列。
值班员可以：
- 打开任一表的演示读事务并保持不结束（持有与 DDL 冲突的表级锁）；
- 提交加列申请，观察迁移在应用层等锁、普通读写照常完成；
- 手动结束演示事务，迁移自动继续，两列在同一事务中一起生效；
- 等待中撤销申请（确认后即便释锁也不执行）；
- 勾选故障注入，验证加列失败时两表结构照旧；
- 试写订单/明细、查询批次号列是否生效。

锁队列采样由进程级单例 :data:`lock_sampler` 统一完成，多个浏览器窗口
共用同一份采样结果，采样节奏与新鲜度语义见 ``app/lock_sampler.py``。
"""
from __future__ import annotations

import threading
import time

from flask import Flask, jsonify, render_template, request

from . import db
# 注意：这里 bare 名 ``lock_sampler`` 保持为“子模块”，不能被同名单例实例
# 覆盖，否则 ``import app.lock_sampler as m`` 会拿到实例而非模块。
# 需要进程级单例时用 ``lock_sampler.lock_sampler`` / get_sampler()，
# 或下面的 ``shared_sampler``。
from . import lock_sampler
from . import sampler  # noqa: F401  # 重导出别名子模块 app/sampler.py
from .lock_sampler import (
    LockSampler,
    get_lock_sample,
    get_lock_sampler,
    get_sampler,
)
from .migration import MigrationManager

app = Flask(__name__)
manager = MigrationManager()

# 进程级共享采样单例的不冲突导出名（``from app import sampler`` 则会拿到
# app/sampler.py 重导出子模块，其上也有 .lock_sampler / .sampler 单例）。
_sampler = lock_sampler.lock_sampler
shared_sampler = _sampler

# 保持打开的演示读事务：{token: {"conn": conn, "table": ..., "pid": ...,
#                                "started_at": ..., "rows": ...}}
_demo_lock = threading.RLock()
_demo_conns: dict[str, dict] = {}


@app.after_request
def _no_store_api_responses(resp):
    """接口数据一律不允许浏览器/中间层缓存，避免多窗口拿到旧响应。"""
    if request.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
    return resp


# ---------------------------------------------------------------- 页面
@app.get("/")
def index():
    return render_template("index.html")


def _held_transactions_locked() -> list[dict]:
    return [
        {
            "token": token,
            "table": item["table"],
            "pid": item["pid"],
            "started_at": item["started_at"],
            "rows": item["rows"],
        }
        for token, item in _demo_conns.items()
    ]


def _mutation_payload(extra: dict, sample: dict) -> dict:
    """组装开/关演示事务的响应：保留原有字段，并附带待更新的旧采样。

    注意不能直接展开 sample：其中的 rows 是“锁行列表”，会覆盖演示事务
    原有的 rows（表行数）字段。
    """
    payload = dict(extra)
    payload.update({
        "pending_update": True,
        "lock_sampler": sample,
        "sampler": sample,
        "lock_sample": sample,
        "sample_status": sample["status"],
        "sampling_status": sample["status"],
        "status": sample["status"],
        "sample_count": sample["sample_count"],
        "actual_sample_count": sample["sample_count"],
        "generation": sample["generation"],
        # 操作成功瞬间的旧列表（sample 内已标 pending_update=True）
        "server_lock_queue": sample["server_lock_queue"],
        "locks": sample["locks"],
    })
    return payload


@app.get("/api/state")
def api_state():
    # 先取迁移状态、再查列：若 snapshot 已报告 succeeded，提交必然已完成，
    # 随后的列查询一定能看到新列，避免“状态成功但 columns=False”的竞态。
    migration_snapshot = manager.snapshot()

    columns = {table: db.column_exists(table) for table in db.TABLES}
    with _demo_lock:
        held = _held_transactions_locked()

    # 非阻塞：只发出采样请求并立即取回缓存快照，绝不等待 pg_locks 查询。
    sample = _sampler.poll()

    payload: dict = {
        "migration": migration_snapshot,
        "columns": columns,
        "migration_done": all(columns.values()),
        "held_transactions": held,
    }
    # 顶层直接给出 server_lock_queue / sample_count / status / in_flight 等
    payload.update(sample)
    # 采样元数据对象的近义挂载点，方便不同版本的前端/测试读取
    payload["lock_sampler"] = sample
    payload["sampler"] = sample
    payload["lock_sample"] = sample
    # 注意：sample 内部 sampling 是“是否采样中”的布尔值；
    # 顶层 sampling 挂载整个元数据对象（对象内仍可取 sampling 布尔）。
    payload["sampling"] = sample
    return jsonify(payload)


@app.get("/api/locks")
@app.get("/api/sampler")
@app.get("/api/sample")
@app.get("/api/lock-sample")
@app.get("/api/lock-sampler")
def api_locks():
    """只看锁采样的轻量接口；同样不阻塞等待采样。

    多个近义路径都返回同一份共享采样快照，方便不同版本的调用方。
    """
    sample = _sampler.poll()
    payload = dict(sample)
    payload["lock_sampler"] = sample
    payload["sampler"] = sample
    payload["lock_sample"] = sample
    payload["sampling"] = sample
    return jsonify(payload)


# ---------------------------------------------------------------- 演示读事务
@app.post("/api/demo-tx/open")
def api_demo_tx_open():
    """在任一表上打开一个保持不结束的读事务。

    显式事务中的 SELECT 会持有该表的 AccessShareLock，它与 ALTER
    需要的 AccessExclusiveLock 冲突，从而制造出等锁场景。
    """
    table = (request.json or {}).get("table")
    if table not in db.TABLES:
        return jsonify({"error": "table 必须是 orders 或 order_items"}), 400

    conn = db.connect()  # 默认 read committed，事务保持打开
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            pid = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM {table}")
            rows = cur.fetchone()[0]
            # 事务保持打开：不 commit / rollback，连接常驻
    except Exception:
        conn.close()
        raise
    token = f"tx-{int(time.time() * 1000)}-{pid}"
    with _demo_lock:
        _demo_conns[token] = {
            "conn": conn,
            "table": table,
            "pid": pid,
            "started_at": time.strftime("%H:%M:%S"),
            "rows": rows,
        }

    # 操作成功：旧锁列表立即标记为待更新，并使操作前出发的采样结果失效。
    sample = _sampler.note_mutation(f"demo-tx open {table} pid={pid}")
    return jsonify(_mutation_payload(
        {"token": token, "table": table, "pid": pid, "rows": rows},
        sample,
    ))


@app.post("/api/demo-tx/close")
def api_demo_tx_close():
    """值班员手动结束演示事务，释放表锁，迁移随后自动继续。"""
    token = (request.json or {}).get("token")
    with _demo_lock:
        item = _demo_conns.pop(token, None)
    if item is None:
        return jsonify({"error": "演示事务不存在或已结束"}), 404
    conn = item["conn"]
    try:
        try:
            conn.rollback()  # 结束只读事务并释放锁
        except Exception:
            # 即便 rollback 失败，下面的 close 也会让服务端释放该连接的锁
            pass
    finally:
        conn.close()

    # 结束成功（连接已关闭、锁已释放）：旧列表标记待更新并作废旧采样。
    sample = _sampler.note_mutation(
        f"demo-tx close {item['table']} pid={item['pid']}"
    )
    return jsonify(_mutation_payload(
        {"closed": token, "table": item["table"]}, sample
    ))


# ---------------------------------------------------------------- 迁移操作
@app.post("/api/migration/submit")
def api_migration_submit():
    payload = request.json or {}
    try:
        manager.submit(inject_failure=bool(payload.get("inject_failure")))
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    return jsonify(manager.snapshot())


@app.post("/api/migration/cancel")
def api_migration_cancel():
    try:
        manager.cancel()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    return jsonify(manager.snapshot())


# ---------------------------------------------------------------- 普通读写
@app.post("/api/orders")
def api_create_order():
    """试写订单 + 明细；迁移完成后可带批次号。"""
    payload = request.get_json(silent=True) or {}
    customer = (payload.get("customer") or "").strip()
    if not customer:
        return jsonify({"error": "customer 必填"}), 400
    items = payload.get("items") or [
        {"sku": "SKU-DEMO", "quantity": 1, "unit_price": "1.00"}
    ]
    cleaned: list[dict] = []
    for raw in items:
        try:
            cleaned.append({
                "sku": str(raw["sku"]),
                "quantity": int(raw["quantity"]),
                "unit_price": str(raw["unit_price"]),
            })
        except (KeyError, TypeError, ValueError):
            return jsonify({"error": "明细字段不合法"}), 400
    try:
        result = db.insert_order(
            customer=customer,
            order_no=(payload.get("order_no") or None),
            items=cleaned,
            batch_no=(payload.get("batch_no") or None),
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 400
    return jsonify(result), 201


@app.get("/api/orders")
def api_list_orders():
    return jsonify(db.list_orders())


@app.get("/api/columns")
def api_columns():
    return jsonify({
        table: {db.BATCH_COLUMN: db.column_exists(table)}
        for table in db.TABLES
    })


# ---------------------------------------------------------------- 重置演示
@app.post("/api/reset")
def api_reset():
    with _demo_lock:
        if _demo_conns:
            return jsonify({"error": "请先结束所有演示事务再重置"}), 409
    try:
        manager.reset()
        db.reset_database()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    # 表被 DROP/CREATE：旧锁采样同样作废，需要重新采样
    sample = _sampler.note_mutation("reset database")
    return jsonify({"ok": True, "lock_sampler": sample, "sampler": sample})


def init_app() -> None:
    db.wait_for_database()
    db.ensure_database()
    db.init_schema(seed=True)
    if db.migration_done():
        manager.state = "succeeded"
    # 启动时同步完成首轮共享采样，保证第一个 /api/state 就有列表；
    # 查询异常已在采样器内部捕获，不会阻断启动（后台会继续重试）。
    try:
        _sampler.run_sample_round()
    except Exception:  # noqa: BLE001
        pass
    _sampler.poll()


__all__ = [
    "app",
    "manager",
    "lock_sampler",
    "sampler",
    "shared_sampler",
    "LockSampler",
    "get_sampler",
    "get_lock_sampler",
    "get_lock_sample",
    "init_app",
]
