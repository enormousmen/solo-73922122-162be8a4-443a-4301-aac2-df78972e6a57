"""多窗口共享采样 / 外部连接锁 / 采样与开关锁交错 的 HTTP 自测。

前置：已用 docker compose up --build 启动服务，且 5432 已发布到本机。

运行：
    BASE_URL=http://localhost:8000 \\
    PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres \\
    PGPASSWORD=postgres PGDATABASE=orders_demo \\
    python tests/multi_window_test.py

不设置 BASE_URL 时默认访问 http://localhost:8000。
本脚本会打开/关闭两个它自己创建的外部 psycopg2 连接，并调用 /api/reset。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2  # noqa: E402

from app import db  # noqa: E402

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def call(method: str, path: str, body: dict | None = None, timeout: int = 10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE_URL + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode())


def state() -> dict:
    return call("GET", "/api/state")[1]


def wait_until(pred, timeout: float = 6.0, interval: float = 0.1) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = state()
        if pred(last):
            return last
        time.sleep(interval)
    raise AssertionError("条件未满足，最后状态: " + json.dumps(last)[:800])


def ext_holder(table: str):
    """模拟 psql/外部服务：开事务查表并持续持有表级 AccessShareLock。"""
    conn = psycopg2.connect(**db.dsn())
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SELECT pg_backend_pid()")
    pid = cur.fetchone()[0]
    cur.execute(f"SELECT count(*) FROM {table}")
    cur.fetchone()
    return conn, pid


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> int:
    print(f"测试目标: {BASE_URL}")
    try:
        state()
    except Exception as exc:  # noqa: BLE001
        print(f"无法访问服务 {BASE_URL}: {exc}")
        print("请先 docker compose up --build，或设置 BASE_URL。")
        return 2

    call("POST", "/api/reset")
    time.sleep(1.2)
    wait_until(lambda s: s["status"] in ("fresh", "pending"))

    section("1. 多窗口突发请求共享同一份采样")
    c0 = state()["sample_count"]
    n_threads, n_req = 8, 12
    results: list[dict] = []
    barrier = threading.Barrier(n_threads)

    def window() -> None:
        barrier.wait()
        for _ in range(n_req):
            try:
                results.append(state())
            except Exception as exc:  # noqa: BLE001
                results.append({"_error": str(exc)})
            time.sleep(0.03)

    threads = [threading.Thread(target=window) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = [r for r in results if "_error" not in r]
    check("所有窗口请求成功", len(ok) == n_threads * n_req,
          f"{len(ok)}/{len(results)}")
    tuples = Counter((r["sample_count"], r.get("sampled_at")) for r in ok)
    counts = sorted({r["sample_count"] for r in ok})
    check(f"96 次请求只看到极少数采样序号 {counts}", len(counts) <= 3)
    _, top_n = tuples.most_common(1)[0]
    check("多数窗口响应共享同一(采样次数,采样时间)", top_n >= 40, f"n={top_n}")
    check("响应带 lock_sampler 元数据与 server_lock_queue",
          all(isinstance(r.get("lock_sampler"), dict)
              and isinstance(r.get("server_lock_queue"), list) for r in ok))
    check("突发期间没有打出大量真实采样",
          max(counts or [c0]) - c0 <= 2, f"c0={c0} counts={counts}")

    section("2. 默认节奏：持续轮询约每秒最多一轮")
    base = state()["sample_count"]
    t0 = time.time()
    while time.time() - t0 < 3.2:
        state()
        time.sleep(0.1)
    delta = state()["sample_count"] - base
    check(f"3.2s 内采样增量在 2..5（实际 {delta}）", 2 <= delta <= 5,
          f"delta={delta}")

    section("3. 外部连接持续持有的两表锁必须显示")
    e_orders, pid_orders = ext_holder("orders")
    e_items, pid_items = ext_holder("order_items")
    print(f"  外部锁 pid: orders={pid_orders}, order_items={pid_items}")

    seen = {"orders": 0, "order_items": 0}
    stop = threading.Event()

    def hammer() -> None:
        while not stop.is_set():
            try:
                s = state()
                pids = {(r["table_name"], r["pid"])
                        for r in s["server_lock_queue"] if r["granted"]}
                seen["orders"] += ("orders", pid_orders) in pids
                seen["order_items"] += ("order_items", pid_items) in pids
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.05)

    h = threading.Thread(target=hammer)
    h.start()
    wait_until(
        lambda x: any(r["pid"] == pid_orders for r in x["server_lock_queue"])
        and any(r["pid"] == pid_items for r in x["server_lock_queue"]),
        timeout=8,
    )
    check("外部 orders 锁出现", True)
    check("外部 order_items 锁出现", True)
    time.sleep(2.0)
    stop.set()
    h.join(2)
    check("持续刷新时 orders 锁稳定显示(>=10次)",
          seen["orders"] >= 10, str(seen))
    check("持续刷新时 order_items 锁稳定显示(>=10次)",
          seen["order_items"] >= 10, str(seen))

    e_orders.rollback(); e_orders.close()
    e_items.rollback(); e_items.close()
    wait_until(
        lambda x: not any(r["pid"] in (pid_orders, pid_items)
                          for r in x["server_lock_queue"]),
        timeout=8,
    )
    check("外部连接释放后锁消失", True)

    section("4. 演示事务开/关与采样交错")
    history: list[dict] = []
    stop_poll = threading.Event()

    def fast_poll() -> None:
        while not stop_poll.is_set():
            try:
                history.append(state())
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.04)

    p = threading.Thread(target=fast_poll)
    p.start()
    time.sleep(1.2)

    st, opened = call("POST", "/api/demo-tx/open", {"table": "orders"})
    check("open 200 且响应内 pending_update=True",
          st == 200 and opened.get("pending_update") is True
          and opened["lock_sampler"]["pending_update"] is True)
    token, open_pid = opened["token"], opened["pid"]

    immediate = state()
    check("open 后状态自洽（pending 或已刷新含新 pid）",
          immediate["pending_update"] is True
          or any(r["pid"] == open_pid
                 for r in immediate["server_lock_queue"]))

    wait_until(
        lambda x: any(r["pid"] == open_pid and r["table_name"] == "orders"
                      for r in x["server_lock_queue"])
        and x["pending_update"] is False,
        timeout=8,
    )
    check("操作后启动的采样刷新列表并清除 pending", True)

    st, closed = call("POST", "/api/demo-tx/close", {"token": token})
    check("close 200 且响应内 pending_update=True",
          st == 200 and closed.get("pending_update") is True)
    wait_until(
        lambda x: not any(r["pid"] == open_pid
                          for r in x["server_lock_queue"])
        and x["pending_update"] is False,
        timeout=8,
    )
    check("关闭后新采样移除该锁并恢复 fresh", True)

    time.sleep(0.5)
    stop_poll.set()
    p.join(2)
    check("高频轮询中观察到 pending_update=True",
          any(s.get("pending_update") for s in history))
    if history:
        req_n = len(history)
        sample_delta = history[-1]["sample_count"] - history[0]["sample_count"]
        check(f"{req_n} 次状态请求仅产生 {sample_delta} 轮采样",
              sample_delta <= 12 and req_n > 40,
              f"req={req_n} delta={sample_delta}")

    section("5. 状态接口不被采样拖慢")
    lat = []
    for _ in range(10):
        t0 = time.monotonic()
        state()
        lat.append((time.monotonic() - t0) * 1000)
        time.sleep(0.05)
    check(f"/api/state 最大延迟 {max(lat):.0f}ms < 1000ms", max(lat) < 1000)

    section("6. /api/locks 与近义路径")
    for path in ("/api/locks", "/api/sampler", "/api/sample"):
        st, d = call("GET", path)
        check(f"GET {path} 200 且含 sample_count/locks",
              st == 200 and "sample_count" in d
              and isinstance(d["locks"], list))

    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
