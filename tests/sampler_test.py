"""共享锁采样器的确定性时序测试（不需要 PostgreSQL）。

运行方式（仓库根目录）：
    python tests/sampler_test.py

覆盖题目里最容易出错的时序语义：
  A. 演示事务操作前已启动的“迟到采样”不能刷新列表（generation 栅栏）；
  B. 采样失败保留上次成功列表并标 expired，后续成功自动恢复；
  C. 从未成功时失败 -> error；
  D. 后台循环任意两轮“开始时刻”间隔 >= min_interval；
  E. 慢采样期间 poll/快照立即返回，且采样单飞、最大并发为 1；
  F. 失败后后台按节奏自动重试并恢复；
  G. 规格常量 MIN_SAMPLE_INTERVAL_SECONDS == 1.0；
  H. run_sample_round 的单飞保护：在飞时直接跳过、不增加采样次数。
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lock_sampler import (  # noqa: E402
    MIN_SAMPLE_INTERVAL_SECONDS,
    LockSampler,
)

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


class FakeQuery:
    def __init__(self) -> None:
        self.start_times: list[float] = []
        self.concurrent = 0
        self.max_concurrent = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block_next = False
        self.result: list = []
        self.fail: Exception | None = None
        self._mu = threading.Lock()

    def __call__(self):
        with self._mu:
            self.start_times.append(time.monotonic())
            self.concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent)
            block = self.block_next
            self.entered.set()
        if block:
            self.release.wait(timeout=5)
        try:
            if self.fail is not None:
                raise self.fail
            return list(self.result)
        finally:
            with self._mu:
                self.concurrent -= 1


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> int:
    section("A. 操作前出发的迟到采样不能刷新列表")
    fq = FakeQuery()
    s = LockSampler(query_fn=fq, autostart=False, min_interval=0)
    fq.result = [{"v": "L1"}]
    check("首轮采纳", s.run_sample_round() is True)
    snap = s.snapshot()
    check("L1 生效且 fresh",
          snap["locks"] == [{"v": "L1"}] and snap["status"] == "fresh")

    fq.block_next = True
    fq.entered.clear()
    fq.release.clear()
    t = threading.Thread(target=s.run_sample_round)
    t.start()
    check("阻塞采样已进入查询", fq.entered.wait(2))
    check("in_flight=True", s.snapshot()["in_flight"] is True)
    check("采样次数=2", s.snapshot()["sample_count"] == 2)

    noted = s.note_mutation("unit-open")
    check("note_mutation 同步返回 pending_update=True",
          noted["pending_update"] is True and noted["status"] == "pending")
    pre = s.snapshot()
    check("操作后旧列表仍在且标 pending",
          pre["locks"] == [{"v": "L1"}] and pre["pending_update"] is True)

    fq.result = [{"v": "L2-late"}]
    fq.release.set()
    t.join(2)
    snap = s.snapshot()
    check("迟到轮被丢弃", snap["discarded_count"] == 1)
    check("迟到结果没有刷新列表",
          snap["locks"] == [{"v": "L1"}], str(snap["locks"]))
    check("仍为 pending",
          snap["pending_update"] is True and snap["status"] == "pending")

    fq.block_next = False
    fq.result = [{"v": "L3-new"}]
    check("操作后启动的采样被采纳", s.run_sample_round() is True)
    snap = s.snapshot()
    check("列表刷新为 L3 且 pending 清除",
          snap["locks"] == [{"v": "L3-new"}]
          and snap["pending_update"] is False
          and snap["status"] == "fresh")
    s.stop()

    section("B. 失败保留上次成功列表并标过期，成功后恢复")
    fq = FakeQuery()
    s = LockSampler(query_fn=fq, autostart=False, min_interval=0)
    fq.result = [{"v": "GOOD"}]
    s.run_sample_round()
    fq.fail = RuntimeError("boom")
    check("失败轮返回 False", s.run_sample_round() is False)
    snap = s.snapshot()
    check("旧列表保留", snap["locks"] == [{"v": "GOOD"}])
    check("标记 expired/stale",
          snap["expired"] is True and snap["stale"] is True
          and snap["status"] == "stale", snap["status"])
    check("记录错误与 failure_count",
          "boom" in (snap["last_error"] or "")
          and snap["failure_count"] == 1)
    fq.fail = None
    fq.result = [{"v": "GOOD2"}]
    s.run_sample_round()
    snap = s.snapshot()
    check("后续成功自动恢复",
          snap["expired"] is False and snap["status"] == "fresh"
          and snap["locks"] == [{"v": "GOOD2"}]
          and snap["last_error"] is None)
    s.stop()

    section("C. 从未成功时失败 -> error 状态")
    fq = FakeQuery()
    s = LockSampler(query_fn=fq, autostart=False, min_interval=0)
    fq.fail = ValueError("cannot connect")
    s.run_sample_round()
    snap = s.snapshot()
    check("status=error", snap["status"] == "error")
    check("无列表且 error_state",
          snap["error_state"] is True and snap["locks"] == [])
    s.stop()

    section("D. 两轮开始间隔 >= min_interval（后台循环）")
    fq = FakeQuery()
    interval = 0.2
    s = LockSampler(query_fn=fq, min_interval=interval, autostart=True)
    s.poll()
    deadline = time.time() + 3
    while len(fq.start_times) < 4 and time.time() < deadline:
        s.poll()
        time.sleep(0.02)
    s.stop()
    gaps = [b - a for a, b in zip(fq.start_times, fq.start_times[1:])]
    check(f"至少 4 轮（实际 {len(fq.start_times)}）",
          len(fq.start_times) >= 4)
    check(f"所有开始间隔 >= {interval - 0.01}s",
          all(g >= interval - 0.01 for g in gaps),
          ", ".join(f"{g:.3f}" for g in gaps))

    section("E. 慢采样期间 poll 立即返回，且绝不并发采样")
    fq = FakeQuery()
    s = LockSampler(query_fn=fq, min_interval=0.05, autostart=True)
    fq.block_next = True
    s.poll()
    check("等待进入慢查询", fq.entered.wait(2))
    time.sleep(0.05)
    t0 = time.monotonic()
    snap = s.poll()
    dt = time.monotonic() - t0
    check(f"poll 不等待采样（{dt * 1000:.1f}ms）", dt < 0.1)
    check("快照明示 in_flight", snap["in_flight"] is True)
    end = time.time() + 0.3
    while time.time() < end:
        s.poll()
        time.sleep(0.01)
    check("查询最大并发=1", fq.max_concurrent == 1, str(fq.max_concurrent))
    fq.release.set()
    s.wait_idle(2)
    s.stop()

    section("F. 失败后后台自动重试并恢复")
    state = {"n": 0}
    mu = threading.Lock()

    def flaky():
        with mu:
            state["n"] += 1
            n = state["n"]
        if n <= 2:
            raise RuntimeError("transient")
        return [{"v": "RECOVERED"}]

    s = LockSampler(query_fn=flaky, min_interval=0.05, autostart=True)
    s.poll()
    check("自动重试到第 3 轮", s.wait_for_samples(3, timeout=3))
    time.sleep(0.2)
    snap = s.snapshot()
    check("自动恢复 fresh 且列表正确",
          snap["status"] == "fresh"
          and snap["locks"] == [{"v": "RECOVERED"}], snap["status"])
    s.stop()

    section("G. 规格常量：最小间隔 1 秒")
    check("MIN_SAMPLE_INTERVAL_SECONDS == 1.0",
          MIN_SAMPLE_INTERVAL_SECONDS == 1.0)

    section("H. 单飞保护：在飞时直接跳过")
    fq = FakeQuery()
    s = LockSampler(query_fn=fq, autostart=False, min_interval=0)
    fq.block_next = True
    fq.entered.clear()
    fq.release.clear()
    t = threading.Thread(target=s.run_sample_round)
    t.start()
    fq.entered.wait(2)
    before = s.snapshot()["sample_count"]
    ret = s.run_sample_round()  # 同步调用，因在飞应跳过
    after = s.snapshot()["sample_count"]
    check("并发的 run_sample_round 返回 None", ret is None)
    check("跳过时不增加采样次数", before == after)
    fq.release.set()
    t.join(2)
    s.stop()

    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
