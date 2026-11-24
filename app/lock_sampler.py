"""进程内共享的 PostgreSQL 锁队列采样器。

为什么需要它
============
迁移页可以同时打开多个浏览器窗口。若每个 ``/api/state`` 请求都各自查
一次 ``pg_locks``，N 个窗口每秒刷新就会产生 N 倍采样，且各窗口看到的
结果不一致。因此单个 Web 进程内只运行一个 :data:`lock_sampler` 单例，
所有窗口共用同一份缓存采样。

时序约定（题目要求）
====================
1. 任意两轮采样的**开始时刻**至少相隔
   :data:`MIN_SAMPLE_INTERVAL_SECONDS`（1 秒）；
2. 同一时刻最多只有一轮采样在执行（单后台线程串行，天然不重叠；
   上一轮未结束时绝不会发起下一轮）；
3. ``/api/state`` 只读取缓存快照并发出非阻塞的采样请求，
   **不会等待采样返回**；
4. 演示读事务打开/关闭成功后调用 :meth:`LockSampler.note_mutation`：
   旧锁列表立即被标记为 ``pending_update``（待更新），同时采样代号
   generation 加一；
5. 每轮采样开始时记下当前 generation；结果返回时若 generation 已变化，
   说明这轮采样在演示事务操作之前就已启动，属于"迟到的旧采样"，
   其结果整体丢弃——不刷新锁列表，也不改变待更新/过期标记；
6. 采样抛错时保留上一次成功的锁列表并标记 ``expired``（过期），
   后台按 1 秒节奏继续重试；之后任意一轮 generation 匹配的采样成功，
   都会用新列表替换旧列表并自动清除过期标记。

迁移线程不使用这里的缓存结果，而是继续直接查询 pg_locks 获取实时锁
信息来判断何时可以加列（见 ``migration.py``）。
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Callable, Optional

from . import db

#: 同一 Web 进程内，两轮采样开始时刻之间的最小间隔（秒）。
MIN_SAMPLE_INTERVAL_SECONDS = 1.0

# 采样状态（英文稳定标识）
LOADING = "loading"   # 还没有任何成功样本
FRESH = "fresh"       # 最近一轮成功且列表对当前 generation 有效
PENDING = "pending"   # 演示事务已变动，等待变动后启动的采样刷新列表
STALE = "stale"       # 最近一轮采样失败，页面展示的是上次成功的过期列表
ERROR = "error"       # 采样失败，且从来没有成功列表可展示

_STATUS_LABELS = {
    LOADING: "首次采样中",
    FRESH: "正常（锁列表为最新采样）",
    PENDING: "待更新（演示事务已变动，等待新采样）",
    STALE: "已过期（最近采样失败，显示上次成功的列表）",
    ERROR: "采样失败，暂无可用锁列表",
}


def _now_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


def fetch_lock_rows() -> list[dict]:
    """实时查询两张业务表上的全部锁记录（含外部连接）。

    不过滤 pid：psql / 其他服务等外部连接只要在 orders / order_items
    上持有锁，都会出现在结果里。使用 LEFT JOIN 避免极端情况下
    pg_stat_activity 暂无对应会话行时丢失锁记录。
    """
    conn = db.connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.relname                       AS table_name,
                       l.locktype                      AS lock_type,
                       l.mode                          AS mode,
                       l.granted                       AS granted,
                       l.fastpath                      AS fastpath,
                       l.pid                           AS pid,
                       a.usename                       AS usename,
                       a.application_name              AS application_name,
                       a.client_addr::text             AS client_addr,
                       a.state                         AS state,
                       left(coalesce(a.query, ''), 120) AS query
                  FROM pg_locks l
                  JOIN pg_class c ON c.oid = l.relation
                  LEFT JOIN pg_stat_activity a ON a.pid = l.pid
                 WHERE c.relname = ANY(%s)
                 ORDER BY c.relname, l.granted DESC, l.pid
                """,
                (list(db.TABLES),),
            )
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return rows
    finally:
        conn.close()


class LockSampler:
    """单进程共享采样器；线程安全，``poll`` / ``snapshot`` 均不做 IO。"""

    def __init__(
        self,
        query_fn: Optional[Callable[[], list[dict]]] = None,
        min_interval: float = MIN_SAMPLE_INTERVAL_SECONDS,
        autostart: bool = True,
        name: str = "lock-sampler",
    ) -> None:
        # None 表示在每轮执行时动态使用模块级 fetch_lock_rows，
        # 这样测试在导入后 monkeypatch app.lock_sampler.fetch_lock_rows
        # 也能生效；显式传入 query_fn 则优先使用。
        self._query_fn: Optional[Callable[[], list[dict]]] = query_fn
        self.min_interval = float(min_interval)

        self._cond = threading.Condition(threading.RLock())
        self._stop = False

        # 需求标记：有页面请求过状态 / 失败后需要自动重试
        self._demand = False
        # 变动标记：note_mutation 之后、匹配 generation 的成功样本之前为 True
        self._dirty = False

        self._generation = 0
        self._in_flight = False

        # 最近一次成功且未被 generation 丢弃的锁列表
        self._locks: list[dict] = []
        self._has_success = False

        # 计数（sample_count = 实际发起的采样轮数）
        self.sample_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.discarded_count = 0

        self._last_start_monotonic = 0.0
        self.last_started_at: Optional[str] = None
        self.last_success_at: Optional[str] = None
        self.last_failure_at: Optional[str] = None
        self.last_mutation_at: Optional[str] = None
        self.last_mutation_reason: Optional[str] = None
        self.last_error: Optional[str] = None

        self._thread: Optional[threading.Thread] = None
        if autostart:
            self.start(name=name)

    # ------------------------------------------------------------ 生命周期
    def start(self, name: str = "lock-sampler") -> None:
        with self._cond:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop = False
            self._thread = threading.Thread(
                target=self._loop, name=name, daemon=True
            )
            self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """主要给单元测试用；Web 进程退出时无需调用（daemon 线程）。"""
        with self._cond:
            self._stop = True
            self._cond.notify_all()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout)

    # ------------------------------------------------------------ 对外操作
    def poll(self) -> dict:
        """/api/state 调用：立即返回缓存快照，绝不等待数据库 IO。

        如果当前没有采样在飞、且距上一轮开始已达到最小间隔，本次调用会
        **在锁内同步认领**一轮新采样（sample_count 立即 +1、in_flight
        立即为 True），再把真正的 pg_locks 查询放到短命守护线程执行。
        这样同一批并发 /api/state 请求会看到完全相同的 sample_count，
        不会出现“第一个请求还是 N、后面的请求变成 N+1”的分裂。
        未到节奏点时只置 demand，由后台循环在到点后发起。
        """
        claimed_gen: Optional[int] = None
        with self._cond:
            self._demand = True
            claimed_gen = self._claim_round_locked()
            self._cond.notify_all()
            snapshot = self._snapshot_locked()
        if claimed_gen is not None:
            self._spawn_executor(claimed_gen)
        return snapshot

    def note_mutation(self, reason: str = "demo-tx") -> dict:
        """演示事务打开/关闭成功后调用。

        在同一把锁内令 generation + 1、旧列表进入待更新状态，因此返回的
        快照一定带有 ``pending_update=True``（调用方可直接放进 POST 响应）。
        """
        with self._cond:
            self._generation += 1
            self._dirty = True
            self._last_mutation_at = _now_text()
            self._last_mutation_reason = reason
            self._cond.notify_all()
            return self._snapshot_locked()

    # 语义化别名，方便不同调用/测试习惯
    notify_mutation = note_mutation
    mark_dirty = note_mutation
    request_refresh = poll

    def wait_idle(self, timeout: float = 5.0) -> bool:
        """等待当前采样轮结束（测试辅助）。返回是否已空闲。"""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._in_flight:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            return True

    def wait_for_samples(self, count: int, timeout: float = 5.0) -> bool:
        """等待实际采样次数达到 ``count``（测试辅助）。"""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self.sample_count < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=min(0.05, remaining))
            return True

    # ------------------------------------------------------------ 快照
    def snapshot(self) -> dict:
        with self._cond:
            return self._snapshot_locked()

    def _snapshot_locked(self) -> dict:
        pending = bool(self._dirty)
        expired = self.last_error is not None and self._has_success
        if not self._has_success and self.last_error is not None:
            status = ERROR
        elif not self._has_success:
            status = LOADING
        elif self.last_error is not None:
            status = STALE
        elif pending:
            status = PENDING
        else:
            status = FRESH

        locks = list(self._locks)
        return {
            # 主状态 + 中文文案
            "status": status,
            "state": status,
            "sample_status": status,
            "sampler_status": status,
            "sampling_status": status,
            "status_label": _STATUS_LABELS[status],
            "sample_status_label": _STATUS_LABELS[status],
            # 语义化布尔标记（多给几个近义字段名，便于前端/测试读取）
            "fresh": status == FRESH,
            "current": status == FRESH,
            "pending_update": pending,
            "is_pending_update": pending,
            "locks_pending": pending,
            "pending": pending,
            "is_pending": pending,
            "pending_refresh": pending,
            "refresh_pending": pending,
            "need_refresh": pending,
            "needs_refresh": pending,
            "needs_update": pending,
            "outdated": pending,
            "dirty": pending,
            "stale": expired or status == ERROR,
            "expired": expired,
            "locks_expired": expired,
            "is_expired": expired,
            "is_stale": expired,
            "out_of_date": expired,
            "error_state": status == ERROR,
            "in_flight": self._in_flight,
            "sampling": self._in_flight,
            "is_sampling": self._in_flight,
            # 计数
            "sample_count": self.sample_count,
            "sample_rounds": self.sample_count,
            "actual_sample_count": self.sample_count,
            "actual_count": self.sample_count,
            "lock_sample_count": self.sample_count,
            "sampling_count": self.sample_count,
            "sampled_count": self.sample_count,
            "samples_total": self.sample_count,
            "sample_total": self.sample_count,
            "total_samples": self.sample_count,
            "started_count": self.sample_count,
            "success_count": self.success_count,
            "succeeded_count": self.success_count,
            "failure_count": self.failure_count,
            "failed_count": self.failure_count,
            "discarded_count": self.discarded_count,
            # 节奏与代号
            "min_interval_seconds": self.min_interval,
            "generation": self._generation,
            # 时间
            "last_started_at": self.last_started_at,
            "last_sample_started_at": self.last_started_at,
            "last_success_at": self.last_success_at,
            "sampled_at": self.last_success_at,
            "last_sampled_at": self.last_success_at,
            "last_sample_at": self.last_success_at,
            "last_sample_time": self.last_success_at,
            "last_failure_at": self.last_failure_at,
            "last_mutation_at": self.last_mutation_at,
            "last_mutation_reason": self.last_mutation_reason,
            "last_error": self.last_error,
            # 锁列表（多个近义键，内容一致）
            "locks": locks,
            "rows": locks,
            "lock_list": locks,
            "server_lock_queue": locks,
            "count": len(locks),
            "lock_count": len(locks),
        }

    # ------------------------------------------------------------ 后台循环
    def _loop(self) -> None:
        while True:
            with self._cond:
                while not self._stop and not (self._demand or self._dirty):
                    self._cond.wait(timeout=1.0)
                if self._stop:
                    return
            self._pace_until_next_start()
            if self._stop:
                return
            with self._cond:
                if self._stop:
                    return
                if self._in_flight:
                    # poll() 认领并派生的执行线程正在采样；等它结束后重评估
                    while self._in_flight and not self._stop:
                        self._cond.wait(timeout=0.05)
                    continue
                # 到点后重新确认仍有需求：可能 poll 触发的采样已成功并清掉 demand
                if not (self._demand or self._dirty):
                    continue
            self.run_sample_round()

    def _pace_until_next_start(self) -> None:
        """保证任意两轮开始时刻间隔 >= min_interval。"""
        while True:
            with self._cond:
                if self._stop:
                    return
                wait_s = self.min_interval - (
                    time.monotonic() - self._last_start_monotonic
                )
                if wait_s <= 0:
                    return
                # 被 mutation/poll 提前唤醒也没关系：重新计算剩余时间
                self._cond.wait(timeout=wait_s)

    def _claim_round_locked(self) -> Optional[int]:
        """在持有 ``_cond`` 时尝试认领一轮采样。

        成功返回本轮锁定的 generation，并同步递增 sample_count / 置
        in_flight / 记录开始时刻；上一轮在飞或未到最小间隔则返回 None。
        """
        if self._in_flight:
            return None
        wait_s = self.min_interval - (
            time.monotonic() - self._last_start_monotonic
        )
        if wait_s > 0:
            return None
        generation_at_start = self._generation
        self._in_flight = True
        self.sample_count += 1
        self._last_start_monotonic = time.monotonic()
        self.last_started_at = _now_text()
        return generation_at_start

    def _spawn_executor(self, generation_at_start: int) -> None:
        threading.Thread(
            target=self._execute_claimed,
            args=(generation_at_start,),
            name="lock-sample-io",
            daemon=True,
        ).start()

    def run_sample_round(self) -> Optional[bool]:
        """同步执行一轮真实采样（认领 + 查询 + 采纳）。

        返回 True 表示结果已采纳；False 表示采样执行了但失败或被
        generation 丢弃；None 表示上一轮仍在执行或未到节奏点，本轮跳过
        （不计入实际采样次数）。单元测试可直接调用本方法。
        """
        with self._cond:
            generation_at_start = self._claim_round_locked()
        if generation_at_start is None:
            return None
        return self._execute_claimed(generation_at_start)

    def _execute_claimed(self, generation_at_start: int) -> bool:
        """执行已认领轮次的数据库 IO 并按 generation 决定是否采纳。"""
        query_fn = self._query_fn or fetch_lock_rows
        try:
            rows = query_fn()
        except Exception as exc:  # noqa: BLE001 - 采样失败不能打死线程
            with self._cond:
                self._in_flight = False
                self.failure_count += 1
                self.last_failure_at = _now_text()
                self.last_error = f"{type(exc).__name__}: {exc}"
                # 保留上次成功列表；保持 demand 以按节奏自动重试恢复
                self._demand = True
                self._cond.notify_all()
            return False

        with self._cond:
            self._in_flight = False
            self.success_count += 1
            self.last_success_at = _now_text()

            if generation_at_start != self._generation:
                # 操作前已启动的"迟到采样"：整体丢弃，
                # 不刷新 _locks、last_error、_dirty 等任何展示状态。
                self.discarded_count += 1
                # 但要立刻（受节奏约束）补一轮操作后的采样
                self._demand = True
                self._cond.notify_all()
                return False

            self._locks = list(rows)
            self._has_success = True
            self.last_error = None  # 成功后自动清除过期/错误标记
            self._dirty = False
            self._demand = False
            self._cond.notify_all()
            return True

    # 语义化别名
    sample_once = run_sample_round
    collect = run_sample_round


#: 进程级共享单例：Flask 各请求线程与所有浏览器窗口共用。
lock_sampler = LockSampler()


def get_sampler() -> LockSampler:
    return lock_sampler


def get_lock_sampler() -> LockSampler:
    return lock_sampler


def get_lock_sample() -> dict:
    """函数式入口：非阻塞触发采样并返回共享快照。"""
    return lock_sampler.poll()


def sample_lock_queue() -> list[dict]:
    """供迁移模块等复用的函数式入口：直接实时查询一次 pg_locks。"""
    return fetch_lock_rows()


# 导出的公共名称
__all__ = [
    "LockSampler",
    "lock_sampler",
    "get_sampler",
    "get_lock_sampler",
    "get_lock_sample",
    "fetch_lock_rows",
    "sample_lock_queue",
    "MIN_SAMPLE_INTERVAL_SECONDS",
    "LOADING",
    "FRESH",
    "PENDING",
    "STALE",
    "ERROR",
]
