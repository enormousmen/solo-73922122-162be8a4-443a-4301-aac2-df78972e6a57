"""名称兼容别名：采样器实现位于 :mod:`app.lock_sampler`。"""
from __future__ import annotations

from .lock_sampler import (  # noqa: F401
    ERROR,
    FRESH,
    LOADING,
    MIN_SAMPLE_INTERVAL_SECONDS,
    PENDING,
    STALE,
    LockSampler,
    fetch_lock_rows,
    get_lock_sample,
    get_lock_sampler,
    get_sampler,
    lock_sampler,
    sample_lock_queue,
)

# 部分调用方可能用 sampler.sampler / sampler.shared_sampler 取单例
sampler = lock_sampler
shared_sampler = lock_sampler

__all__ = [
    "LockSampler",
    "lock_sampler",
    "sampler",
    "shared_sampler",
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
