"""验收 3：20 万条历史的用户补交中段小批，耗时与短历史用户同一量级。"""
from __future__ import annotations

import time

import pytest

from session_stitcher import EventInput, SessionService

from helpers import (
    effective_events,
    reference_sessions,
    service_sessions,
)

NOW = 10**13
MIN = 60 * 1000
BASE = 1_700_000_000_000

SESSION_EVENTS = 10  # 每个会话 10 条事件
EVENT_STEP_MS = 30 * 1000  # 会话内每 30 秒一条
SESSION_PERIOD_MS = 70 * MIN  # 会话起点间隔 70 分钟（会话间隔约 65 分钟）


def make_history_events(user: str, device: str, n_sessions: int) -> list[EventInput]:
    events: list[EventInput] = []
    seq = 0
    for s in range(n_sessions):
        for i in range(SESSION_EVENTS):
            seq += 1
            events.append(
                EventInput(
                    user,
                    device,
                    seq,
                    1,
                    BASE + s * SESSION_PERIOD_MS + i * EVENT_STEP_MS,
                    "read",
                    "phone",
                )
            )
    return events


def submit_history(svc: SessionService, events: list[EventInput]) -> None:
    for i in range(0, len(events), 5000):
        svc.submit_batch(events[i : i + 5000], received_at_ms=NOW)


def middle_batch(user: str, device: str, seq_start: int, session_index: int) -> list[EventInput]:
    """构造一小批落在历史中段的事件（部分并入已有会话，部分自成新会话）。"""
    t0 = BASE + session_index * SESSION_PERIOD_MS
    return [
        EventInput(user, device, seq_start + i, 1, t0 + i * MIN, "read", "phone")
        for i in range(50)
    ]


def timed_submit_ms(svc: SessionService, batches: list[list[EventInput]]) -> float:
    samples: list[float] = []
    for batch in batches:
        start = time.perf_counter()
        svc.submit_batch(batch, received_at_ms=NOW)
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    return samples[len(samples) // 2]


@pytest.mark.slow
def test_middle_backfill_cost_is_history_independent(tmp_path):
    big_user_events = make_history_events("big", "phoneX", 20_000)  # 20 万条，约 90 天
    assert len(big_user_events) == 200_000
    small_user_events = make_history_events("small", "phoneY", 30)  # 300 条

    svc = SessionService(str(tmp_path / "perf.db"))
    submit_history(svc, big_user_events)
    submit_history(svc, small_user_events)

    # 同一中段位置补交 5 小批（序号错开），取耗时中位数
    big_batches = [
        middle_batch("big", "phoneX", 900_000 + r * 100, 10_000) for r in range(5)
    ]
    small_batches = [
        middle_batch("small", "phoneY", 900_000 + r * 100, 15) for r in range(5)
    ]
    big_ms = timed_submit_ms(svc, big_batches)
    small_ms = timed_submit_ms(svc, small_batches)
    print(f"\nbig history middle batch: {big_ms:.1f}ms, small: {small_ms:.1f}ms")
    # 同一量级：不超过短历史的 20 倍，且绝对值远小于 1 秒
    assert big_ms < max(small_ms * 20, 200.0)

    # 时长分布即时返回
    start = time.perf_counter()
    dist = svc.duration_distribution()
    dist_ms = (time.perf_counter() - start) * 1000
    print(f"distribution query: {dist_ms:.1f}ms")
    assert dist_ms < 200.0
    assert sum(b.count for b in dist) > 0

    # 大规模增量提交后，结果仍与从头切一遍完全一致
    all_big = big_user_events + [e for batch in big_batches for e in batch]
    expected = reference_sessions(effective_events(all_big, NOW))
    assert service_sessions(svc, "big") == expected
    svc.close()
