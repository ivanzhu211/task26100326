"""验收 1：同一组事件按不同批次和顺序提交，结果与从头切一遍完全一致。"""
from __future__ import annotations

import random

from session_stitcher import EventInput, SessionService

from helpers import (
    effective_events,
    reference_distribution,
    reference_sessions,
    service_distribution,
    service_sessions,
)

NOW = 10**13
MIN = 60 * 1000
HOUR = 60 * MIN
BASE = 1_760_000_000_000


def build_submissions() -> list[EventInput]:
    """构造同一批提交内容：多设备交错、结束事件、重发、改时间的修订。"""
    events: list[EventInput] = []
    # u1 的手机：3 个会话，第二个以 session_end 收尾
    t = BASE
    seq = 0
    for n, gap_after, end in [(5, 2 * HOUR, False), (4, 45 * MIN, True), (6, 0, False)]:
        for _ in range(n):
            seq += 1
            events.append(EventInput("u1", "phoneA", seq, 1, t, "read", "phone"))
            t += 5 * MIN
        if end:
            seq += 1
            events.append(EventInput("u1", "phoneA", seq, 1, t, "session_end", "phone"))
            t += 1 * MIN
        t += gap_after
    # u1 的平板：事件落在手机事件的 10 分钟内，跨设备合并会话
    for i in range(8):
        events.append(
            EventInput(
                "u1", "tabletB", i + 1, 1, BASE + 2 * MIN + i * 5 * MIN, "read", "tablet"
            )
        )
    # u2 的手机：两个会话
    for i in range(6):
        events.append(
            EventInput("u2", "phoneC", i + 1, 1, BASE + i * 3 * MIN, "read", "phone")
        )
    for i in range(4):
        events.append(
            EventInput(
                "u2", "phoneC", 10 + i, 1, BASE + 3 * HOUR + i * 3 * MIN, "read", "phone"
            )
        )
    # 改时间的修订：phoneA seq 3 从原位置挪到 26 小时后（跨过 UTC 日期）
    events.append(EventInput("u1", "phoneA", 3, 2, BASE + 26 * HOUR, "read", "phone"))
    # 同修订号、更晚时间：tabletB seq 2 挪 2 分钟
    events.append(
        EventInput("u1", "tabletB", 2, 1, BASE + 2 * MIN + 7 * MIN, "read", "tablet")
    )
    # 修订改变设备类型
    events.append(EventInput("u2", "phoneC", 2, 2, BASE + 3 * MIN, "read", "tablet"))
    # 完全重复的重发
    events.append(EventInput("u1", "phoneA", 1, 1, BASE, "read", "phone"))
    events.append(
        EventInput("u2", "phoneC", 11, 1, BASE + 3 * HOUR + 3 * MIN, "read", "phone")
    )
    # 会被拒收的未来事件
    events.append(EventInput("u1", "phoneA", 900, 1, NOW + 10 * MIN, "read", "phone"))
    return events


def make_plans(events: list[EventInput]) -> dict[str, list[list[EventInput]]]:
    """把同一组事件组织成不同的批次与到达顺序。"""
    plans: dict[str, list[list[EventInput]]] = {}
    by_device: dict[str, list[EventInput]] = {}
    for ev in events:
        by_device.setdefault(ev.device_id, []).append(ev)
    # 计划 A：按设备、按时间顺序，小批次
    plans["ordered"] = [
        sorted(dev_evs, key=lambda e: (e.event_time_ms, e.seq, e.revision))[i : i + 7]
        for dev_evs in by_device.values()
        for i in range(0, len(dev_evs), 7)
    ]
    # 计划 B：设备内随机打乱后切批，批次再整体打乱
    rng = random.Random(42)
    shuffled_batches: list[list[EventInput]] = []
    for dev_evs in by_device.values():
        copy = list(dev_evs)
        rng.shuffle(copy)
        shuffled_batches.extend(copy[i : i + 13] for i in range(0, len(copy), 13))
    rng.shuffle(shuffled_batches)
    plans["shuffled"] = shuffled_batches
    # 计划 C：每设备一整批且批内倒序，设备顺序颠倒
    plans["whole"] = [
        list(reversed(dev_evs)) for dev_evs in reversed(list(by_device.values()))
    ]
    return plans


def test_submission_order_invariance(tmp_path):
    events = build_submissions()
    plans = make_plans(events)
    effective = effective_events(events, NOW)
    expected_by_user = {
        user: reference_sessions([e for e in effective if e.user_id == user])
        for user in ("u1", "u2")
    }
    expected_dist = reference_distribution(effective)

    results = {}
    for name, batches in plans.items():
        svc = SessionService(str(tmp_path / f"{name}.db"))
        for batch in batches:
            svc.submit_batch(batch, received_at_ms=NOW)
        results[name] = (
            {u: service_sessions(svc, u) for u in ("u1", "u2")},
            service_distribution(svc),
        )
        svc.close()

    for name, (sessions_by_user, dist) in results.items():
        for user in ("u1", "u2"):
            assert sessions_by_user[user] == expected_by_user[user], (
                f"plan {name} user {user} sessions mismatch"
            )
        assert dist == expected_dist, f"plan {name} distribution mismatch"

    names = list(results)
    for other in names[1:]:
        assert results[other] == results[names[0]]
