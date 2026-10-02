"""验收 2：改时间的修订把事件挪走导致会话拆分，补记结束与时长分布联动更新。"""
from __future__ import annotations

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
BASE = 1_760_000_000_000


def ev(seq, rev, t, name="read"):
    return EventInput("u", "phoneA", seq, rev, t, name, "phone")


def test_revision_moves_event_and_splits_session(tmp_path):
    svc = SessionService(str(tmp_path / "t.db"))
    submitted: list[EventInput] = [
        ev(1, 1, BASE),
        ev(2, 1, BASE + 30 * MIN),
        ev(3, 1, BASE + 60 * MIN),
        ev(4, 1, BASE + 70 * MIN, "session_end"),
    ]
    svc.submit_batch(submitted, received_at_ms=NOW)
    before = service_sessions(svc, "u")
    assert len(before) == 1
    assert before[0][3] == "active_end"
    assert service_distribution(svc) == [(140, 1)]  # 70 分钟 -> 第 140 档
    # 修订把 seq2 从 30 分钟处挪到 5 小时后，原会话被拆成两段
    moved = ev(2, 2, BASE + 5 * 60 * MIN)
    submitted.append(moved)
    svc.submit_batch([moved], received_at_ms=NOW)
    expected = reference_sessions(effective_events(submitted, NOW))
    got = service_sessions(svc, "u")
    assert got == expected
    assert len(got) == 3
    # 第一段：A 单独，无结束事件且后面有会话 -> 补记结束
    assert got[0] == (BASE, BASE, 1, "backfilled", got[0][4], "phone")
    # 第二段：C + 结束事件 -> 主动结束
    assert got[1][0] == BASE + 60 * MIN
    assert got[1][3] == "active_end"
    # 第三段：被挪走的事件自成会话，进行中
    assert got[2][0] == BASE + 5 * 60 * MIN
    assert got[2][3] == "ongoing"
    # 时长分布跟着更新：30 分钟档 -1，新增 0 档（补记）和 10 分钟档（主动结束）
    assert service_distribution(svc) == reference_distribution(
        effective_events(submitted, NOW)
    )
    assert service_distribution(svc) == [(0, 1), (20, 1)]
    svc.close()


def test_revision_moves_event_and_merges_sessions(tmp_path):
    """反向场景：修订把事件挪到两个会话之间，把它们连成一段。"""
    svc = SessionService(str(tmp_path / "t.db"))
    submitted: list[EventInput] = [
        ev(1, 1, BASE),
        ev(2, 1, BASE + 10 * MIN),
        # seq3 原本在 10 小时外，自成会话
        ev(3, 1, BASE + 10 * 60 * MIN),
        ev(4, 1, BASE + 60 * MIN),
        ev(5, 1, BASE + 70 * MIN),
    ]
    svc.submit_batch(submitted, received_at_ms=NOW)
    assert len(service_sessions(svc, "u")) == 3
    # 修订把 seq3 挪到 40 分钟处，把前后两段桥接成一段
    moved = ev(3, 2, BASE + 40 * MIN)
    submitted.append(moved)
    svc.submit_batch([moved], received_at_ms=NOW)
    expected = reference_sessions(effective_events(submitted, NOW))
    got = service_sessions(svc, "u")
    assert got == expected
    assert len(got) == 1
    assert got[0][3] == "ongoing"
    assert service_distribution(svc) == reference_distribution(
        effective_events(submitted, NOW)
    )
    svc.close()
