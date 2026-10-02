"""受理规则、会话状态流转、时长分布过滤的单元测试。"""
from __future__ import annotations

import pytest

from session_stitcher import (
    BUCKET_COUNT,
    BatchRejectedError,
    EventInput,
    SessionService,
    SessionStatus,
)

from helpers import service_sessions

NOW = 10**13
MIN = 60 * 1000


def make_service(tmp_path):
    return SessionService(str(tmp_path / "test.db"))


def ev(user, device, seq, rev, t, name="read", dtype="phone"):
    return EventInput(user, device, seq, rev, t, name, dtype)


def test_future_event_rejected(tmp_path):
    svc = make_service(tmp_path)
    stats = svc.submit_batch([ev("u", "d", 1, 1, NOW + 10 * MIN)], received_at_ms=NOW)
    assert stats == {"accepted": 0, "duplicates": 0, "rejected": 1}
    assert service_sessions(svc, "u") == []
    svc.close()


def test_rejection_keeps_existing_version(tmp_path):
    svc = make_service(tmp_path)
    svc.submit_batch([ev("u", "d", 1, 1, NOW - 10 * MIN)], received_at_ms=NOW)
    # 同一序号更高修订号，但事件时间超出未来容忍，拒收且不影响已生效版本
    stats = svc.submit_batch(
        [ev("u", "d", 1, 2, NOW + 10 * MIN)], received_at_ms=NOW
    )
    assert stats["rejected"] == 1
    sessions = service_sessions(svc, "u")
    assert len(sessions) == 1
    assert sessions[0][0] == NOW - 10 * MIN
    svc.close()


def test_revision_and_time_precedence(tmp_path):
    svc = make_service(tmp_path)
    base = NOW - 60 * MIN
    svc.submit_batch([ev("u", "d", 1, 2, base + 5 * MIN)], received_at_ms=NOW)
    # 修订号小：忽略
    assert svc.submit_batch([ev("u", "d", 1, 1, base)], received_at_ms=NOW)["duplicates"] == 1
    # 修订号相同、时间更早：忽略
    assert svc.submit_batch([ev("u", "d", 1, 2, base)], received_at_ms=NOW)["duplicates"] == 1
    # 修订号相同、时间相同：重复
    assert svc.submit_batch([ev("u", "d", 1, 2, base + 5 * MIN)], received_at_ms=NOW)["duplicates"] == 1
    # 修订号相同、时间更晚：生效
    assert svc.submit_batch([ev("u", "d", 1, 2, base + 9 * MIN)], received_at_ms=NOW)["accepted"] == 1
    # 修订号更大：生效
    assert svc.submit_batch([ev("u", "d", 1, 3, base + 7 * MIN)], received_at_ms=NOW)["accepted"] == 1
    sessions = service_sessions(svc, "u")
    assert len(sessions) == 1
    assert sessions[0][0] == base + 7 * MIN
    svc.close()


def test_gap_boundary_exactly_30min_not_split(tmp_path):
    svc = make_service(tmp_path)
    base = NOW - 10 * 60 * MIN
    svc.submit_batch(
        [ev("u", "d", 1, 1, base), ev("u", "d", 2, 1, base + 30 * MIN)],
        received_at_ms=NOW,
    )
    sessions = service_sessions(svc, "u")
    assert len(sessions) == 1
    assert sessions[0][2] == 2
    # 30 分钟 + 1 毫秒：切开
    svc.submit_batch([ev("u", "d", 3, 1, base + 60 * MIN + 1)], received_at_ms=NOW)
    sessions = service_sessions(svc, "u")
    assert len(sessions) == 2
    svc.close()


def test_session_end_and_backfill_and_ongoing(tmp_path):
    svc = make_service(tmp_path)
    base = NOW - 10 * 60 * MIN
    svc.submit_batch(
        [
            ev("u", "d", 1, 1, base),
            ev("u", "d", 2, 1, base + MIN, "session_end"),
            ev("u", "d", 3, 1, base + 2 * MIN),  # 前一条是结束事件，开新会话
            ev("u", "d", 4, 1, base + 3 * MIN),
        ],
        received_at_ms=NOW,
    )
    sessions = service_sessions(svc, "u")
    assert [s[3] for s in sessions] == ["active_end", "ongoing"]
    assert sessions[0][1] == base + MIN  # 结束事件算在它结束的会话里
    # 再补一个更晚的会话后，原进行中的会话应补记结束
    svc.submit_batch([ev("u", "d", 5, 1, base + 100 * MIN)], received_at_ms=NOW)
    sessions = service_sessions(svc, "u")
    assert [s[3] for s in sessions] == ["active_end", "backfilled", "ongoing"]
    assert sessions[1][1] == base + 3 * MIN  # 补记结束时间取最后一条事件时间
    svc.close()


def test_duration_distribution_filters_and_cap(tmp_path):
    svc = make_service(tmp_path)
    day1 = 1_700_000_000_000  # 2023-11-14T22:13:20Z
    day2 = day1 + 24 * 3600 * 1000
    # 会话1：phone，day1，时长 60s -> bucket 2，主动结束
    svc.submit_batch(
        [
            ev("u", "d1", 1, 1, day1, "open", "phone"),
            ev("u", "d1", 2, 1, day1 + 60_000, "session_end", "phone"),
        ],
        received_at_ms=NOW,
    )
    # 会话2：tablet，day2，每 20 分钟一条事件共 3h -> 归入最后一档
    long_events = [
        ev("u", "d2", i + 1, 1, day2 + i * 20 * MIN, "read", "tablet")
        for i in range(9)
    ]
    long_events.append(
        ev("u", "d2", 10, 1, day2 + 3 * 3600 * 1000, "session_end", "tablet")
    )
    svc.submit_batch(long_events, received_at_ms=NOW)
    all_buckets = svc.duration_distribution()
    assert {(b.bucket_index, b.count) for b in all_buckets} == {
        (2, 1),
        (BUCKET_COUNT - 1, 1),
    }
    phone_only = svc.duration_distribution(device_type="phone")
    assert [(b.bucket_index, b.count) for b in phone_only] == [(2, 1)]
    from datetime import datetime, timezone

    d1 = datetime.fromtimestamp(day1 / 1000, tz=timezone.utc).date().isoformat()
    d2 = datetime.fromtimestamp(day2 / 1000, tz=timezone.utc).date().isoformat()
    assert [(b.bucket_index, b.count) for b in svc.duration_distribution(d1, d1)] == [(2, 1)]
    assert [(b.bucket_index, b.count) for b in svc.duration_distribution(d2, d2)] == [
        (BUCKET_COUNT - 1, 1)
    ]
    svc.close()


def test_batch_validation(tmp_path):
    svc = make_service(tmp_path)
    with pytest.raises(BatchRejectedError):
        svc.submit_batch(
            [ev("u", "d", i, 1, NOW - MIN) for i in range(5001)], received_at_ms=NOW
        )
    with pytest.raises(BatchRejectedError):
        svc.submit_batch(
            [ev("u", "d1", 1, 1, NOW - MIN), ev("u", "d2", 1, 1, NOW - MIN)],
            received_at_ms=NOW,
        )
    svc.close()


def test_ongoing_not_counted_in_distribution(tmp_path):
    svc = make_service(tmp_path)
    svc.submit_batch([ev("u", "d", 1, 1, NOW - 10 * MIN)], received_at_ms=NOW)
    assert svc.duration_distribution() == []
    svc.close()
