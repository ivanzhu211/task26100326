"""参照实现：把全部已生效事件从头切一遍。用于校验增量引擎的一致性。"""
from __future__ import annotations

from datetime import datetime, timezone

from session_stitcher.models import (
    BUCKET_COUNT,
    MAX_CLOCK_SKEW_MS,
    SESSION_END_EVENT,
    SESSION_GAP_MS,
    Event,
    SessionStatus,
    duration_bucket,
)


def effective_events(arrival_order: list[Event], now_ms: int) -> dict[str, list[Event]]:
    """按到达顺序应用拒收/修订/去重规则，返回每个用户的已生效事件集。"""
    chosen: dict[tuple[str, int], Event] = {}
    for ev in arrival_order:
        if ev.event_time_ms > now_ms + MAX_CLOCK_SKEW_MS:
            continue
        key = (ev.device_id, ev.seq)
        old = chosen.get(key)
        if old is None or (ev.revision, ev.event_time_ms) > (old.revision, old.event_time_ms):
            chosen[key] = ev
    by_user: dict[str, list[Event]] = {}
    for ev in chosen.values():
        by_user.setdefault(ev.user_id, []).append(ev)
    return by_user


def _utc_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()


def reference_sessions(events: list[Event]) -> list[tuple]:
    """从头切会话，返回可比较的规范化元组列表。"""
    ordered = sorted(events, key=lambda e: (e.event_time_ms, e.device_id, e.seq))
    sessions: list[list[Event]] = []
    for ev in ordered:
        if sessions:
            prev = sessions[-1][-1]
            if (ev.event_time_ms - prev.event_time_ms <= SESSION_GAP_MS
                    and prev.event_name != SESSION_END_EVENT):
                sessions[-1].append(ev)
                continue
        sessions.append([ev])

    result = []
    for i, group in enumerate(sessions):
        last = group[-1]
        if last.event_name == SESSION_END_EVENT:
            status = SessionStatus.ENDED
        elif i < len(sessions) - 1:
            status = SessionStatus.AUTO_CLOSED
        else:
            status = SessionStatus.ACTIVE
        result.append((
            group[0].event_time_ms,
            last.event_time_ms,
            len(group),
            status.value,
            _utc_date(group[0].event_time_ms),
            group[0].device_type,
        ))
    return result


def reference_dist(sessions: list[tuple], date_from: str, date_to: str,
                   device_type: str | None = None) -> list[int]:
    result = [0] * BUCKET_COUNT
    for start, end, _count, status, date, dtype in sessions:
        if status == SessionStatus.ACTIVE.value:
            continue
        if not (date_from <= date <= date_to):
            continue
        if device_type is not None and dtype != device_type:
            continue
        result[duration_bucket(end - start)] += 1
    return result
