"""测试辅助：独立的参照实现，用全量数据从头切一遍作为对比基准。"""
from __future__ import annotations

from datetime import datetime, timezone

from session_stitcher.models import (
    BUCKET_COUNT,
    BUCKET_WIDTH_MS,
    FUTURE_TOLERANCE_MS,
    SESSION_GAP_MS,
    EventInput,
)


def effective_events(
    all_submitted: list[EventInput], received_at_ms: int
) -> list[EventInput]:
    """按受理规则过滤出全部已生效事件（与库内实现相互独立）。"""
    effective: dict[tuple[str, int], EventInput] = {}
    for ev in all_submitted:
        if ev.event_time_ms > received_at_ms + FUTURE_TOLERANCE_MS:
            continue
        key = (ev.device_id, ev.seq)
        cur = effective.get(key)
        if cur is None or (ev.revision, ev.event_time_ms) > (
            cur.revision,
            cur.event_time_ms,
        ):
            effective[key] = ev
    return list(effective.values())


def reference_sessions(events: list[EventInput]) -> list[tuple]:
    """从头切一遍，返回 (start, end, count, status, date, device_type) 元组列表。"""
    if not events:
        return []
    ordered = sorted(events, key=lambda e: (e.event_time_ms, e.device_id, e.seq))
    groups: list[list[EventInput]] = []
    cur: list[EventInput] = []
    prev: EventInput | None = None
    for ev in ordered:
        if prev is not None and (
            ev.event_time_ms - prev.event_time_ms > SESSION_GAP_MS
            or prev.event_name == "session_end"
        ):
            groups.append(cur)
            cur = []
        cur.append(ev)
        prev = ev
    groups.append(cur)

    result = []
    for i, group in enumerate(groups):
        first, last = group[0], group[-1]
        if last.event_name == "session_end":
            status = "active_end"
        elif i == len(groups) - 1:
            status = "ongoing"
        else:
            status = "backfilled"
        date = (
            datetime.fromtimestamp(first.event_time_ms / 1000, tz=timezone.utc)
            .date()
            .isoformat()
        )
        result.append(
            (
                first.event_time_ms,
                last.event_time_ms,
                len(group),
                status,
                date,
                first.device_type,
            )
        )
    return result


def reference_distribution(
    events: list[EventInput],
    start_date: str | None = None,
    end_date: str | None = None,
    device_type: str | None = None,
) -> list[tuple[int, int]]:
    """从参照会话算出时长分布，返回 (bucket, count) 列表。"""
    counts: dict[int, int] = {}
    by_user: dict[str, list[EventInput]] = {}
    for ev in events:
        by_user.setdefault(ev.user_id, []).append(ev)
    for user_events in by_user.values():
        for start, end, _n, status, date, dtype in reference_sessions(user_events):
            if status == "ongoing":
                continue
            if start_date is not None and date < start_date:
                continue
            if end_date is not None and date > end_date:
                continue
            if device_type is not None and dtype != device_type:
                continue
            bucket = min((end - start) // BUCKET_WIDTH_MS, BUCKET_COUNT - 1)
            counts[bucket] = counts.get(bucket, 0) + 1
    return sorted(counts.items())


def service_sessions(service, user_id: str) -> list[tuple]:
    return [
        (s.start_time_ms, s.end_time_ms, s.event_count, s.status.value, s.date, s.device_type)
        for s in service.list_sessions(user_id, 0, 2**62)
    ]


def service_distribution(
    service,
    start_date: str | None = None,
    end_date: str | None = None,
    device_type: str | None = None,
) -> list[tuple[int, int]]:
    return [
        (b.bucket_index, b.count)
        for b in service.duration_distribution(start_date, end_date, device_type)
    ]
