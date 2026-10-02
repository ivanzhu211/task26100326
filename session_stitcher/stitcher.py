"""会话切分与增量维护逻辑。"""
from __future__ import annotations

from datetime import datetime, timezone

from .models import (
    FUTURE_TOLERANCE_MS,
    SESSION_END_EVENT,
    SESSION_GAP_MS,
    EventInput,
    Session,
    SessionStatus,
    StoredEvent,
    duration_bucket_index,
)
from .storage import Storage


def _utc_date(event_time_ms: int) -> str:
    return datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).date().isoformat()


def cut_sessions(events: list[StoredEvent], has_successor: bool) -> list[Session]:
    """把一组已排序的已生效事件从头切成会话。

    has_successor 表示该用户在这些事件之后是否还有其它事件（会话），
    用于决定最后一个无结束事件的会话是补记结束还是进行中。
    """
    if not events:
        return []
    ordered = sorted(events, key=lambda e: e.sort_key)
    groups: list[list[StoredEvent]] = [[ordered[0]]]
    for prev, cur in zip(ordered, ordered[1:]):
        if (
            cur.event_time_ms - prev.event_time_ms > SESSION_GAP_MS
            or prev.event_name == SESSION_END_EVENT
        ):
            groups.append([cur])
        else:
            groups[-1].append(cur)

    sessions: list[Session] = []
    for index, group in enumerate(groups):
        first, last = group[0], group[-1]
        is_last_group = index == len(groups) - 1
        if last.event_name == SESSION_END_EVENT:
            status = SessionStatus.ACTIVE_END
        elif is_last_group and not has_successor:
            status = SessionStatus.ONGOING
        else:
            status = SessionStatus.BACKFILLED
        sessions.append(
            Session(
                user_id=first.user_id,
                start_time_ms=first.event_time_ms,
                end_time_ms=last.event_time_ms,
                event_count=len(group),
                status=status,
                date=_utc_date(first.event_time_ms),
                device_type=first.device_type,
                first_device_id=first.device_id,
                first_seq=first.seq,
                last_device_id=last.device_id,
                last_seq=last.seq,
            )
        )
    return sessions


class Stitcher:
    """在 Storage 之上做增量会话维护。"""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def apply_batch(self, events: list[EventInput], received_at_ms: int) -> dict:
        """应用一批事件，返回受理/拒收/重复的统计。"""
        stats = {"accepted": 0, "duplicates": 0, "rejected": 0}
        changes_by_user: dict[str, list[tuple[StoredEvent | None, StoredEvent]]] = {}
        for ev in events:
            if ev.event_time_ms > received_at_ms + FUTURE_TOLERANCE_MS:
                stats["rejected"] += 1
                continue
            existing = self._storage.get_event(ev.device_id, ev.seq)
            if existing is not None:
                if ev.revision < existing.revision:
                    stats["duplicates"] += 1
                    continue
                if ev.revision == existing.revision:
                    if ev.event_time_ms <= existing.event_time_ms:
                        stats["duplicates"] += 1
                        continue
            stored = StoredEvent(
                user_id=ev.user_id,
                device_id=ev.device_id,
                seq=ev.seq,
                revision=ev.revision,
                event_time_ms=ev.event_time_ms,
                event_name=ev.event_name,
                device_type=ev.device_type,
            )
            self._storage.upsert_event(stored, received_at_ms)
            stats["accepted"] += 1
            changes_by_user.setdefault(ev.user_id, []).append((existing, stored))
        for user_id, changes in changes_by_user.items():
            self._recut_user(user_id, changes)
        return stats

    def _recut_user(
        self, user_id: str, changes: list[tuple[StoredEvent | None, StoredEvent]]
    ) -> None:
        """围绕发生变化的事件做局部重切，并同步时长分布。"""
        # 同一序号在一批内可能变更多次：合并为（批前旧版本 -> 最终新版本）
        consolidated: dict[tuple[str, int], tuple[StoredEvent | None, StoredEvent]] = {}
        for old, new in changes:
            key = (new.device_id, new.seq)
            if key in consolidated:
                consolidated[key] = (consolidated[key][0], new)
            else:
                consolidated[key] = (old, new)
        changes = list(consolidated.values())

        times: list[int] = []
        for old, new in changes:
            if old is not None:
                times.append(old.event_time_ms)
            times.append(new.event_time_ms)
        win_start = min(times) - SESSION_GAP_MS
        win_end = max(times) + SESSION_GAP_MS

        doomed = self._storage.find_sessions_overlapping(user_id, win_start, win_end)
        self._storage.delete_sessions(doomed)
        for session in doomed:
            if session.status is not SessionStatus.ONGOING:
                self._add_dist(session, -1)

        if doomed:
            run_start = (doomed[0].start_time_ms, doomed[0].first_device_id, doomed[0].first_seq)
            run_end = (doomed[-1].end_time_ms, doomed[-1].last_device_id, doomed[-1].last_seq)
            pool = self._storage.fetch_events_between(user_id, run_start, run_end)
            seen = {(e.device_id, e.seq) for e in pool}
            for _, new in changes:
                if (new.device_id, new.seq) not in seen:
                    pool.append(new)
                    seen.add((new.device_id, new.seq))
        else:
            pool = [new for _, new in changes]

        pool.sort(key=lambda e: e.sort_key)
        has_successor = self._storage.has_event_after(user_id, pool[-1].sort_key)
        new_sessions = cut_sessions(pool, has_successor)
        for session in new_sessions:
            self._storage.insert_session(session)
            if session.status is not SessionStatus.ONGOING:
                self._add_dist(session, +1)

        if new_sessions:
            first = new_sessions[0]
            predecessor = self._storage.get_session_before(
                user_id, (first.start_time_ms, first.first_device_id, first.first_seq)
            )
            if predecessor is not None and predecessor.status is SessionStatus.ONGOING:
                self._storage.update_session_status(predecessor, SessionStatus.BACKFILLED)
                self._add_dist(predecessor, +1)

        last = self._storage.get_last_session(user_id)
        if last is not None and last.status is SessionStatus.BACKFILLED:
            self._storage.update_session_status(last, SessionStatus.ONGOING)
            self._add_dist(last, -1)

    def _add_dist(self, session: Session, delta: int) -> None:
        self._storage.add_duration(
            session.date,
            session.device_type,
            duration_bucket_index(session.duration_ms),
            delta,
        )
