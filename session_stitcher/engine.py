"""会话拼接引擎：提交批次、增量维护会话与时长分布、查询接口。

增量维护的不变量：
- 每个会话是按 (event_time_ms, device_id, seq) 排序的极大连续段，
  段内相邻事件间隔 <= SESSION_GAP_MS，且除末事件外无 session_end。
- 非最后一个会话：末事件是 session_end 则 ENDED，否则 AUTO_CLOSED。
- 最后一个会话：末事件是 session_end 则 ENDED，否则 ACTIVE。
- dist 表只累计 ENDED / AUTO_CLOSED 会话，会话变化时先减旧值再加新值。
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Callable, Optional

from .models import (
    BUCKET_COUNT,
    MAX_BATCH_SIZE,
    MAX_CLOCK_SKEW_MS,
    SESSION_END_EVENT,
    SESSION_GAP_MS,
    Event,
    SessionInfo,
    SessionStatus,
    duration_bucket,
)
from .store import EventRow, SessionRow, Store

_ENDED_STATUSES = (SessionStatus.ENDED, SessionStatus.AUTO_CLOSED)


def _utc_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()


class SessionEngine:
    def __init__(self, db_path: str, clock: Optional[Callable[[], int]] = None) -> None:
        self._store = Store(db_path)
        self._clock = clock or (lambda: int(time.time() * 1000))

    def close(self) -> None:
        self._store.close()

    # ================= 提交 =================

    def submit_batch(self, events: list[Event]) -> dict:
        if len(events) > MAX_BATCH_SIZE:
            raise ValueError(f"batch too large: {len(events)} > {MAX_BATCH_SIZE}")
        if not events:
            return {"inserted": 0, "updated": 0, "duplicates": 0,
                    "stale": 0, "rejected": 0}
        device_id = events[0].device_id
        if any(e.device_id != device_id for e in events):
            raise ValueError("all events in a batch must come from the same device")

        now_ms = self._clock()
        stats = {"inserted": 0, "updated": 0, "duplicates": 0,
                 "stale": 0, "rejected": 0}
        self._store.begin()
        try:
            for ev in events:
                if ev.event_time_ms > now_ms + MAX_CLOCK_SKEW_MS:
                    stats["rejected"] += 1
                    continue
                old = self._store.get_event(ev.device_id, ev.seq)
                if old is None:
                    self._apply_insert(ev)
                    stats["inserted"] += 1
                elif (ev.revision, ev.event_time_ms) > (old.revision, old.event_time_ms):
                    self._apply_delete(old)
                    self._apply_insert(ev)
                    stats["updated"] += 1
                elif (ev.revision, ev.event_time_ms) == (old.revision, old.event_time_ms):
                    stats["duplicates"] += 1
                else:
                    stats["stale"] += 1
            self._store.commit()
        except Exception:
            self._store.rollback()
            raise
        return stats

    # ================= 查询 =================

    def list_sessions(
        self, user_id: str, start_ms: int, end_ms: int
    ) -> list[SessionInfo]:
        return [s.to_info() for s in self._store.list_sessions(user_id, start_ms, end_ms)]

    def duration_distribution(
        self,
        user_id: str,
        date_from: str,
        date_to: str,
        device_type: Optional[str] = None,
    ) -> list[int]:
        counts = self._store.query_dist(user_id, date_from, date_to, device_type)
        result = [0] * BUCKET_COUNT
        for bucket, count in counts.items():
            if 0 <= bucket < BUCKET_COUNT:
                result[bucket] = count
        return result

    # ================= 时长分布记账 =================

    def _dist_remove(self, s: SessionRow) -> None:
        if s.status in _ENDED_STATUSES:
            self._store.add_dist(
                s.user_id, s.date, s.device_type,
                duration_bucket(s.end_time_ms - s.start_time_ms), -1,
            )

    def _dist_add(self, s: SessionRow) -> None:
        if s.status in _ENDED_STATUSES:
            self._store.add_dist(
                s.user_id, s.date, s.device_type,
                duration_bucket(s.end_time_ms - s.start_time_ms), 1,
            )

    def _replace_session(self, old: SessionRow, new: SessionRow) -> None:
        """更新会话行并同步时长分布（先减旧值，再加新值）。"""
        self._dist_remove(old)
        self._store.update_session(new)
        self._dist_add(new)

    # ================= 增量插入 =================

    def _apply_insert(self, ev: Event) -> None:
        store = self._store
        key = (ev.event_time_ms, ev.device_id, ev.seq)
        prev = store.prev_event(ev.user_id, key)
        nxt = store.next_event(ev.user_id, key)

        boundary_before = (
            prev is None
            or ev.event_time_ms - prev.event_time_ms > SESSION_GAP_MS
            or prev.event_name == SESSION_END_EVENT
        )
        boundary_after = (
            nxt is None
            or nxt.event_time_ms - ev.event_time_ms > SESSION_GAP_MS
            or ev.event_name == SESSION_END_EVENT
        )

        if boundary_before and boundary_after:
            self._insert_as_new_session(ev, prev, nxt)
        elif not boundary_before and boundary_after:
            self._insert_append_to_prev(ev, prev, nxt)
        elif boundary_before and not boundary_after:
            self._insert_prepend_to_next(ev, nxt)
        elif prev.session_id == nxt.session_id:
            # 前后邻居同属一个会话：直接插入该会话中部
            session = self._store.get_session(prev.session_id)
            self._store.insert_event(ev, session.id)
            self._replace_session(session, SessionRow(
                **{**session.__dict__, "event_count": session.event_count + 1}))
        else:
            self._insert_merge(ev, prev, nxt)

    def _insert_as_new_session(self, ev: Event, prev: Optional[EventRow],
                               nxt: Optional[EventRow]) -> None:
        if ev.event_name == SESSION_END_EVENT:
            status = SessionStatus.ENDED
        elif nxt is None:
            status = SessionStatus.ACTIVE
        else:
            status = SessionStatus.AUTO_CLOSED
        session = SessionRow(
            id=0, user_id=ev.user_id, start_time_ms=ev.event_time_ms,
            end_time_ms=ev.event_time_ms, event_count=1, status=status,
            date=_utc_date(ev.event_time_ms), device_type=ev.device_type,
            first_device_id=ev.device_id, first_seq=ev.seq,
            last_device_id=ev.device_id, last_seq=ev.seq,
            last_event_name=ev.event_name,
        )
        session_id = self._store.create_session(session)
        self._store.insert_event(ev, session_id)
        self._dist_add(SessionRow(**{**session.__dict__, "id": session_id}))
        # 新会话出现前，若前一个会话是“进行中”，现在补记结束
        if nxt is None and prev is not None:
            prev_session = self._store.get_session(prev.session_id)
            if prev_session is not None and prev_session.status == SessionStatus.ACTIVE:
                self._replace_session(prev_session, SessionRow(
                    **{**prev_session.__dict__, "status": SessionStatus.AUTO_CLOSED}))

    def _insert_append_to_prev(self, ev: Event, prev: EventRow,
                               nxt: Optional[EventRow]) -> None:
        session = self._store.get_session(prev.session_id)
        if (ev.event_name == SESSION_END_EVENT and nxt is not None
                and nxt.session_id == session.id):
            # session_end 插到会话中部：先并入，再把它后面的部分拆成新会话
            self._store.insert_event(ev, session.id)
            ev_row = EventRow(
                user_id=ev.user_id, device_id=ev.device_id, seq=ev.seq,
                revision=ev.revision, event_time_ms=ev.event_time_ms,
                event_name=ev.event_name, device_type=ev.device_type,
                session_id=session.id)
            self._split_session(session, session.event_count + 1, ev_row, nxt)
            return
        if ev.event_name == SESSION_END_EVENT:
            status = SessionStatus.ENDED
        elif nxt is None:
            status = SessionStatus.ACTIVE
        else:
            status = SessionStatus.AUTO_CLOSED
        new = SessionRow(
            **{**session.__dict__,
               "end_time_ms": ev.event_time_ms,
               "event_count": session.event_count + 1,
               "status": status,
               "last_device_id": ev.device_id,
               "last_seq": ev.seq,
               "last_event_name": ev.event_name})
        self._store.insert_event(ev, session.id)
        self._replace_session(session, new)

    def _insert_prepend_to_next(self, ev: Event, nxt: EventRow) -> None:
        session = self._store.get_session(nxt.session_id)
        new = SessionRow(
            **{**session.__dict__,
               "start_time_ms": ev.event_time_ms,
               "event_count": session.event_count + 1,
               "date": _utc_date(ev.event_time_ms),
               "device_type": ev.device_type,
               "first_device_id": ev.device_id,
               "first_seq": ev.seq})
        self._store.insert_event(ev, session.id)
        self._replace_session(session, new)

    def _insert_merge(self, ev: Event, prev: EventRow, nxt: EventRow) -> None:
        left = self._store.get_session(prev.session_id)
        right = self._store.get_session(nxt.session_id)
        merged_count = left.event_count + right.event_count + 1
        # 保留事件多的一方的会话 id，减少重挂事件的开销
        if left.event_count >= right.event_count:
            survivor, absorbed = left, right
        else:
            survivor, absorbed = right, left
        self._dist_remove(left)
        self._dist_remove(right)
        self._store.insert_event(ev, survivor.id)
        self._store.reassign_session_events(absorbed.id, survivor.id)
        self._store.delete_session(absorbed.id)
        merged = SessionRow(
            **{**survivor.__dict__,
               "start_time_ms": left.start_time_ms,
               "end_time_ms": right.end_time_ms,
               "event_count": merged_count,
               "status": right.status,
               "date": left.date,
               "device_type": left.device_type,
               "first_device_id": left.first_device_id,
               "first_seq": left.first_seq,
               "last_device_id": right.last_device_id,
               "last_seq": right.last_seq,
               "last_event_name": right.last_event_name})
        self._store.update_session(merged)
        self._dist_add(merged)

    # ================= 增量删除 =================

    def _apply_delete(self, ev: EventRow) -> None:
        store = self._store
        session = store.get_session(ev.session_id)
        store.delete_event(ev.device_id, ev.seq)

        if session.event_count == 1:
            self._dist_remove(session)
            store.delete_session(session.id)
            self._maybe_merge_neighbors(session)
            self._refresh_last_session(ev.user_id)
            return

        remaining = session.event_count - 1
        key = ev.order_key
        is_first = (session.first_device_id, session.first_seq) == (ev.device_id, ev.seq)
        is_last = (session.last_device_id, session.last_seq) == (ev.device_id, ev.seq)

        if is_first:
            new_first = store.next_event(ev.user_id, key)
            new = SessionRow(
                **{**session.__dict__,
                   "start_time_ms": new_first.event_time_ms,
                   "event_count": remaining,
                   "date": _utc_date(new_first.event_time_ms),
                   "device_type": new_first.device_type,
                   "first_device_id": new_first.device_id,
                   "first_seq": new_first.seq})
            self._replace_session(session, new)
        elif is_last:
            new_last = store.prev_event(ev.user_id, key)
            status = session.status
            if ev.event_name == SESSION_END_EVENT:
                # 结束事件被删，会话不再主动结束
                status = SessionStatus.AUTO_CLOSED
            new = SessionRow(
                **{**session.__dict__,
                   "end_time_ms": new_last.event_time_ms,
                   "event_count": remaining,
                   "status": status,
                   "last_device_id": new_last.device_id,
                   "last_seq": new_last.seq,
                   "last_event_name": new_last.event_name})
            self._replace_session(session, new)
            if ev.event_name == SESSION_END_EVENT:
                self._maybe_merge_with_next(self._store.get_session(session.id))
        else:
            prev = store.prev_event(ev.user_id, key)
            nxt = store.next_event(ev.user_id, key)
            need_split = (
                nxt.event_time_ms - prev.event_time_ms > SESSION_GAP_MS
                or prev.event_name == SESSION_END_EVENT
            )
            if need_split:
                self._split_session(session, remaining, prev, nxt)
            else:
                self._replace_session(session, SessionRow(
                    **{**session.__dict__, "event_count": remaining}))

        self._refresh_last_session(ev.user_id)

    def _split_session(self, session: SessionRow, remaining: int,
                       left_last: EventRow, right_first: EventRow) -> None:
        """在 left_last 与 right_first 之间把会话切成两段。"""
        store = self._store
        self._dist_remove(session)
        right_count = store.count_session_events_after(session.id, left_last.order_key)
        left_count = remaining - right_count
        left_status = (SessionStatus.ENDED if left_last.event_name == SESSION_END_EVENT
                       else SessionStatus.AUTO_CLOSED)

        if right_count <= left_count:
            # 右段新建会话，左段保留原 id
            right = SessionRow(
                id=0, user_id=session.user_id,
                start_time_ms=right_first.event_time_ms,
                end_time_ms=session.end_time_ms,
                event_count=right_count, status=session.status,
                date=_utc_date(right_first.event_time_ms),
                device_type=right_first.device_type,
                first_device_id=right_first.device_id, first_seq=right_first.seq,
                last_device_id=session.last_device_id, last_seq=session.last_seq,
                last_event_name=session.last_event_name)
            right_id = store.create_session(right)
            store.reassign_events_after(session.id, right_id, left_last.order_key)
            left = SessionRow(
                **{**session.__dict__,
                   "end_time_ms": left_last.event_time_ms,
                   "event_count": left_count, "status": left_status,
                   "last_device_id": left_last.device_id,
                   "last_seq": left_last.seq,
                   "last_event_name": left_last.event_name})
            store.update_session(left)
            self._dist_add(left)
            self._dist_add(SessionRow(**{**right.__dict__, "id": right_id}))
        else:
            # 左段新建会话，右段保留原 id
            left = SessionRow(
                id=0, user_id=session.user_id,
                start_time_ms=session.start_time_ms,
                end_time_ms=left_last.event_time_ms,
                event_count=left_count, status=left_status,
                date=session.date, device_type=session.device_type,
                first_device_id=session.first_device_id,
                first_seq=session.first_seq,
                last_device_id=left_last.device_id, last_seq=left_last.seq,
                last_event_name=left_last.event_name)
            left_id = store.create_session(left)
            store.reassign_events_before(session.id, left_id, left_last.order_key)
            right = SessionRow(
                **{**session.__dict__,
                   "start_time_ms": right_first.event_time_ms,
                   "event_count": right_count,
                   "date": _utc_date(right_first.event_time_ms),
                   "device_type": right_first.device_type,
                   "first_device_id": right_first.device_id,
                   "first_seq": right_first.seq})
            store.update_session(right)
            self._dist_add(SessionRow(**{**left.__dict__, "id": left_id}))
            self._dist_add(right)

    def _maybe_merge_neighbors(self, deleted: SessionRow) -> None:
        """单事件会话被删后，检查左右两个会话是否应当合并。"""
        store = self._store
        prev = store.prev_session(deleted.user_id, deleted.first_key)
        nxt = store.next_session(deleted.user_id, deleted.first_key)
        if prev is None or nxt is None:
            return
        if (nxt.start_time_ms - prev.end_time_ms <= SESSION_GAP_MS
                and prev.last_event_name != SESSION_END_EVENT):
            self._merge_sessions(prev, nxt)

    def _maybe_merge_with_next(self, session: Optional[SessionRow]) -> None:
        if session is None:
            return
        nxt = self._store.next_session(session.user_id, session.first_key)
        if nxt is None:
            return
        if (nxt.start_time_ms - session.end_time_ms <= SESSION_GAP_MS
                and session.last_event_name != SESSION_END_EVENT):
            self._merge_sessions(session, nxt)

    def _merge_sessions(self, left: SessionRow, right: SessionRow) -> None:
        store = self._store
        self._dist_remove(left)
        self._dist_remove(right)
        if left.event_count >= right.event_count:
            survivor, absorbed = left, right
        else:
            survivor, absorbed = right, left
        store.reassign_session_events(absorbed.id, survivor.id)
        store.delete_session(absorbed.id)
        merged = SessionRow(
            **{**survivor.__dict__,
               "start_time_ms": left.start_time_ms,
               "end_time_ms": right.end_time_ms,
               "event_count": left.event_count + right.event_count,
               "status": right.status,
               "date": left.date,
               "device_type": left.device_type,
               "first_device_id": left.first_device_id,
               "first_seq": left.first_seq,
               "last_device_id": right.last_device_id,
               "last_seq": right.last_seq,
               "last_event_name": right.last_event_name})
        store.update_session(merged)
        self._dist_add(merged)

    def _refresh_last_session(self, user_id: str) -> None:
        """用户最后一个会话若是补记结束状态，应恢复为进行中。"""
        last = self._store.last_session(user_id)
        if last is not None and last.status == SessionStatus.AUTO_CLOSED:
            self._replace_session(last, SessionRow(
                **{**last.__dict__, "status": SessionStatus.ACTIVE}))
