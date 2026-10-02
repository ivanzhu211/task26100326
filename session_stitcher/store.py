"""SQLite 持久化层：事件、会话、时长分布聚合。"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterator, Optional

from .models import Event, SessionInfo, SessionStatus


@dataclass(frozen=True)
class EventRow:
    """已入库的事件（带所属会话）。"""

    user_id: str
    device_id: str
    seq: int
    revision: int
    event_time_ms: int
    event_name: str
    device_type: str
    session_id: Optional[int]

    @property
    def order_key(self) -> tuple[int, str, int]:
        return (self.event_time_ms, self.device_id, self.seq)


@dataclass(frozen=True)
class SessionRow:
    id: int
    user_id: str
    start_time_ms: int
    end_time_ms: int
    event_count: int
    status: SessionStatus
    date: str
    device_type: str
    # 首/末事件的排序键与末事件名，用于边界判断
    first_device_id: str
    first_seq: int
    last_device_id: str
    last_seq: int
    last_event_name: str

    @property
    def first_key(self) -> tuple[int, str, int]:
        return (self.start_time_ms, self.first_device_id, self.first_seq)

    @property
    def last_key(self) -> tuple[int, str, int]:
        return (self.end_time_ms, self.last_device_id, self.last_seq)

    def to_info(self) -> SessionInfo:
        return SessionInfo(
            session_id=self.id,
            user_id=self.user_id,
            start_time_ms=self.start_time_ms,
            end_time_ms=self.end_time_ms,
            event_count=self.event_count,
            status=self.status,
            date=self.date,
            device_type=self.device_type,
        )


_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    device_id      TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    user_id        TEXT NOT NULL,
    revision       INTEGER NOT NULL,
    event_time_ms  INTEGER NOT NULL,
    event_name     TEXT NOT NULL,
    device_type    TEXT NOT NULL,
    session_id     INTEGER,
    PRIMARY KEY (device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_user_order
    ON events (user_id, event_time_ms, device_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_session
    ON events (session_id);

CREATE TABLE IF NOT EXISTS sessions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id          TEXT NOT NULL,
    start_time_ms    INTEGER NOT NULL,
    end_time_ms      INTEGER NOT NULL,
    event_count      INTEGER NOT NULL,
    status           TEXT NOT NULL,
    date             TEXT NOT NULL,
    device_type      TEXT NOT NULL,
    first_device_id  TEXT NOT NULL,
    first_seq        INTEGER NOT NULL,
    last_device_id   TEXT NOT NULL,
    last_seq         INTEGER NOT NULL,
    last_event_name  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_user_time
    ON sessions (user_id, start_time_ms);

CREATE TABLE IF NOT EXISTS dist (
    user_id     TEXT NOT NULL,
    date        TEXT NOT NULL,
    device_type TEXT NOT NULL,
    bucket      INTEGER NOT NULL,
    count       INTEGER NOT NULL,
    PRIMARY KEY (user_id, date, device_type, bucket)
);
"""


def _to_event_row(r: tuple) -> EventRow:
    return EventRow(
        user_id=r[0], device_id=r[1], seq=r[2], revision=r[3],
        event_time_ms=r[4], event_name=r[5], device_type=r[6], session_id=r[7],
    )


def _to_session_row(r: tuple) -> SessionRow:
    return SessionRow(
        id=r[0], user_id=r[1], start_time_ms=r[2], end_time_ms=r[3],
        event_count=r[4], status=SessionStatus(r[5]), date=r[6],
        device_type=r[7], first_device_id=r[8], first_seq=r[9],
        last_device_id=r[10], last_seq=r[11], last_event_name=r[12],
    )


_EVENT_COLS = "user_id, device_id, seq, revision, event_time_ms, event_name, device_type, session_id"
_SESSION_COLS = (
    "id, user_id, start_time_ms, end_time_ms, event_count, status, date, device_type, "
    "first_device_id, first_seq, last_device_id, last_seq, last_event_name"
)


class Store:
    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    def _create_schema(self) -> None:
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---------------- 事件 ----------------

    def get_event(self, device_id: str, seq: int) -> Optional[EventRow]:
        cur = self._conn.execute(
            f"SELECT {_EVENT_COLS} FROM events WHERE device_id=? AND seq=?",
            (device_id, seq),
        )
        row = cur.fetchone()
        return _to_event_row(row) if row else None

    def insert_event(self, ev: Event, session_id: Optional[int]) -> None:
        self._conn.execute(
            "INSERT INTO events (device_id, seq, user_id, revision, event_time_ms,"
            " event_name, device_type, session_id) VALUES (?,?,?,?,?,?,?,?)",
            (ev.device_id, ev.seq, ev.user_id, ev.revision, ev.event_time_ms,
             ev.event_name, ev.device_type, session_id),
        )

    def delete_event(self, device_id: str, seq: int) -> None:
        self._conn.execute(
            "DELETE FROM events WHERE device_id=? AND seq=?", (device_id, seq)
        )

    def prev_event(self, user_id: str, key: tuple[int, str, int]) -> Optional[EventRow]:
        cur = self._conn.execute(
            f"SELECT {_EVENT_COLS} FROM events WHERE user_id=?"
            " AND (event_time_ms, device_id, seq) < (?,?,?)"
            " ORDER BY event_time_ms DESC, device_id DESC, seq DESC LIMIT 1",
            (user_id, key[0], key[1], key[2]),
        )
        row = cur.fetchone()
        return _to_event_row(row) if row else None

    def next_event(self, user_id: str, key: tuple[int, str, int]) -> Optional[EventRow]:
        cur = self._conn.execute(
            f"SELECT {_EVENT_COLS} FROM events WHERE user_id=?"
            " AND (event_time_ms, device_id, seq) > (?,?,?)"
            " ORDER BY event_time_ms, device_id, seq LIMIT 1",
            (user_id, key[0], key[1], key[2]),
        )
        row = cur.fetchone()
        return _to_event_row(row) if row else None

    def iter_session_events(self, session_id: int) -> Iterator[EventRow]:
        cur = self._conn.execute(
            f"SELECT {_EVENT_COLS} FROM events WHERE session_id=?"
            " ORDER BY event_time_ms, device_id, seq",
            (session_id,),
        )
        for row in cur:
            yield _to_event_row(row)

    def count_session_events(self, session_id: int) -> int:
        cur = self._conn.execute(
            "SELECT COUNT(*) FROM events WHERE session_id=?", (session_id,)
        )
        return cur.fetchone()[0]

    def reassign_session_events(self, from_session_id: int, to_session_id: int) -> None:
        self._conn.execute(
            "UPDATE events SET session_id=? WHERE session_id=?",
            (to_session_id, from_session_id),
        )

    def reassign_events_after(
        self, from_session_id: int, to_session_id: int, key: tuple[int, str, int]
    ) -> None:
        """把 from 会话中排序键严格大于 key 的事件划到 to 会话。"""
        self._conn.execute(
            "UPDATE events SET session_id=? WHERE session_id=?"
            " AND (event_time_ms, device_id, seq) > (?,?,?)",
            (to_session_id, from_session_id, key[0], key[1], key[2]),
        )

    def reassign_events_before(
        self, from_session_id: int, to_session_id: int, key: tuple[int, str, int]
    ) -> None:
        """把 from 会话中排序键小于等于 key 的事件划到 to 会话。"""
        self._conn.execute(
            "UPDATE events SET session_id=? WHERE session_id=?"
            " AND (event_time_ms, device_id, seq) <= (?,?,?)",
            (to_session_id, from_session_id, key[0], key[1], key[2]),
        )

    # ---------------- 会话 ----------------

    def create_session(self, s: SessionRow) -> int:
        cur = self._conn.execute(
            "INSERT INTO sessions (user_id, start_time_ms, end_time_ms, event_count,"
            " status, date, device_type, first_device_id, first_seq,"
            " last_device_id, last_seq, last_event_name)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (s.user_id, s.start_time_ms, s.end_time_ms, s.event_count, s.status.value,
             s.date, s.device_type, s.first_device_id, s.first_seq,
             s.last_device_id, s.last_seq, s.last_event_name),
        )
        return cur.lastrowid

    def update_session(self, s: SessionRow) -> None:
        self._conn.execute(
            "UPDATE sessions SET user_id=?, start_time_ms=?, end_time_ms=?,"
            " event_count=?, status=?, date=?, device_type=?, first_device_id=?,"
            " first_seq=?, last_device_id=?, last_seq=?, last_event_name=?"
            " WHERE id=?",
            (s.user_id, s.start_time_ms, s.end_time_ms, s.event_count, s.status.value,
             s.date, s.device_type, s.first_device_id, s.first_seq,
             s.last_device_id, s.last_seq, s.last_event_name, s.id),
        )

    def delete_session(self, session_id: int) -> None:
        self._conn.execute("DELETE FROM sessions WHERE id=?", (session_id,))

    def get_session(self, session_id: int) -> Optional[SessionRow]:
        cur = self._conn.execute(
            f"SELECT {_SESSION_COLS} FROM sessions WHERE id=?", (session_id,)
        )
        row = cur.fetchone()
        return _to_session_row(row) if row else None

    def next_session(self, user_id: str, first_key: tuple[int, str, int]) -> Optional[SessionRow]:
        cur = self._conn.execute(
            f"SELECT {_SESSION_COLS} FROM sessions WHERE user_id=?"
            " AND (start_time_ms, first_device_id, first_seq) > (?,?,?)"
            " ORDER BY start_time_ms, first_device_id, first_seq LIMIT 1",
            (user_id, first_key[0], first_key[1], first_key[2]),
        )
        row = cur.fetchone()
        return _to_session_row(row) if row else None

    def prev_session(self, user_id: str, first_key: tuple[int, str, int]) -> Optional[SessionRow]:
        cur = self._conn.execute(
            f"SELECT {_SESSION_COLS} FROM sessions WHERE user_id=?"
            " AND (start_time_ms, first_device_id, first_seq) < (?,?,?)"
            " ORDER BY start_time_ms DESC, first_device_id DESC, first_seq DESC LIMIT 1",
            (user_id, first_key[0], first_key[1], first_key[2]),
        )
        row = cur.fetchone()
        return _to_session_row(row) if row else None

    def last_session(self, user_id: str) -> Optional[SessionRow]:
        cur = self._conn.execute(
            f"SELECT {_SESSION_COLS} FROM sessions WHERE user_id=?"
            " ORDER BY start_time_ms DESC, first_device_id DESC, first_seq DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        return _to_session_row(row) if row else None

    def list_sessions(self, user_id: str, start_ms: int, end_ms: int) -> list[SessionRow]:
        """返回与 [start_ms, end_ms] 时间区间有交集的会话，按开始时间排序。"""
        cur = self._conn.execute(
            f"SELECT {_SESSION_COLS} FROM sessions WHERE user_id=?"
            " AND start_time_ms <= ? AND end_time_ms >= ?"
            " ORDER BY start_time_ms, first_device_id, first_seq",
            (user_id, end_ms, start_ms),
        )
        return [_to_session_row(r) for r in cur]

    def count_session_events_after(self, session_id: int, key: tuple[int, str, int]) -> int:
        cur = self._conn.execute(
            "SELECT COUNT(*) FROM events WHERE session_id=?"
            " AND (event_time_ms, device_id, seq) > (?,?,?)",
            (session_id, key[0], key[1], key[2]),
        )
        return cur.fetchone()[0]

    # ---------------- 时长分布 ----------------

    def add_dist(self, user_id: str, date: str, device_type: str, bucket: int, delta: int) -> None:
        self._conn.execute(
            "INSERT INTO dist (user_id, date, device_type, bucket, count)"
            " VALUES (?,?,?,?,?)"
            " ON CONFLICT (user_id, date, device_type, bucket)"
            " DO UPDATE SET count = count + excluded.count",
            (user_id, date, device_type, bucket, delta),
        )
        self._conn.execute(
            "DELETE FROM dist WHERE user_id=? AND date=? AND device_type=?"
            " AND bucket=? AND count=0",
            (user_id, date, device_type, bucket),
        )

    def query_dist(
        self, user_id: str, date_from: str, date_to: str, device_type: Optional[str]
    ) -> dict[int, int]:
        if device_type is None:
            cur = self._conn.execute(
                "SELECT bucket, SUM(count) FROM dist WHERE user_id=?"
                " AND date BETWEEN ? AND ? GROUP BY bucket",
                (user_id, date_from, date_to),
            )
        else:
            cur = self._conn.execute(
                "SELECT bucket, SUM(count) FROM dist WHERE user_id=?"
                " AND date BETWEEN ? AND ? AND device_type=? GROUP BY bucket",
                (user_id, date_from, date_to, device_type),
            )
        return {row[0]: row[1] for row in cur}

    # ---------------- 事务 ----------------

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()
