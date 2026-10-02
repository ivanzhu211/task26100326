"""SQLite 持久化层：事件表、会话表、时长分布表。"""
from __future__ import annotations

import sqlite3

from .models import Session, SessionStatus, StoredEvent

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    device_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    user_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    event_time_ms INTEGER NOT NULL,
    event_name TEXT NOT NULL,
    device_type TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    PRIMARY KEY (device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_user_order
    ON events(user_id, event_time_ms, device_id, seq);

CREATE TABLE IF NOT EXISTS sessions (
    user_id TEXT NOT NULL,
    start_time_ms INTEGER NOT NULL,
    end_time_ms INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    date TEXT NOT NULL,
    device_type TEXT NOT NULL,
    first_device_id TEXT NOT NULL,
    first_seq INTEGER NOT NULL,
    last_device_id TEXT NOT NULL,
    last_seq INTEGER NOT NULL,
    PRIMARY KEY (user_id, first_device_id, first_seq)
);
CREATE INDEX IF NOT EXISTS idx_sessions_user_time
    ON sessions(user_id, start_time_ms, end_time_ms);
CREATE INDEX IF NOT EXISTS idx_sessions_user_end
    ON sessions(user_id, end_time_ms);

CREATE TABLE IF NOT EXISTS duration_dist (
    date TEXT NOT NULL,
    device_type TEXT NOT NULL,
    bucket INTEGER NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (date, device_type, bucket)
);
"""


def _row_to_event(row: sqlite3.Row) -> StoredEvent:
    return StoredEvent(
        user_id=row["user_id"],
        device_id=row["device_id"],
        seq=row["seq"],
        revision=row["revision"],
        event_time_ms=row["event_time_ms"],
        event_name=row["event_name"],
        device_type=row["device_type"],
    )


def _row_to_session(row: sqlite3.Row) -> Session:
    return Session(
        user_id=row["user_id"],
        start_time_ms=row["start_time_ms"],
        end_time_ms=row["end_time_ms"],
        event_count=row["event_count"],
        status=SessionStatus(row["status"]),
        date=row["date"],
        device_type=row["device_type"],
        first_device_id=row["first_device_id"],
        first_seq=row["first_seq"],
        last_device_id=row["last_device_id"],
        last_seq=row["last_seq"],
    )


class Storage:
    """封装 SQLite 读写。所有方法假定调用方已在外层事务中。"""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---- 事务 ----

    def begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    # ---- 事件 ----

    def get_event(self, device_id: str, seq: int) -> StoredEvent | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE device_id=? AND seq=?",
            (device_id, seq),
        ).fetchone()
        return _row_to_event(row) if row else None

    def has_event_after(self, user_id: str, key: tuple[int, str, int]) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM events
            WHERE user_id=? AND (event_time_ms, device_id, seq) > (?, ?, ?)
            LIMIT 1
            """,
            (user_id, *key),
        ).fetchone()
        return row is not None

    def upsert_event(self, event: StoredEvent, received_at_ms: int) -> None:
        self._conn.execute(
            """
            INSERT INTO events (device_id, seq, user_id, revision, event_time_ms,
                                event_name, device_type, received_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (device_id, seq) DO UPDATE SET
                user_id=excluded.user_id,
                revision=excluded.revision,
                event_time_ms=excluded.event_time_ms,
                event_name=excluded.event_name,
                device_type=excluded.device_type,
                received_at_ms=excluded.received_at_ms
            """,
            (
                event.device_id,
                event.seq,
                event.user_id,
                event.revision,
                event.event_time_ms,
                event.event_name,
                event.device_type,
                received_at_ms,
            ),
        )

    def fetch_events_between(
        self,
        user_id: str,
        start_key: tuple[int, str, int],
        end_key: tuple[int, str, int],
    ) -> list[StoredEvent]:
        """按 (event_time, device_id, seq) 排序键取闭区间内的已生效事件。"""
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE user_id=?
              AND (event_time_ms, device_id, seq) >= (?, ?, ?)
              AND (event_time_ms, device_id, seq) <= (?, ?, ?)
            ORDER BY event_time_ms, device_id, seq
            """,
            (user_id, *start_key, *end_key),
        ).fetchall()
        return [_row_to_event(r) for r in rows]

    # ---- 会话 ----

    def find_sessions_overlapping(
        self, user_id: str, start_ms: int, end_ms: int
    ) -> list[Session]:
        return self._overlap_query(user_id, start_ms, end_ms)

    def _overlap_query(self, user_id: str, start_ms: int, end_ms: int) -> list[Session]:
        # 会话互不重叠且按开始时间有序：先定位最早一个 end>=start_ms 的会话，
        # 重叠会话必然从它开始连续排列，之后只需扫 start 落在局部区间的行。
        anchor = self._conn.execute(
            """
            SELECT start_time_ms FROM sessions
            WHERE user_id=? AND end_time_ms>=?
            ORDER BY end_time_ms LIMIT 1
            """,
            (user_id, start_ms),
        ).fetchone()
        if anchor is None:
            return []
        rows = self._conn.execute(
            """
            SELECT * FROM sessions
            WHERE user_id=? AND start_time_ms>=? AND start_time_ms<=?
            ORDER BY start_time_ms, first_device_id, first_seq
            """,
            (user_id, anchor["start_time_ms"], end_ms),
        ).fetchall()
        return [_row_to_session(r) for r in rows]

    def delete_sessions(self, sessions: list[Session]) -> None:
        self._conn.executemany(
            "DELETE FROM sessions WHERE user_id=? AND first_device_id=? AND first_seq=?",
            [(s.user_id, s.first_device_id, s.first_seq) for s in sessions],
        )

    def insert_session(self, session: Session) -> None:
        self._conn.execute(
            """
            INSERT INTO sessions (user_id, start_time_ms, end_time_ms, event_count,
                                  status, date, device_type,
                                  first_device_id, first_seq, last_device_id, last_seq)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session.user_id,
                session.start_time_ms,
                session.end_time_ms,
                session.event_count,
                session.status.value,
                session.date,
                session.device_type,
                session.first_device_id,
                session.first_seq,
                session.last_device_id,
                session.last_seq,
            ),
        )

    def get_last_session(self, user_id: str) -> Session | None:
        row = self._conn.execute(
            """
            SELECT * FROM sessions WHERE user_id=?
            ORDER BY start_time_ms DESC, first_device_id DESC, first_seq DESC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        return _row_to_session(row) if row else None

    def get_session_before(
        self, user_id: str, start_key: tuple[int, str, int]
    ) -> Session | None:
        row = self._conn.execute(
            """
            SELECT * FROM sessions
            WHERE user_id=?
              AND (start_time_ms, first_device_id, first_seq) < (?, ?, ?)
            ORDER BY start_time_ms DESC, first_device_id DESC, first_seq DESC
            LIMIT 1
            """,
            (user_id, *start_key),
        ).fetchone()
        return _row_to_session(row) if row else None

    def update_session_status(self, session: Session, status: SessionStatus) -> None:
        self._conn.execute(
            """
            UPDATE sessions SET status=?
            WHERE user_id=? AND first_device_id=? AND first_seq=?
            """,
            (status.value, session.user_id, session.first_device_id, session.first_seq),
        )

    def list_sessions(
        self, user_id: str, start_ms: int, end_ms: int
    ) -> list[Session]:
        return self._overlap_query(user_id, start_ms, end_ms)

    # ---- 时长分布 ----

    def add_duration(self, date: str, device_type: str, bucket: int, delta: int) -> None:
        self._conn.execute(
            """
            INSERT INTO duration_dist (date, device_type, bucket, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (date, device_type, bucket)
            DO UPDATE SET count = count + excluded.count
            """,
            (date, device_type, bucket, delta),
        )

    def query_duration(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        device_type: str | None = None,
    ) -> list[tuple[int, int]]:
        sql = "SELECT bucket, SUM(count) AS c FROM duration_dist"
        clauses: list[str] = []
        params: list[object] = []
        if start_date is not None:
            clauses.append("date >= ?")
            params.append(start_date)
        if end_date is not None:
            clauses.append("date <= ?")
            params.append(end_date)
        if device_type is not None:
            clauses.append("device_type = ?")
            params.append(device_type)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " GROUP BY bucket ORDER BY bucket"
        rows = self._conn.execute(sql, params).fetchall()
        return [(int(r["bucket"]), int(r["c"])) for r in rows if r["c"] != 0]
