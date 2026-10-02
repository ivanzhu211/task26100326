"""对上层采集服务暴露的库 API。"""
from __future__ import annotations

import time

from .models import MAX_BATCH_SIZE, DurationBucket, EventInput, Session
from .stitcher import Stitcher
from .storage import Storage


class BatchRejectedError(ValueError):
    pass


class SessionService:
    """会话拼接库入口。同一时刻只处理一次提交，调用方无需加锁。"""

    def __init__(self, db_path: str) -> None:
        self._storage = Storage(db_path)
        self._stitcher = Stitcher(self._storage)

    def close(self) -> None:
        self._storage.close()

    def submit_batch(
        self, events: list[EventInput], received_at_ms: int | None = None
    ) -> dict:
        """提交一批（<=5000 条、同一台设备）事件。"""
        if not events:
            return {"accepted": 0, "duplicates": 0, "rejected": 0}
        if len(events) > MAX_BATCH_SIZE:
            raise BatchRejectedError(
                f"batch size {len(events)} exceeds limit {MAX_BATCH_SIZE}"
            )
        device_ids = {e.device_id for e in events}
        if len(device_ids) != 1:
            raise BatchRejectedError("batch must come from a single device")
        if received_at_ms is None:
            received_at_ms = int(time.time() * 1000)
        self._storage.begin()
        try:
            stats = self._stitcher.apply_batch(events, received_at_ms)
        except Exception:
            self._storage.rollback()
            raise
        self._storage.commit()
        return stats

    def list_sessions(
        self, user_id: str, start_ms: int, end_ms: int
    ) -> list[Session]:
        """查某用户一段时间内的会话列表。"""
        return self._storage.list_sessions(user_id, start_ms, end_ms)

    def duration_distribution(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        device_type: str | None = None,
    ) -> list[DurationBucket]:
        """查时长分布，可按 UTC 日期范围和设备类型过滤。"""
        rows = self._storage.query_duration(start_date, end_date, device_type)
        return [DurationBucket(bucket_index=b, count=c) for b, c in rows]
