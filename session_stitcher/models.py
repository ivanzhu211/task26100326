"""数据类型定义：事件、会话、时长分布。"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


SESSION_GAP_MS = 30 * 60 * 1000
FUTURE_TOLERANCE_MS = 5 * 60 * 1000
MAX_BATCH_SIZE = 5000
SESSION_END_EVENT = "session_end"

BUCKET_WIDTH_MS = 30 * 1000
BUCKET_CAP_MS = 2 * 60 * 60 * 1000
BUCKET_COUNT = BUCKET_CAP_MS // BUCKET_WIDTH_MS + 1


class SessionStatus(str, Enum):
    ACTIVE_END = "active_end"
    BACKFILLED = "backfilled"
    ONGOING = "ongoing"


@dataclass(frozen=True)
class EventInput:
    """上层采集服务提交的一条事件。"""

    user_id: str
    device_id: str
    seq: int
    revision: int
    event_time_ms: int
    event_name: str
    device_type: str


@dataclass(frozen=True)
class StoredEvent:
    """已生效、持久化的事件版本。"""

    user_id: str
    device_id: str
    seq: int
    revision: int
    event_time_ms: int
    event_name: str
    device_type: str

    @property
    def sort_key(self) -> tuple[int, str, int]:
        return (self.event_time_ms, self.device_id, self.seq)


@dataclass(frozen=True)
class Session:
    """一个会话。归属日期与设备类型取第一条事件的 UTC 日期和设备类型。"""

    user_id: str
    start_time_ms: int
    end_time_ms: int
    event_count: int
    status: SessionStatus
    date: str
    device_type: str
    first_device_id: str
    first_seq: int
    last_device_id: str
    last_seq: int

    @property
    def duration_ms(self) -> int:
        return self.end_time_ms - self.start_time_ms


@dataclass(frozen=True)
class DurationBucket:
    """时长分布的一档。"""

    bucket_index: int
    count: int


def duration_bucket_index(duration_ms: int) -> int:
    idx = duration_ms // BUCKET_WIDTH_MS
    if idx >= BUCKET_COUNT:
        idx = BUCKET_COUNT - 1
    return int(idx)
