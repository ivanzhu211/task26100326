"""数据模型与常量定义。"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# 会话切分阈值：相邻事件间隔超过 30 分钟则切开（正好 30 分钟不切）
SESSION_GAP_MS = 30 * 60 * 1000
# 事件时间允许超过服务端接收时间的最大偏移：5 分钟
MAX_CLOCK_SKEW_MS = 5 * 60 * 1000
# 时长分布分桶：每 30 秒一档
BUCKET_MS = 30 * 1000
# 2 小时及以上归入最后一档
MAX_BUCKET_MS = 2 * 60 * 60 * 1000
BUCKET_COUNT = MAX_BUCKET_MS // BUCKET_MS  # 240 档，最后一档为 >=2h
# 单批最大事件数
MAX_BATCH_SIZE = 5000

SESSION_END_EVENT = "session_end"


class SessionStatus(str, Enum):
    ACTIVE = "active"            # 进行中（用户最后一个会话且无结束事件）
    ENDED = "ended"              # 主动结束（以 session_end 收尾）
    AUTO_CLOSED = "auto_closed"  # 补记结束（无结束事件但后面已有下一会话）


@dataclass(frozen=True)
class Event:
    user_id: str
    device_id: str
    seq: int                 # 设备内单调递增序号
    revision: int            # 修订号
    event_time_ms: int       # 事件时间（毫秒）
    event_name: str
    device_type: str


@dataclass(frozen=True)
class SessionInfo:
    session_id: int
    user_id: str
    start_time_ms: int
    end_time_ms: int
    event_count: int
    status: SessionStatus
    date: str                # 首条事件的 UTC 日期 YYYY-MM-DD
    device_type: str         # 首条事件的设备类型


def duration_bucket(duration_ms: int) -> int:
    """时长（毫秒）-> 桶号 [0, BUCKET_COUNT-1]，>=2h 归入最后一档。"""
    if duration_ms < 0:
        return 0
    idx = duration_ms // BUCKET_MS
    return min(idx, BUCKET_COUNT - 1)
