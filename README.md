# session-stitcher

移动端行为事件的会话拼接库。由上层采集服务直接以库的形式调用，无接口层；
数据持久化在本地 SQLite 文件中。

## 能力

- 提交一批事件（<=5000 条、同一台设备）：乱序、重发、改时间的修订都支持，
  最终会话与时长分布和把全部已生效事件从头切一遍完全一致。
- 查某用户一段时间内的会话列表（起止时间、事件数、结束状态）。
- 查时长分布（30 秒一档，2 小时及以上归入末档），可按 UTC 日期范围与设备类型过滤。

## 用法

```python
from session_stitcher import EventInput, SessionService

svc = SessionService("events.db")

stats = svc.submit_batch([
    EventInput(
        user_id="u1", device_id="phoneA", seq=1, revision=1,
        event_time_ms=1_760_000_000_000, event_name="read", device_type="phone",
    ),
    # ... 同设备最多 5000 条
])  # -> {"accepted": ..., "duplicates": ..., "rejected": ...}

sessions = svc.list_sessions("u1", start_ms=0, end_ms=2**62)
# Session(start_time_ms, end_time_ms, event_count, status, date, device_type, ...)
# status: active_end 主动结束 / backfilled 补记结束 / ongoing 进行中

buckets = svc.duration_distribution(
    start_date="2026-09-01", end_date="2026-10-01", device_type="phone"
)
# [DurationBucket(bucket_index, count), ...] 只含已结束会话
```

## 规则实现要点

- 同一 `(device_id, seq)` 多次到达：修订号大的生效；修订号相同取事件时间晚的；
  都相同视为重复。事件时间超过接收时间 5 分钟的拒收，且不影响已生效版本。
- 会话按用户划分、跨设备合并；事件按 `(event_time, device_id, seq)` 排序，
  间隔超过 30 分钟（正好 30 分钟不算）或前一条是 `session_end` 时切分。
- 增量维护：每批只围绕变化事件的新旧时间开窗（前后各 30 分钟），
  删除窗口覆盖的会话并与其事件一起局部重切，同时增减时长分布计数，
  因此补交耗时只与批次及附近会话有关，与历史总量无关。
- 时长分布存于计数表，提交时同步增减，查询为一次分组求和，即时返回。

## 测试

```bash
python3 -m pytest tests/
```

覆盖：提交顺序/批次/重发不变性（对照独立的全量重切参照实现）、
修订挪走事件导致会话拆分与合并、20 万事件历史的中段补交性能。
