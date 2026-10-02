# session_stitcher

移动端行为事件的会话拼接库。上层采集服务直接以库的方式调用，无接口层；
数据持久化在本地 SQLite 文件中。

## 特性

- **乱序/分批/重发无关**：事件无论按什么顺序、分几批、重发几次到达，
  会话列表与时长分布都和把全部已生效事件从头切一遍的结果一致。
- **修订生效规则**：同一 `(device_id, seq)` 多次到达时，修订号大的生效；
  修订号相同取事件时间晚的；两项都相同按重复处理。
- **未来事件拒收**：事件时间比服务端接收时间晚 5 分钟以上的拒收，
  不影响该序号已生效的版本。
- **增量维护**：补交落在历史中段时，耗时只与该批事件及其附近会话有关，
  不随用户历史变长而变慢；时长分布基于预聚合表即时返回。

## 会话规则

- 会话按用户划分、跨设备合并；事件按 `(事件时间, 设备, 序号)` 排序。
- 相邻事件间隔超过 30 分钟（正好 30 分钟不算），或前一条是 `session_end`，
  则后一条开始新会话；`session_end` 算在它结束的会话里。
- 结束状态：`ended`（主动结束，以 `session_end` 收尾）、
  `auto_closed`（补记结束，无结束事件但后面已有下一会话，结束时间取最后一条事件时间）、
  `active`（进行中，用户最后一个会话且无结束事件）。
- 会话时长 = 末事件时间 - 首事件时间；会话按首事件的 UTC 日期和设备类型归类。
- 时长分布只统计 `ended` / `auto_closed` 会话，30 秒一档，>=2 小时归入最后一档（共 240 档）。

## 用法

```python
from session_stitcher import SessionEngine, Event

engine = SessionEngine("events.db")  # 本地 SQLite 文件

# 提交一批事件（同一设备，<=5000 条）
stats = engine.submit_batch([
    Event(user_id="u1", device_id="phone-1", seq=1, revision=1,
          event_time_ms=1_700_000_000_000, event_name="open", device_type="phone"),
    ...
])
# stats: {"inserted": n, "updated": n, "duplicates": n, "stale": n, "rejected": n}

# 查会话列表（返回与 [start_ms, end_ms] 相交的会话）
sessions = engine.list_sessions("u1", start_ms=0, end_ms=2**62)
# SessionInfo(start_time_ms, end_time_ms, event_count, status, date, device_type)

# 查时长分布（按 UTC 日期范围，可选设备类型过滤）
dist = engine.duration_distribution("u1", "2026-09-01", "2026-10-01",
                                    device_type=None)  # -> list[int]，240 档
```

测试注入时钟：`SessionEngine("events.db", clock=lambda: now_ms)`。

## 实现要点

- `events` 表以 `(device_id, seq)` 为主键，`(user_id, event_time_ms, device_id, seq)`
  索引支撑有序邻居查找；每条事件记录所属会话。
- `sessions` 表维护每个会话的首末事件排序键、事件数、状态、归属日期与设备类型。
- 插入/删除（修订 = 删旧 + 插新）只影响相邻会话：合并时保留事件多的一方会话 id，
  拆分时重挂事件少的一侧，代价与历史总量无关。
- `dist` 表按 `(user_id, date, device_type, bucket)` 预聚合计数，
  会话变化时先减旧值再加新值，查询即 `SUM ... GROUP BY bucket`。

## 测试

```bash
python3 -m unittest discover tests -v
```

覆盖：乱序/分批/重发/修订与从头切一遍的一致性（含随机压力）、
修订导致会话拆分后的补记结束与分布更新、20 万条历史用户的中段补交性能。
