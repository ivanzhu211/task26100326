"""验收测试 2：改时间的修订把事件挪走，会话拆开，补记结束与时长分布随之更新。"""
import unittest

from session_stitcher import SessionEngine, Event, SessionStatus
from session_stitcher.models import duration_bucket

NOW = 1_700_000_000_000
T0 = NOW - 10 * 24 * 3600 * 1000
MIN = 60_000


def ev(seq, revision, t, name="read"):
    return Event("u1", "phone", seq, revision, T0 + t, name, "phone")


class TestRevisionSplit(unittest.TestCase):
    def setUp(self):
        self.engine = SessionEngine(":memory:", clock=lambda: NOW)

    def tearDown(self):
        self.engine.close()

    def test_revision_moves_event_and_splits_session(self):
        # 5 条事件，间隔 20 分钟：一个会话
        self.engine.submit_batch([ev(i, 1, i * 20 * MIN) for i in range(5)])
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].status, SessionStatus.ACTIVE)
        self.assertEqual(sessions[0].event_count, 5)
        # 进行中的会话不计入时长分布
        self.assertEqual(sum(self.engine.duration_distribution(
            "u1", "2000-01-01", "2100-01-01")), 0)

        # 修订：把 seq=2（t=40min）挪到 5 小时后
        self.engine.submit_batch([ev(2, 2, 300 * MIN)])

        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 3)

        left, middle, right = sessions
        # 左段：[0, 20min]，被补记结束
        self.assertEqual((left.event_count, left.status),
                         (2, SessionStatus.AUTO_CLOSED))
        self.assertEqual(left.end_time_ms - left.start_time_ms, 20 * MIN)
        # 中段：[60min, 80min]，被补记结束
        self.assertEqual((middle.event_count, middle.status),
                         (2, SessionStatus.AUTO_CLOSED))
        self.assertEqual(middle.end_time_ms - middle.start_time_ms, 20 * MIN)
        # 挪走的事件自成最后一个会话，进行中
        self.assertEqual((right.event_count, right.status),
                         (1, SessionStatus.ACTIVE))

        # 时长分布：两个 20 分钟的补记会话
        dist = self.engine.duration_distribution("u1", "2000-01-01", "2100-01-01")
        self.assertEqual(sum(dist), 2)
        self.assertEqual(dist[duration_bucket(20 * MIN)], 2)

        # 修订再改回去：应恢复成一个 5 事件的进行中会话
        self.engine.submit_batch([ev(2, 3, 40 * MIN)])
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].status, SessionStatus.ACTIVE)
        self.assertEqual(sessions[0].event_count, 5)
        self.assertEqual(sum(self.engine.duration_distribution(
            "u1", "2000-01-01", "2100-01-01")), 0)

    def test_revision_changes_session_end_position(self):
        # 会话以 session_end 收尾
        self.engine.submit_batch([
            ev(0, 1, 0), ev(1, 1, 10 * MIN), ev(2, 1, 20 * MIN, "session_end"),
        ])
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].status, SessionStatus.ENDED)
        dist = self.engine.duration_distribution("u1", "2000-01-01", "2100-01-01")
        self.assertEqual(dist[duration_bucket(20 * MIN)], 1)

        # 修订：session_end 挪到 2 小时后 -> 原会话变补记结束，session_end 自成会话
        self.engine.submit_batch([ev(2, 2, 120 * MIN, "session_end")])
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0].status, SessionStatus.AUTO_CLOSED)
        self.assertEqual(sessions[1].status, SessionStatus.ENDED)
        dist = self.engine.duration_distribution("u1", "2000-01-01", "2100-01-01")
        self.assertEqual(dist[duration_bucket(10 * MIN)], 1)  # 左段 10 分钟
        self.assertEqual(dist[duration_bucket(0)], 1)          # 单事件会话时长 0
        self.assertEqual(sum(dist), 2)


if __name__ == "__main__":
    unittest.main()
