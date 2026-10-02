import unittest

from session_stitcher import SessionEngine, Event, SessionStatus
from session_stitcher.models import MAX_CLOCK_SKEW_MS

NOW = 1_700_000_000_000


def ev(seq, revision, t, name="read", user="u1", device="phone", dtype="phone"):
    return Event(user, device, seq, revision, t, name, dtype)


class TestSubmitRules(unittest.TestCase):
    def setUp(self):
        self.engine = SessionEngine(":memory:", clock=lambda: NOW)

    def tearDown(self):
        self.engine.close()

    def test_future_event_rejected(self):
        # 比服务端时间晚 5 分钟以上 -> 拒收
        stats = self.engine.submit_batch([ev(1, 1, NOW + MAX_CLOCK_SKEW_MS + 1)])
        self.assertEqual(stats["rejected"], 1)
        self.assertEqual(self.engine.list_sessions("u1", 0, 2**62), [])
        # 正好 5 分钟 -> 受理
        stats = self.engine.submit_batch([ev(2, 1, NOW + MAX_CLOCK_SKEW_MS)])
        self.assertEqual(stats["inserted"], 1)

    def test_rejection_keeps_existing_version(self):
        self.engine.submit_batch([ev(1, 1, NOW - 1000)])
        # 同一序号的未来事件被拒，不影响已生效版本
        stats = self.engine.submit_batch([ev(1, 2, NOW + MAX_CLOCK_SKEW_MS + 1)])
        self.assertEqual(stats["rejected"], 1)
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].start_time_ms, NOW - 1000)

    def test_duplicate_and_stale(self):
        self.engine.submit_batch([ev(1, 2, NOW - 1000)])
        stats = self.engine.submit_batch([ev(1, 2, NOW - 1000)])  # 完全重复
        self.assertEqual(stats["duplicates"], 1)
        stats = self.engine.submit_batch([ev(1, 1, NOW - 500)])   # 旧修订号
        self.assertEqual(stats["stale"], 1)
        stats = self.engine.submit_batch([ev(1, 2, NOW - 2000)])  # 同修订号更早时间
        self.assertEqual(stats["stale"], 1)
        # 同修订号更晚时间 -> 生效
        stats = self.engine.submit_batch([ev(1, 2, NOW - 100)])
        self.assertEqual(stats["updated"], 1)
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].start_time_ms, NOW - 100)

    def test_batch_validation(self):
        with self.assertRaises(ValueError):
            self.engine.submit_batch([ev(i, 1, NOW - 1000) for i in range(5001)])
        with self.assertRaises(ValueError):
            self.engine.submit_batch([ev(1, 1, NOW - 1000, device="a"),
                                      ev(1, 1, NOW - 1000, device="b")])
        self.assertEqual(self.engine.submit_batch([]),
                         {"inserted": 0, "updated": 0, "duplicates": 0,
                          "stale": 0, "rejected": 0})

    def test_exactly_30min_gap_stays_in_session(self):
        t0 = NOW - 3600_000
        self.engine.submit_batch([ev(1, 1, t0), ev(2, 1, t0 + 30 * 60_000)])
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 1)
        # 30 分钟 + 1 毫秒 -> 切开
        self.engine.submit_batch([ev(3, 1, t0 + 60 * 60_000 + 1)])
        sessions = self.engine.list_sessions("u1", 0, 2**62)
        self.assertEqual(len(sessions), 2)


if __name__ == "__main__":
    unittest.main()
