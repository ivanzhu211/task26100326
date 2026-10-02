"""验收测试 3：20 万条历史的用户补交中段小批，耗时与短历史用户同一量级。"""
import os
import tempfile
import time
import unittest

from session_stitcher import SessionEngine, Event

NOW = 1_700_000_000_000
MIN = 60_000


def make_history(user, device, n):
    """20 条一分钟间隔成会话、会话间隔 31 分钟的历史数据。"""
    evs = []
    t = NOW - 300 * 24 * 3600 * 1000  # 300 天前开始
    seq = 1
    while len(evs) < n:
        for _ in range(20):
            if len(evs) >= n:
                break
            evs.append(Event(user, device, seq, 1, t, "read", "phone"))
            seq += 1
            t += MIN
        t += 31 * MIN
    return evs


class TestPerformance(unittest.TestCase):
    def test_mid_history_batch_is_history_size_independent(self):
        tmpdir = tempfile.mkdtemp()
        big_db = os.path.join(tmpdir, "big.db")
        small_db = os.path.join(tmpdir, "small.db")
        clock = lambda: NOW + 10**15
        try:
            big = SessionEngine(big_db, clock=clock)
            big_evs = make_history("big", "phone", 200_000)
            for i in range(0, len(big_evs), 5000):
                big.submit_batch(big_evs[i:i + 5000])

            small = SessionEngine(small_db, clock=clock)
            small_evs = make_history("small", "phone", 500)
            small.submit_batch(small_evs)

            # 中段补交 50 条（另一台设备，落在历史中段）
            mid_t = big_evs[100_000].event_time_ms
            batch = [Event("big", "pad", i, 1, mid_t + i * 30_000, "read", "tablet")
                     for i in range(50)]
            t0 = time.perf_counter()
            big.submit_batch(batch)
            big_dt = time.perf_counter() - t0

            mid_t2 = small_evs[250].event_time_ms
            batch2 = [Event("small", "pad", i, 1, mid_t2 + i * 30_000, "read", "tablet")
                      for i in range(50)]
            t0 = time.perf_counter()
            small.submit_batch(batch2)
            small_dt = time.perf_counter() - t0

            print(f"\nbig(200k): {big_dt*1000:.1f}ms  small(500): {small_dt*1000:.1f}ms")
            # 同一量级：允许 10 倍余量，且绝对耗时不超过 1 秒
            self.assertLess(big_dt, max(small_dt * 10, 0.1))
            self.assertLess(big_dt, 1.0)

            # 时长分布查询也应是即时的
            t0 = time.perf_counter()
            dist = big.duration_distribution("big", "2000-01-01", "2100-01-01")
            dist_dt = time.perf_counter() - t0
            self.assertGreater(sum(dist), 0)
            self.assertLess(dist_dt, 0.1)

            big.close()
            small.close()
        finally:
            for f in os.listdir(tmpdir):
                os.remove(os.path.join(tmpdir, f))
            os.rmdir(tmpdir)


if __name__ == "__main__":
    unittest.main()
