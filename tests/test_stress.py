"""随机压力测试：更多事件、更激进的修订（跨会话搬移），与参照实现比对。"""
import random
import unittest

from session_stitcher import SessionEngine, Event

from tests.reference import effective_events, reference_dist, reference_sessions
from tests.test_consistency import canonical_sessions, shuffled_batches

NOW = 1_700_000_000_000
BASE = NOW - 90 * 24 * 3600 * 1000
MIN = 60_000


def build_large_scenario(seed: int, n_events: int = 800) -> list[Event]:
    rng = random.Random(seed)
    arrivals: list[Event] = []
    devices = [("dev-a", "phone"), ("dev-b", "tablet"), ("dev-c", "phone")]
    seqs = {d: 0 for d, _ in devices}
    t = BASE
    for _ in range(n_events):
        device, dtype = rng.choice(devices)
        r = rng.random()
        if r < 0.75:
            t += rng.randint(1, 15) * MIN
        else:
            t += rng.randint(31, 300) * MIN
        seqs[device] += 1
        name = "session_end" if rng.random() < 0.1 else "read"
        ev = Event("u1", device, seqs[device], 1, t, name, dtype)
        arrivals.append(ev)
        roll = rng.random()
        if roll < 0.1:
            arrivals.append(ev)  # 重发
        elif roll < 0.25:
            # 修订：大幅改时间（可能跨会话、跨天）
            revised = Event("u1", device, seqs[device], 2,
                            t + rng.randint(-400, 400) * MIN, name, dtype)
            arrivals.append(revised)
            if rng.random() < 0.4:
                # 再修订一次
                arrivals.append(Event("u1", device, seqs[device], 3,
                                      t + rng.randint(-400, 400) * MIN, name, dtype))
    return arrivals


class TestStress(unittest.TestCase):
    def test_large_scenario_consistency(self):
        for scenario_seed in (11, 22, 33):
            arrivals = build_large_scenario(scenario_seed)
            ref = effective_events(arrivals, NOW)["u1"]
            want_sessions = sorted(reference_sessions(ref))
            want_dist = reference_dist(want_sessions, "2000-01-01", "2100-01-01")
            want_dist_tablet = reference_dist(
                want_sessions, "2000-01-01", "2100-01-01", "tablet")

            for order_seed in (1, 2, 3):
                engine = SessionEngine(":memory:", clock=lambda: NOW)
                for batch in shuffled_batches(arrivals, order_seed):
                    engine.submit_batch(batch)
                self.assertEqual(canonical_sessions(engine, "u1"), want_sessions,
                                 f"scenario={scenario_seed} order={order_seed}")
                self.assertEqual(
                    engine.duration_distribution("u1", "2000-01-01", "2100-01-01"),
                    want_dist)
                self.assertEqual(
                    engine.duration_distribution("u1", "2000-01-01", "2100-01-01", "tablet"),
                    want_dist_tablet)
                engine.close()


if __name__ == "__main__":
    unittest.main()
