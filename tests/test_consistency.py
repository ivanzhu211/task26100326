"""验收测试 1：乱序/分批/重发/修订下，结果与从头切一遍完全一致。"""
import random
import unittest

from session_stitcher import SessionEngine, Event
from session_stitcher.models import BUCKET_COUNT

from tests.reference import effective_events, reference_dist, reference_sessions

NOW = 1_700_000_000_000
BASE = NOW - 30 * 24 * 3600 * 1000  # 30 天前开始
MIN = 60_000


def build_scenario_events() -> list[Event]:
    """构造一组多设备交错、含 session_end、重发、改时间修订的事件流（按到达顺序）。"""
    rng = random.Random(20261003)
    arrivals: list[Event] = []
    users = ["u1", "u2"]
    devices = {"u1": [("phone-a", "phone"), ("pad-a", "tablet")],
               "u2": [("phone-b", "phone")]}
    seqs = {d: 0 for devs in devices.values() for d, _ in devs}

    t = BASE
    for _ in range(60):  # 60 个“真实”事件
        user = rng.choice(users)
        device, dtype = rng.choice(devices[user])
        # 70% 间隔 1~10 分钟（同会话），否则 40~120 分钟（跨会话）
        t += rng.randint(1, 10) * MIN if rng.random() < 0.7 else rng.randint(40, 120) * MIN
        seqs[device] += 1
        name = "session_end" if rng.random() < 0.15 else rng.choice(
            ["open", "read", "scroll", "click"])
        ev = Event(user, device, seqs[device], 1, t, name, dtype)
        arrivals.append(ev)

        roll = rng.random()
        if roll < 0.15:
            # 原样重发（重复）
            arrivals.append(ev)
        elif roll < 0.30:
            # 时钟校正后的修订：修订号 +1，时间平移 -15~+15 分钟
            revised = Event(user, device, seqs[device], 2,
                            t + rng.randint(-15, 15) * MIN, name, dtype)
            arrivals.append(revised)
            if rng.random() < 0.3:
                # 修订再重发一次
                arrivals.append(revised)
        elif roll < 0.38:
            # 旧修订号重发（应判为 stale）
            arrivals.append(Event(user, device, seqs[device], 1,
                                  t + rng.randint(1, 5) * MIN, name, dtype))
    return arrivals


def shuffled_batches(arrivals: list[Event], seed: int) -> list[list[Event]]:
    """把到达序列按设备内乱序、随机分批（同批同设备、<=5000），设备间随机交错。"""
    rng = random.Random(seed)
    by_device: dict[str, list[Event]] = {}
    for ev in arrivals:
        by_device.setdefault(ev.device_id, []).append(ev)
    chunks: list[list[Event]] = []
    for device, evs in by_device.items():
        evs = evs[:]
        rng.shuffle(evs)
        i = 0
        while i < len(evs):
            n = rng.randint(1, 17)
            chunks.append(evs[i:i + n])
            i += n
    rng.shuffle(chunks)
    return chunks


def canonical_sessions(engine: SessionEngine, user: str) -> list[tuple]:
    return sorted(
        (s.start_time_ms, s.end_time_ms, s.event_count, s.status.value,
         s.date, s.device_type)
        for s in engine.list_sessions(user, 0, 2**62)
    )


class TestConsistency(unittest.TestCase):
    def test_shuffled_arrivals_match_reference(self):
        arrivals = build_scenario_events()
        ref_by_user = effective_events(arrivals, NOW)
        ref_sessions = {u: reference_sessions(evs) for u, evs in ref_by_user.items()}
        ref_dists = {u: reference_dist(ss, "2000-01-01", "2100-01-01")
                     for u, ss in ref_sessions.items()}

        for seed in range(6):
            engine = SessionEngine(":memory:", clock=lambda: NOW)
            for batch in shuffled_batches(arrivals, seed):
                engine.submit_batch(batch)
            for user in ref_sessions:
                got = canonical_sessions(engine, user)
                want = sorted(ref_sessions[user])
                self.assertEqual(got, want, f"seed={seed} user={user} sessions differ")
                got_dist = engine.duration_distribution(user, "2000-01-01", "2100-01-01")
                self.assertEqual(got_dist, ref_dists[user],
                                 f"seed={seed} user={user} dist differ")
                self.assertEqual(sum(got_dist), sum(ref_dists[user]))
            engine.close()


if __name__ == "__main__":
    unittest.main()
