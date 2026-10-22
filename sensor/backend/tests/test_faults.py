import numpy as np
import pytest

from engine.config import Config, Faults
from engine.data import COLUMNS, N_SETTINGS
from engine.fleet import Fleet

SENSORS = COLUMNS[N_SETTINGS:]


def run(ds, faults, ticks, machines=1000, seed=0, late_max_s=5):
    cfg = Config(machines=machines, speed=10, faults=Faults(**faults), late_max_s=late_max_s)
    fleet = Fleet(ds, cfg, np.random.default_rng(seed), first_id=1)
    out = []  # (nhịp phát ra, Bản tin)
    for t in range(ticks):
        out += [(t, m) for m in fleet.tick(cfg, f"t{t:05d}")[0]]
    return fleet, out


def test_each_fault_rate_matches_its_setting(ds):
    rate = 0.05
    fleet, out = run(ds, dict.fromkeys(("bad_value", "drop", "late", "duplicate"), rate), ticks=100)
    originals = 1000 * 100  # k = 10: mỗi máy một Bản tin mỗi nhịp
    seen = {f: sum(m["truth"]["fault"] == f for _, m in out) for f in ("bad_value", "late", "duplicate")}
    seen["drop"] = fleet.fault_counts["drop"]
    # Bản tin trễ phát trong 50 nhịp cuối có thể chưa kịp ra; đếm theo bộ đếm của bộ chèn lỗi.
    seen["late"] = fleet.fault_counts["late"]
    for fault, n in seen.items():
        assert n / originals == pytest.approx(rate, rel=0.1), fault
    assert {k: fleet.fault_counts[k] for k in ("bad_value", "duplicate")} == \
           {k: seen[k] for k in ("bad_value", "duplicate")}


def test_late_messages_keep_their_event_time_and_arrive_at_least_a_second_later(ds):
    _, out = run(ds, {"late": 0.2}, ticks=120, machines=50, late_max_s=3)
    late = [(t, m) for t, m in out if m["truth"]["fault"] == "late"]
    assert late
    for t, m in late:
        emitted_at = int(m["event_time"][1:])
        assert 10 <= t - emitted_at <= 30


def test_duplicate_is_an_identical_copy_sent_in_the_same_tick(ds):
    _, out = run(ds, {"duplicate": 0.1}, ticks=20, machines=50)
    assert any(m["truth"]["fault"] == "duplicate" for _, m in out)
    for i, (t, m) in enumerate(out):
        if m["truth"]["fault"] == "duplicate":
            t0, original = out[i - 1]
            assert t0 == t and original["truth"]["fault"] is None
            assert {**original, "truth": None} == {**m, "truth": None}


def test_bad_value_breaks_exactly_one_sensor(ds):
    real = np.vstack(list(ds.sources.values()))
    lo, hi = real.min(0), real.max(0)
    _, out = run(ds, {"bad_value": 0.2}, ticks=20, machines=50)
    bad = [m for _, m in out if m["truth"]["fault"] == "bad_value"]
    assert bad
    for m in bad:
        broken = [c for c in SENSORS
                  if m[c] == -9999 or not 0.5 * lo[COLUMNS.index(c)] <= m[c] <= 2 * hi[COLUMNS.index(c)]]
        assert len(broken) == 1


def test_no_faults_by_default(ds):
    fleet, out = run(ds, {}, ticks=20, machines=50)
    assert all(m["truth"]["fault"] is None for _, m in out)
    assert len(out) == 50 * 20 and sum(fleet.fault_counts.values()) == 0
