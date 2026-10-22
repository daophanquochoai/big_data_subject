import numpy as np
import pytest

from engine.config import Config
from engine.data import COLUMNS, INFORMATIVE, N_SETTINGS
from engine.fleet import Fleet
from engine.generators import generate

NOW = "2026-10-15T09:30:12.400Z"
S4, S11 = COLUMNS.index("s4"), COLUMNS.index("s11")


ALL = ["gauss", "warp", "blend", "parametric"]


def config(**kw):
    return Config(**kw)


def trend(ds, data, col):
    """Trung bình z ở 20% cuối trừ 20% đầu vòng đời."""
    z = ds.normalize(data)[:, col]
    n = max(1, len(z) // 5)
    return z[-n:].mean() - z[:n].mean()


def test_normalize_then_denormalize_returns_the_raw_readings(ds):
    src = ds.sources["FD002-001"]
    assert np.allclose(ds.denormalize(ds.normalize(src)), src, rtol=0, atol=1e-9)


@pytest.mark.parametrize("method", ALL)
def test_every_generated_cycle_uses_a_real_settings_row_of_its_source(ds, method):
    rng = np.random.default_rng(0)
    for _ in range(20):
        life = generate(method, ds, rng, config())
        real = {tuple(r) for b in life.base for r in ds.sources[b][:, :N_SETTINGS]}
        assert all(tuple(r) in real for r in life.data[:, :N_SETTINGS])


def test_warp_length_stays_within_the_factor_range(ds):
    rng = np.random.default_rng(1)
    cfg = config(warp_range=(0.7, 1.5))
    for _ in range(50):
        life = generate("warp", ds, rng, cfg)
        length = len(ds.sources[life.base[0]])
        assert round(length * 0.7) <= len(life.data) <= round(length * 1.5)
        assert life.params["factor"] == pytest.approx(len(life.data) / length, abs=1 / length + 1e-9)


@pytest.mark.parametrize("method", ["gauss", "warp", "blend"])
def test_degradation_trend_keeps_the_direction_of_the_source(ds, method):
    rng = np.random.default_rng(2)
    for _ in range(20):
        life = generate(method, ds, rng, config())
        src = ds.sources[life.base[0]]
        for col in (S4, S11):
            assert np.sign(trend(ds, life.data, col)) == np.sign(trend(ds, src, col))


def test_blend_length_lies_between_its_two_sources(ds):
    rng = np.random.default_rng(10)
    for _ in range(50):
        life = generate("blend", ds, rng, config())
        a, b = (len(ds.sources[i]) for i in life.base)
        assert len(set(life.base)) == 2 and life.base[0][:5] == life.base[1][:5]
        assert min(a, b) <= len(life.data) <= max(a, b)
        assert 0 <= life.params["weight"] <= 1


def test_parametric_degrades_like_most_machines_of_its_subset(ds):
    rng = np.random.default_rng(12)
    for subset in ("FD001", "FD002"):
        cfg = config(subsets=(subset,))
        for col in (S4, S11):
            usual = np.sign(np.median([trend(ds, ds.sources[i], col) for i in ds.by_subset[subset]]))
            signs = [np.sign(trend(ds, generate("parametric", ds, rng, cfg).data, col)) for _ in range(30)]
            assert np.mean(np.array(signs) == usual) >= 0.9
    lengths = [len(generate("parametric", ds, rng, config()).data) for _ in range(100)]
    assert min(lengths) >= 30 and np.std(lengths) > 10


@pytest.mark.parametrize("method", ["warp", "blend", "parametric"])
def test_generated_values_stay_in_the_range_of_real_data(ds, method):
    rng = np.random.default_rng(13)
    real = np.vstack([ds.normalize(v)[:, INFORMATIVE] for v in ds.sources.values()])
    z = np.vstack([ds.normalize(generate(method, ds, rng, config()).data)[:, INFORMATIVE] for _ in range(200)])
    assert np.abs(z).max() <= 1.5 * np.abs(real).max()
    assert np.percentile(z, 1) >= np.percentile(real, 1) - 1 and np.percentile(z, 99) <= np.percentile(real, 99) + 1


def test_gauss_keeps_the_source_lifecycle(ds):
    life = generate("gauss", ds, np.random.default_rng(3), config())
    assert np.array_equal(life.data, ds.sources[life.base[0]])
    assert life.params == {}


def run(fleet, cfg, ticks):
    messages, failures = [], []
    for _ in range(ticks):
        m, f = fleet.tick(cfg, NOW)
        messages += m
        failures += f
    return messages, failures


def test_speed_2_5_emits_25_messages_per_machine_in_100_ticks(ds):
    cfg = config(machines=20, speed=2.5)
    fleet = Fleet(ds, cfg, np.random.default_rng(4), first_id=1)
    first = {m.id for m in fleet.machines}
    messages, _ = run(fleet, cfg, 100)  # 100 nhịp = 10 giây
    # Máy thay thế giữa chừng có bộ đếm riêng, nên chỉ xét máy sống trọn 100 nhịp.
    survivors = first & {m.id for m in fleet.machines}
    counts = {i: sum(m["machine_id"] == i for m in messages) for i in survivors}
    assert survivors and set(counts.values()) == {25}


def test_speed_1_emits_a_tenth_of_the_fleet_per_tick_on_average(ds):
    cfg = config(machines=50, speed=1.0)
    messages, _ = run(Fleet(ds, cfg, np.random.default_rng(5), first_id=1), cfg, 100)
    assert len(messages) == pytest.approx(50 * 0.1 * 100, rel=0.02)


def test_fleet_size_is_constant_and_replacements_start_fresh(ds):
    cfg = config(machines=30, speed=10)
    fleet = Fleet(ds, cfg, np.random.default_rng(6), first_id=1)
    first = {m.id for m in fleet.machines}
    seen, last_cycle, lengths = set(), {}, {}
    failures = []
    for _ in range(600):
        messages, f = fleet.tick(cfg, NOW)
        failures += f
        assert len(fleet.machines) == 30
        for m in messages:
            i = m["machine_id"]
            if i in last_cycle:
                assert m["cycle"] == last_cycle[i] + 1
            elif i not in first:
                assert m["cycle"] == 1
            last_cycle[i] = m["cycle"]
            lengths[i] = m["cycle"] + m["truth"]["rul"]
            assert m["truth"]["rul"] == lengths[i] - m["cycle"]
    assert failures
    for f in failures:
        assert f["last_cycle"] == lengths[f["machine_id"]] == last_cycle[f["machine_id"]]
    assert len({f["machine_id"] for f in failures}) == len(failures)
    assert len(last_cycle) > 30


def test_messages_follow_the_schema(ds):
    cfg = config(machines=5, speed=10)
    m = Fleet(ds, cfg, np.random.default_rng(7), first_id=123).tick(cfg, NOW)[0][0]
    assert list(m) == ["machine_id", "cycle", "event_time", *COLUMNS, "truth"]
    assert m["machine_id"] == "vm-000123" and m["event_time"] == NOW
    assert set(m["truth"]) == {"rul", "method", "fault"} and m["truth"]["fault"] is None


def test_noise_changes_informative_sensors_only(ds):
    quiet, noisy = config(machines=5, speed=10, noise=0), config(machines=5, speed=10, noise=0.5)
    a = Fleet(ds, quiet, np.random.default_rng(8), first_id=1).tick(quiet, NOW)[0]
    b = Fleet(ds, noisy, np.random.default_rng(8), first_id=1).tick(noisy, NOW)[0]
    informative = {COLUMNS[i] for i in INFORMATIVE}
    for x, y in zip(a, b):
        assert all((x[c] != y[c]) == (c in informative) for c in COLUMNS)


def test_shrinking_the_fleet_drops_the_newest_machines(ds):
    cfg = config(machines=10, speed=1)
    fleet = Fleet(ds, cfg, np.random.default_rng(9), first_id=1)
    _, failures = fleet.tick(config(machines=4, speed=1), NOW)
    assert [m.id for m in fleet.machines] == [f"vm-{i:06d}" for i in range(1, 5)]
    assert all(f["machine_id"] not in {f"vm-{i:06d}" for i in range(5, 11)} for f in failures)


def strip(messages):
    return [{k: v for k, v in m.items() if k not in ("machine_id", "event_time")} for m in messages]


def test_same_seed_and_config_reproduce_the_stream(ds):
    cfg = config(machines=40, speed=3, seed=11)
    a = run(Fleet(ds, cfg, np.random.default_rng(cfg.seed), first_id=1), cfg, 50)[0]
    b = run(Fleet(ds, cfg, np.random.default_rng(cfg.seed), first_id=900), cfg, 50)[0]
    assert strip(a) == strip(b)


def test_low_speed_spreads_messages_over_the_ticks_instead_of_bursting(ds):
    cfg = config(machines=1000, speed=1)
    fleet = Fleet(ds, cfg, np.random.default_rng(14), first_id=1)
    per_tick = [len(fleet.tick(cfg, NOW)[0]) for _ in range(50)]
    assert max(per_tick) <= 0.2 * 1000


def test_restricting_sources_to_a_group_keeps_every_machine_inside_it():
    from tests.conftest import DATA_DIR
    from engine.data import load_dataset
    group = {f"{s}-{i:03d}" for s in ("FD001", "FD002", "FD003", "FD004") for i in range(1, 40, 3)}
    small = load_dataset(DATA_DIR, machines=group)
    assert set(small.sources) == group
    rng = np.random.default_rng(15)
    for method in ALL:
        for _ in range(10):
            assert set(generate(method, small, rng, config()).base) <= group
