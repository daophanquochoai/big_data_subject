import pytest
from pyspark.sql import functions as F

from pdm import spark_session
from pdm.features import normalize
from pdm.gate import accepts
from pdm.labels import with_labels
from pdm.schema import MESSAGE_SCHEMA, SENSORS


@pytest.fixture(scope="session")
def spark():
    return spark_session("tests")


GROUND = (0.0, 0.0, 100.0)      # Điều kiện vận hành "0_0_100"
CRUISE = (20.0, 0.70, 100.0)    # Điều kiện vận hành "20_70_100"


def readings(spark, rows):
    """rows: (machine_id, cycle, (setting1, setting2, setting3), s2). Cảm biến khác bằng 0."""
    others = [s for s in SENSORS if s != "s2"]
    data = [(m, c, float(c), *st, float(s2), *[0.0] * len(others)) for m, c, st, s2 in rows]
    cols = ["machine_id", "cycle", "ts", "setting1", "setting2", "setting3", "s2", *others]
    df = spark.createDataFrame(data, cols)
    return df.withColumn("event_time", F.timestamp_seconds("ts")).drop("ts")


@pytest.fixture(scope="session")
def stats(spark):
    """s2: trung bình 10, độ lệch 2 ở GROUND; trung bình 100, độ lệch 5 ở CRUISE."""
    others = [s for s in SENSORS if s != "s2"]
    cols = ["condition", "s2_mu", "s2_sigma"]
    cols += [f"{s}_{k}" for s in others for k in ("mu", "sigma")]
    rows = [("0_0_100", 10.0, 2.0, *[0.0, 1.0] * len(others)),
            ("20_70_100", 100.0, 5.0, *[0.0, 1.0] * len(others))]
    return spark.createDataFrame(rows, cols)


def test_same_raw_value_normalizes_differently_per_operating_condition(spark, stats):
    df = readings(spark, [("m1", 1, GROUND, 110.0), ("m1", 2, CRUISE, 110.0)])
    s2 = {r.cycle: r.s2 for r in normalize(df, stats).collect()}

    assert s2[1] == pytest.approx(50.0)  # (110 - 10) / 2
    assert s2[2] == pytest.approx(2.0)   # (110 - 100) / 5


def test_settings_just_below_zero_belong_to_the_ground_condition(spark, stats):
    df = readings(spark, [("m1", 1, (-0.0007, -0.0004, 100.0), 110.0)])

    assert [r.s2 for r in normalize(df, stats).collect()] == [pytest.approx(50.0)]


@pytest.fixture(scope="session")
def rul_by_cycle(spark):
    """Một Máy sống 200 Chu kỳ; mỗi tuổi một cửa sổ."""
    windows = spark.createDataFrame([("m1", a) for a in range(1, 201)], ["machine_id", "age"])
    last = spark.createDataFrame([("m1", 200)], ["machine_id", "last_cycle"])
    return {r.age: r for r in with_labels(windows, last).collect()}


def test_last_reading_has_zero_rul(rul_by_cycle):
    assert rul_by_cycle[200].rul == 0


def test_rul_is_capped_at_125(rul_by_cycle):
    assert rul_by_cycle[1].rul == 125    # RUL thật 199
    assert rul_by_cycle[75].rul == 125   # RUL thật 125
    assert rul_by_cycle[76].rul == 124


def test_failing_label_flips_at_60_cycles_left(rul_by_cycle):
    assert rul_by_cycle[140].failing == 1  # RUL thật 60
    assert rul_by_cycle[139].failing == 0  # RUL thật 61


def test_sixteen_informative_sensors():
    assert len(SENSORS) == 16
    assert not {"s1", "s5", "s16", "s18", "s19"} & set(SENSORS)


CURRENT = {"rul_rmse": 18.0, "failing_f1": 0.86}


def test_gate_accepts_lower_rmse_with_f1_not_lower():
    assert accepts({"rul_rmse": 17.9, "failing_f1": 0.86}, CURRENT)


def test_gate_rejects_candidate_that_is_only_equal():
    assert not accepts(dict(CURRENT), CURRENT)


def test_gate_rejects_lower_rmse_when_f1_drops():
    assert not accepts({"rul_rmse": 15.0, "failing_f1": 0.85}, CURRENT)


def test_parsing_drops_the_truth_field_and_broken_messages(spark):
    from pdm.cleaning import parse
    good = ('{"machine_id": "vm-000001", "cycle": 3, "event_time": "2026-10-15T09:30:12.400Z", "s2": 642.1,'
            ' "truth": {"rul": 120, "method": "warp", "fault": null}}')
    out = parse(spark.createDataFrame([(good,), ("{not json",), ('{"machine_id": "vm-2"}',)], ["value"]))
    assert "truth" not in out.columns and not any("rul" in c for c in out.columns)
    assert [(r.machine_id, r.cycle, r.s2) for r in out.collect()] == [("vm-000001", 3, 642.1)]


def test_failed_set_takes_the_latest_failures_and_labels_from_the_failure_event(spark, stats):
    from datetime import datetime
    from pdm.training import failed_windows
    rows = [(m, c, GROUND, 10.0 + c) for m in ("vm-1", "vm-2", "vm-3") for c in range(1, 39)]  # kho thiếu Chu kỳ 39, 40
    failures = spark.createDataFrame(
        [("vm-1", datetime(2026, 10, 15, 9, 0), 40), ("vm-2", datetime(2026, 10, 15, 9, 5), 40),
         ("vm-3", datetime(2026, 10, 15, 9, 9), 40)],
        "machine_id STRING, failed_at TIMESTAMP, last_cycle INT")
    out = failed_windows(readings(spark, rows), failures, stats, max_failed=2).collect()
    assert {r.machine_id for r in out} == {"vm-2", "vm-3"}
    by_age = {r.age: r for r in out if r.machine_id == "vm-2"}
    assert by_age[30].rul == 10 and by_age[30].failing == 1.0   # 40 − 30, không phải 38 − 30
    assert by_age[20].rul == 20


@pytest.fixture(scope="session")
def ranges(spark):
    from pdm.cleaning import valid_ranges
    from pdm.schema import read_train
    return valid_ranges(read_train(spark))


def test_out_of_range_values_become_missing_and_in_range_values_stay(spark, ranges):
    from pdm.cleaning import mask_out_of_range
    from pdm.schema import ALL_SENSORS, read_train
    real = read_train(spark).limit(400).toPandas()
    rows = []
    for i, c in enumerate(ALL_SENSORS):  # mỗi cột hỏng theo cả hai kiểu của bộ giả lập
        for broken in (-9999.0, real.at[i, c] * 10):
            r = real.iloc[i].to_dict()
            r[c] = broken
            rows.append((c, r))
    import pandas as pd
    df = spark.createDataFrame(pd.concat([pd.DataFrame([r for _, r in rows]), real.iloc[100:400]], ignore_index=True))
    out = mask_out_of_range(df, ranges).toPandas()
    for k, (c, _) in enumerate(rows):
        assert out[c].isna().iloc[k], f"{c} không bị bắt"
        assert out.iloc[k][ALL_SENSORS].isna().sum() == 1  # chỉ cột hỏng thành rỗng, các cột khác của Bản tin giữ
    assert not out.iloc[len(rows):][ALL_SENSORS].isna().any().any(), "số đo thật bị lọc nhầm"
