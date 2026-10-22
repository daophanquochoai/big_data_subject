import pytest

from pdm import spark_session
from pdm.orders import SAFETY, new_orders


@pytest.fixture(scope="module")
def spark():
    return spark_session("tests")


def scored(spark, rows):
    return spark.createDataFrame(rows, "machine_id STRING, age INT, rul_pred DOUBLE, failing_prob DOUBLE, model_version STRING")


def test_one_order_at_the_first_window_over_the_threshold(spark):
    out = {r.machine_id: r for r in new_orders(scored(spark, [
        ("m1", 40, 80.0, 0.2, "v1"), ("m1", 50, 60.0, 0.9, "v1"), ("m1", 45, 70.0, 0.6, "v1"),
        ("m2", 40, 90.0, 0.49, "v1"),
    ])).collect()}
    assert set(out) == {"m1"}
    assert (out["m1"].cycle, out["m1"].rul_pred, out["m1"].failing_prob) == (45, 70.0, 0.6)
    assert out["m1"].suggested_cycle == 45 + 70 - SAFETY


def test_suggested_cycle_is_never_in_the_past(spark):
    out = new_orders(scored(spark, [("m3", 200, 5.0, 0.95, "v1"), ("m4", 200, -12.0, 0.99, "v1")])).collect()
    assert {r.machine_id: r.suggested_cycle for r in out} == {"m3": 200, "m4": 200}
