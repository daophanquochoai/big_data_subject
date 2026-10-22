import time

import pandas as pd
import pytest

from pdm import spark_session
from pdm.schema import SENSORS
from pdm.windows import spark_windows, stream_windows, static_windows
from test_windows import life

INPUT_SCHEMA = "machine_id STRING, cycle INT, event_time TIMESTAMP, " + ", ".join(f"{s} DOUBLE" for s in SENSORS)


@pytest.fixture(scope="module")
def spark():
    return spark_session("tests")


def rows(df):
    return df.toPandas().sort_values(["machine_id", "age"]).reset_index(drop=True)


def test_spark_static_windows_match_pandas(spark):
    pdf = pd.concat([life("m1", range(1, 74), 0), life("m2", range(10, 61), 1)])
    got = rows(spark_windows(spark.createDataFrame(pdf, INPUT_SCHEMA)))
    want = pd.concat([static_windows(life("m1", range(1, 74), 0)), static_windows(life("m2", range(10, 61), 1))])
    assert len(got) == len(want) > 10
    assert got[["machine_id", "age", "n_readings"]].values.tolist() == \
        want.sort_values(["machine_id", "age"])[["machine_id", "age", "n_readings"]].values.tolist()
    assert got["s2_mean"].tolist() == pytest.approx(want.sort_values(["machine_id", "age"])["s2_mean"].tolist())


def test_streaming_state_matches_static_over_several_batches(spark, tmp_path):
    pdf = pd.concat([life("m1", range(1, 74), 0), life("m2", range(10, 61), 1)])
    src = tmp_path / "in"
    # Ba lô: mỗi lô là một file; trigger availableNow đọc từng file một.
    for i, part in enumerate([pdf[pdf.cycle <= 25], pdf[(pdf.cycle > 25) & (pdf.cycle <= 50)], pdf[pdf.cycle > 50]]):
        spark.createDataFrame(part, INPUT_SCHEMA).coalesce(1).write.json(str(src / f"batch{i}"))
    stream = spark.readStream.schema(INPUT_SCHEMA).option("maxFilesPerTrigger", 1).json(str(src / "*"))
    q = (stream_windows(stream).writeStream.outputMode("append").format("memory").queryName("w")
         .option("checkpointLocation", str(tmp_path / "ckpt")).trigger(processingTime="1 second").start())
    # Hết hạn theo thời gian xử lý khiến Spark luôn chạy thêm lô rỗng, nên availableNow và processAllAvailable
    # không tự dừng: chờ tới khi đã đọc hết đầu vào và có thêm một lô sau đó.
    deadline = time.time() + 180
    while time.time() < deadline:
        progress = q.recentProgress
        if sum(p["numInputRows"] for p in progress) == len(pdf) and progress[-1]["numInputRows"] == 0:
            break
        time.sleep(1)
    q.stop()
    got = rows(spark.table("w"))
    want = pd.concat([static_windows(life("m1", range(1, 74), 0)).query("age + 5 <= 73"),
                      static_windows(life("m2", range(10, 61), 1)).query("age + 5 <= 60")])
    want = want.sort_values(["machine_id", "age"]).reset_index(drop=True)
    assert got[["machine_id", "age", "n_readings"]].values.tolist() == want[["machine_id", "age", "n_readings"]].values.tolist()
    assert got["s11_std"].tolist() == pytest.approx(want["s11_std"].tolist())
