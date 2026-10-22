"""Chia Máy train thành ba nhóm và tính bảng thống kê chuẩn hóa. Chạy một lần."""
import json
import os
import random

from pyspark.sql import functions as F

from pdm import spark_session
from pdm.features import condition_stats, normalize, with_condition
from pdm.schema import NORM_DIR, SENSORS, read_train

SEED = 42


def split(machine_ids):
    """70% huấn luyện gốc, 10% kiểm định, 20% phát lại; chia theo Máy."""
    ids = sorted(machine_ids)
    random.Random(SEED).shuffle(ids)
    a = round(0.7 * len(ids))
    b = a + round(0.1 * len(ids))
    return {"train": sorted(ids[:a]), "validation": sorted(ids[a:b]), "replay": sorted(ids[b:])}


if __name__ == "__main__":
    spark = spark_session("prepare-data")
    readings = read_train(spark).cache()

    ids = [r.machine_id for r in readings.select("machine_id").distinct().collect()]
    groups = split(ids)
    members = [m for g in groups.values() for m in g]
    assert len(members) == len(set(members)) == len(ids) == 709, "ba nhóm phải rời nhau và đủ 709 Máy"

    train = readings.where(F.col("machine_id").isin(groups["train"]))
    stats = condition_stats(train).orderBy("condition").cache()
    rows = [r.asDict() for r in stats.collect()]
    assert len(rows) == 6, f"cần đúng 6 Điều kiện vận hành, có {len(rows)}"
    # normalize() loại Bản tin có điều kiện lạ, nên hai nhóm còn lại không được có điều kiện nào ngoài bảng.
    assert with_condition(readings).select("condition").distinct().count() == 6, "nhóm khác có điều kiện ngoài bảng"
    assert all(r[f"{s}_sigma"] and r[f"{s}_sigma"] > 0 for r in rows for s in SENSORS), "độ lệch chuẩn bằng 0"

    # Sau chuẩn hóa, trung bình mỗi cảm biến trong mỗi điều kiện phải xấp xỉ 0.
    means = normalize(train, stats).groupBy("condition").agg(*[F.avg(s).alias(s) for s in SENSORS]).collect()
    worst = max(abs(r[s]) for r in means for s in SENSORS)
    assert worst < 1e-6, f"chuẩn hóa lệch: {worst}"

    os.makedirs(NORM_DIR, exist_ok=True)
    with open(f"{NORM_DIR}/groups.json", "w") as f:
        json.dump(groups, f, indent=1)
    with open(f"{NORM_DIR}/stats.json", "w") as f:
        json.dump(rows, f, indent=1)

    print({g: len(m) for g, m in groups.items()})
    print(f"Bảng thống kê: {len(rows)} điều kiện x {len(SENSORS)} cảm biến -> {NORM_DIR}")
