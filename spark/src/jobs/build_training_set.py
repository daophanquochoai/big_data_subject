"""Dựng tập huấn luyện lại: cả nhóm huấn luyện gốc cộng tối đa N Máy ảo đã hỏng gần nhất trong kho.

Dùng: build_training_set.py <thư mục ra> [--max-failed 500]
"""
import argparse
import json

from pyspark.sql import functions as F

from pdm import spark_session
from pdm.schema import LAKE_DIR, NORM_DIR
from pdm.training import failed_windows
from train import group_dataset

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("out")
    p.add_argument("--max-failed", type=int, default=500)
    args = p.parse_args()

    spark = spark_session("build-training-set")
    with open(f"{NORM_DIR}/stats.json") as f:
        stats = spark.createDataFrame(json.load(f))
    with open(f"{NORM_DIR}/groups.json") as f:
        validation = json.load(f)["validation"]

    # shortcut: đọc cả kho mỗi lần chạy (lọc theo máy hỏng sau); khi kho lớn thì chỉ đọc các ngày gần đây.
    failed = failed_windows(spark.read.parquet(f"{LAKE_DIR}/readings"), spark.read.parquet(f"{LAKE_DIR}/failures"),
                            stats, args.max_failed)
    out = group_dataset(spark, "train").unionByName(failed)
    out.write.mode("overwrite").parquet(args.out)

    out = spark.read.parquet(args.out)
    assert out.where(F.col("machine_id").isin(validation)).count() == 0, "tập huấn luyện có Máy của nhóm kiểm định"
    counts = out.groupBy(F.col("machine_id").startswith("vm-").alias("from_stream")).agg(
        F.countDistinct("machine_id").alias("machines"), F.count("*").alias("windows")).collect()
    print({("luồng" if r.from_stream else "gốc"): (r.machines, r.windows) for r in counts})
