"""Huấn luyện một phiên bản mới: mô hình hồi quy RUL và mô hình phân lớp Sắp hỏng.

Dùng: train.py [tỷ_lệ] [--training-set <thư mục Parquet>] [--version <tên>]
  tỷ_lệ: phần số Máy đem học (mặc định 1.0), của nhóm huấn luyện gốc hoặc của tập do DAG dựng.
  --version: DAG tự đặt tên để các bước sau biết phiên bản; mặc định theo giờ hiện tại.
"""
import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F

from pdm import spark_session
from pdm.labels import FAILING_WITHIN, RUL_CAP
from pdm.schema import MODELS_DIR, NORM_DIR, read_train
from pdm.training import labeled_windows
from pdm.windows import FEATURES, MIN_READINGS, SLIDE, WAIT, WINDOW

PARAMS = {"maxIter": 50, "maxDepth": 5, "stepSize": 0.1, "seed": 42}
SEED = 42


def dataset(readings, stats):
    """Cửa sổ đủ Bản tin của các Máy đã chạy tới lúc hỏng, gắn nhãn theo Chu kỳ cuối của từng Máy."""
    return labeled_windows(readings, stats, readings.groupBy("machine_id").agg(F.max("cycle").alias("last_cycle")))


def group_dataset(spark, group, fraction=1.0):
    """Đặc trưng và nhãn của một nhóm Máy: "train" hoặc "validation"; fraction < 1 chỉ lấy một phần số Máy."""
    with open(f"{NORM_DIR}/groups.json") as f:
        machines = json.load(f)[group]
    if fraction < 1:
        # Xáo rồi mới cắt: danh sách đã sắp xếp nên cắt thẳng chỉ lấy được Máy FD001.
        random.Random(SEED).shuffle(machines)
        machines = machines[:round(fraction * len(machines))]
    with open(f"{NORM_DIR}/stats.json") as f:
        stats = spark.createDataFrame(json.load(f))
    return dataset(read_train(spark).where(F.col("machine_id").isin(machines)), stats)


def measure(version, data):
    """Số đo của một phiên bản trên data, bằng mô hình nạp từ đĩa."""
    vdir = f"{MODELS_DIR}/{version}"
    scored = PipelineModel.load(f"{vdir}/failing").transform(PipelineModel.load(f"{vdir}/rul").transform(data))
    hit, flagged = F.col("failing") == 1, F.col("failing_pred") == 1
    m = scored.agg(
        F.sqrt(F.avg((F.col("rul_pred") - F.col("rul")) ** 2)).alias("rmse"),
        F.sum((hit & flagged).cast("int")).alias("tp"),
        F.sum((~hit & flagged).cast("int")).alias("fp"),
        F.sum((hit & ~flagged).cast("int")).alias("fn"),
    ).first()
    # Mô hình không báo Máy nào Sắp hỏng thì precision, recall và F1 đều tính là 0.
    return {
        "rul_rmse": m.rmse,
        "failing_precision": m.tp / (m.tp + m.fp) if m.tp + m.fp else 0.0,
        "failing_recall": m.tp / (m.tp + m.fn) if m.tp + m.fn else 0.0,
        "failing_f1": 2 * m.tp / (2 * m.tp + m.fp + m.fn) if m.tp else 0.0,
    }


def pipeline(name, estimator):
    # Tên cột riêng cho từng mô hình để job luồng áp nối tiếp cả hai lên cùng một bảng.
    return Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol=f"{name}_features"), estimator])


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("fraction", nargs="?", type=float, default=1.0)
    p.add_argument("--training-set")
    p.add_argument("--version", default=datetime.now(timezone.utc).strftime("v%Y%m%dT%H%M%S"))
    args = p.parse_args()
    fraction = args.fraction
    assert 0 < fraction <= 1, f"tỷ lệ phải trong (0, 1], nhận {fraction}"
    spark = spark_session("train")
    if args.training_set:
        train = spark.read.parquet(args.training_set)
        if fraction < 1:  # cố ý học ít, để thử một lần Cổng kiểm định từ chối
            machines = sorted(r.machine_id for r in train.select("machine_id").distinct().collect())
            random.Random(SEED).shuffle(machines)
            train = train.where(F.col("machine_id").isin(machines[:max(1, round(fraction * len(machines)))]))
    else:
        train = group_dataset(spark, "train", fraction)
    train = train.cache()
    val = group_dataset(spark, "validation").cache()

    rul = pipeline("rul", GBTRegressor(
        featuresCol="rul_features", labelCol="rul", predictionCol="rul_pred", **PARAMS)).fit(train)
    failing = pipeline("failing", GBTClassifier(
        featuresCol="failing_features", labelCol="failing", predictionCol="failing_pred", **PARAMS
    ).setRawPredictionCol("failing_raw").setProbabilityCol("failing_prob")).fit(train)

    version = args.version
    vdir = f"{MODELS_DIR}/{version}"
    rul.save(f"{vdir}/rul")
    failing.save(f"{vdir}/failing")
    # measure() nạp lại mô hình từ đĩa: nếu lưu hoặc nạp hỏng thì job dừng ở đây.
    measured = measure(version, val)
    mean_rul = train.agg(F.avg("rul")).first()[0]
    baseline = val.agg(F.sqrt(F.avg((F.lit(mean_rul) - F.col("rul")) ** 2))).first()[0]

    metrics = {
        "version": version,
        "measured_on": "nhóm kiểm định",
        **measured,
        "rul_rmse_constant_baseline": baseline,
        "train": {"fraction": fraction, "training_set": args.training_set, "machines": train.select("machine_id").distinct().count(), "windows": train.count()},
        "validation": {"machines": val.select("machine_id").distinct().count(), "windows": val.count()},
        "params": PARAMS,
        "labels": {"rul_cap": RUL_CAP, "failing_within": FAILING_WITHIN},
        "windows": {"window_cycles": WINDOW, "slide_cycles": SLIDE, "min_readings": MIN_READINGS, "stream_wait_cycles": WAIT},
        "n_features": len(FEATURES),
        "spark": spark.version,
    }
    with open(f"{vdir}/metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=1, ensure_ascii=False)
    print(json.dumps(metrics, indent=1, ensure_ascii=False))
    assert measured["rul_rmse"] < baseline, "mô hình hồi quy không hơn dự đoán hằng số; kiểm tra bước chuẩn hóa"

    # Chỉ tạo con trỏ cho phiên bản đầu tiên; thay Mô hình đang dùng là việc của Cổng kiểm định.
    current = f"{MODELS_DIR}/current.json"
    if not os.path.exists(current):
        with open(f"{current}.tmp", "w") as f:
            json.dump({"version": version}, f)
        os.replace(f"{current}.tmp", current)  # đổi tên để job luồng không đọc phải file dở dang
        print(f"Mô hình đang dùng: {version}")
