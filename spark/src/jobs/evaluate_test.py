"""Đo một phiên bản trên tập test chính thức của C-MAPSS (chỉ cho báo cáo, không dùng để chọn mô hình).

Dùng: evaluate_test.py <phiên_bản>
Huấn luyện phiên bản học cả nhóm gốc trước: train.py 1.0 --version <phiên_bản>.
Mỗi Máy test: cửa sổ kết thúc ở Chu kỳ cuối (bỏ ngưỡng 20 Bản tin), so với RUL thật trong RUL_FD00x.txt.
"""
import json
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from pdm import spark_session
from pdm.features import normalize
from pdm.labels import FAILING_WITHIN, RUL_CAP
from pdm.schema import MODELS_DIR, NORM_DIR, RAW_DIR, SUBSETS, read_train
from pdm.windows import OUTPUT_SCHEMA, last_window


def true_rul(spark):
    """RUL thật sau Chu kỳ cuối của từng Máy test: dòng thứ i của RUL_FD00x.txt là Máy i."""
    rows = []
    for subset in SUBSETS:
        with open(f"{RAW_DIR}/RUL_{subset}.txt") as f:
            rows += [(f"{subset}-{i:03d}", int(v)) for i, v in enumerate(f.read().split(), 1)]
    return spark.createDataFrame(rows, "machine_id STRING, true_rul INT")


if __name__ == "__main__":
    version = sys.argv[1]
    spark = spark_session("evaluate-test")
    with open(f"{NORM_DIR}/stats.json") as f:
        stats = spark.createDataFrame(json.load(f))
    windows = (normalize(read_train(spark, kind="test"), stats).groupBy("machine_id")
               .applyInPandas(last_window, OUTPUT_SCHEMA).join(true_rul(spark), "machine_id"))
    vdir = f"{MODELS_DIR}/{version}"
    scored = (PipelineModel.load(f"{vdir}/failing").transform(PipelineModel.load(f"{vdir}/rul").transform(windows))
              .withColumn("subset", F.substring("machine_id", 1, 5)).cache())

    def metrics(df):
        hit, flagged = F.col("true_rul") <= FAILING_WITHIN, F.col("failing_pred") == 1
        m = df.agg(F.count("*").alias("n"),
                   F.sqrt(F.avg((F.col("rul_pred") - F.col("true_rul")) ** 2)).alias("rmse"),
                   F.sqrt(F.avg((F.col("rul_pred") - F.least("true_rul", F.lit(RUL_CAP))) ** 2)).alias("rmse_capped"),
                   F.sum((hit & flagged).cast("int")).alias("tp"), F.sum((~hit & flagged).cast("int")).alias("fp"),
                   F.sum((hit & ~flagged).cast("int")).alias("fn")).first()
        return {"machines": m.n, "rul_rmse": m.rmse, "rul_rmse_true_capped_125": m.rmse_capped,
                "failing_precision": m.tp / (m.tp + m.fp) if m.tp + m.fp else 0.0,
                "failing_recall": m.tp / (m.tp + m.fn) if m.tp + m.fn else 0.0}

    result = {"version": version, "measured_on": "tập test chính thức C-MAPSS, cửa sổ cuối mỗi Máy",
              "all": metrics(scored), **{s: metrics(scored.where(F.col("subset") == s)) for s in SUBSETS}}
    with open(f"{vdir}/test_metrics.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1, ensure_ascii=False)
    print(json.dumps(result, indent=1, ensure_ascii=False))
    assert result["all"]["machines"] == 707, result["all"]["machines"]
