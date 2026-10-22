"""Job luồng: đọc Bản tin từ Kafka, chấm điểm từng Máy mỗi 5 Chu kỳ, ghi Postgres.

Chạy trên cụm khi đặt SPARK_MASTER. Spark UI ở cổng 4040, tab Structured Streaming.
"""
import json
import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from pdm import spark_session
from pdm.cleaning import mask_out_of_range, parse, valid_ranges
from pdm.db import JDBC_PROPS, JDBC_URL, insert_ignore
from pdm.features import normalize
from pdm.orders import new_orders
from pdm.schema import LAKE_DIR, MODELS_DIR, NORM_DIR, SENSORS, read_train
from pdm.windows import FEATURES, stream_windows

CORES = 2  # chừa 2 trong 4 core của cụm cho job huấn luyện lại
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINTS = os.environ.get("CHECKPOINT_DIR", "/checkpoints")
FAILURE_SCHEMA = "machine_id STRING, failed_at TIMESTAMP, last_cycle INT, method STRING, base ARRAY<STRING>"

loaded = {}  # Mô hình đang dùng, giữ trong driver giữa các lô


def current_models():
    """Đọc con trỏ mỗi lô; chỉ nạp lại hai mô hình khi phiên bản đổi, nên đổi mô hình không cần khởi động lại."""
    with open(f"{MODELS_DIR}/current.json") as f:
        version = json.load(f)["version"]
    if loaded.get("version") != version:
        vdir = f"{MODELS_DIR}/{version}"
        loaded.update(version=version, rul=PipelineModel.load(f"{vdir}/rul"), failing=PipelineModel.load(f"{vdir}/failing"))
        print(f"Mô hình đang dùng: {version}", flush=True)
    return loaded


def score(windows, _batch_id):
    m = current_models()
    # cache: lô được dùng hai lần (dự đoán, Lệnh bảo trì); không cache thì toán tử có trạng thái chạy lại.
    # Cửa sổ có đặc trưng rỗng (cả cửa sổ mất một cảm biến sau làm sạch) thì bỏ: mô hình không nhận giá trị rỗng.
    scored = (m["failing"].transform(m["rul"].transform(windows.dropna(subset=FEATURES)))
              .select("machine_id", "window_end", "age", "rul_pred",
                      vector_to_array("failing_prob")[1].alias("failing_prob"), F.lit(m["version"]).alias("model_version"))
              .cache())
    # Ít nhất một lần: khởi động lại giữa lô có thể ghi trùng lô đó (Mục 6.6 của v1/docs/architecture.md).
    (scored.withColumnRenamed("age", "window_end_cycle")
     .write.jdbc(JDBC_URL, "predictions", "append", JDBC_PROPS))
    orders = [tuple(r) for r in new_orders(scored).collect()]
    insert_ignore(scored.sparkSession, "maintenance_orders",
                  ["machine_id", "cycle", "rul_pred", "failing_prob", "suggested_cycle", "model_version"], orders)
    scored.unpersist()


def record_failures(batch, _batch_id):
    """Sự kiện hỏng vào Postgres: dashboard loại máy đã hỏng, đánh giá Lệnh bảo trì."""
    rows = [(r.machine_id, r.failed_at.isoformat(), r.last_cycle, r.method, "{" + ",".join(r.base or []) + "}")
            for r in batch.collect()]
    insert_ignore(batch.sparkSession, "failures", ["machine_id", "failed_at", "last_cycle", "method", "base"], rows,
                  casts={"failed_at": "timestamp", "base": "text[]"})


def kafka(spark, topic):
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")  # topic tạo lại hoặc hết hạn lưu thì đọc tiếp, không dừng
            .load())


if __name__ == "__main__":
    spark = spark_session("stream", {
        "spark.cores.max": str(CORES),
        "spark.executor.cores": "1",
        # Khóa số kho trạng thái lúc chạy lần đầu; đổi giá trị này phải xóa checkpoint.
        # Lát 5 đo lại với 200 (mặc định của Spark) bằng biến SHUFFLE_PARTITIONS, trên checkpoint riêng.
        "spark.sql.shuffle.partitions": os.environ.get("SHUFFLE_PARTITIONS", str(CORES)),
        "spark.ui.enabled": "true",  # tab Structured Streaming: tốc độ nhận từ Kafka, tốc độ xử lý, thời gian lô
        "spark.ui.showConsoleProgress": "false",
    })
    with open(f"{NORM_DIR}/stats.json") as f:
        stats = spark.createDataFrame(json.load(f))

    with open(f"{NORM_DIR}/groups.json") as f:
        base = json.load(f)["train"]
    ranges = valid_ranges(read_train(spark).where(F.col("machine_id").isin(base)))
    readings = mask_out_of_range(parse(kafka(spark, "sensor.raw")), ranges)
    windows = stream_windows(normalize(readings, stats).select("machine_id", "cycle", "event_time", *SENSORS))
    (windows.writeStream.queryName("cham_diem").outputMode("append").foreachBatch(score)
     .option("checkpointLocation", f"{CHECKPOINTS}/scoring").trigger(processingTime="5 seconds").start())

    # Kho Bản tin đã làm sạch, cho huấn luyện lại. File sink: đúng một lần; ghi thưa để ít file nhỏ.
    # Khử trùng trong watermark 10 giây: Bản tin trễ hơn thế không vào kho (bộ giả lập mặc định trễ tối đa 5 giây).
    (readings.withWatermark("event_time", "10 seconds").dropDuplicatesWithinWatermark(["machine_id", "cycle"])
     .withColumn("date", F.to_date("event_time")).writeStream.queryName("kho_ban_tin")
     .format("parquet").option("path", f"{LAKE_DIR}/readings").partitionBy("date")
     .option("checkpointLocation", f"{CHECKPOINTS}/lake_readings").trigger(processingTime="60 seconds").start())

    failures = (kafka(spark, "machine.failed")
                .select(F.from_json(F.col("value").cast("string"), FAILURE_SCHEMA).alias("f")).select("f.*")
                .where(F.col("machine_id").isNotNull()))
    # Postgres cần nhanh (dashboard trừ máy đã hỏng); kho thì ghi thưa để không đẻ nhiều file nhỏ trên thư mục
    # gắn từ Windows. File sink cho kho bảo đảm đúng một lần.
    (failures.writeStream.queryName("su_kien_hong").foreachBatch(record_failures)
     .option("checkpointLocation", f"{CHECKPOINTS}/failures").trigger(processingTime="2 seconds").start())
    (failures.writeStream.queryName("kho_su_kien_hong").format("parquet").option("path", f"{LAKE_DIR}/failures")
     .option("checkpointLocation", f"{CHECKPOINTS}/lake_failures").trigger(processingTime="60 seconds").start())

    spark.streams.awaitAnyTermination()
