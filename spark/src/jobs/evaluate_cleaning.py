"""Đối chiếu kho Bản tin với đáp án của bộ giả lập trong một khoảng event_time.

Dùng: evaluate_cleaning.py <từ> <tới>   (ISO-8601 UTC, ví dụ 2026-10-15T12:50:00)
Đọc lại topic sensor.raw bằng lược đồ CÓ truth. Đây là job đánh giá, không phải đường xử lý:
chỉ ở đây mới được nhìn đáp án.
"""
import json
import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from pdm import spark_session
from pdm.schema import ALL_SENSORS, LAKE_DIR, MESSAGE_SCHEMA

TRUTH_SCHEMA = MESSAGE_SCHEMA + ", truth STRUCT<rul: INT, method: STRING, fault: STRING>"

if __name__ == "__main__":
    start, end = (datetime.fromisoformat(a) for a in sys.argv[1:3])
    spark = spark_session("evaluate-cleaning")
    ms = lambda t: str(int(t.timestamp() * 1000))  # noqa: E731
    in_range = F.col("event_time").between(start, end)

    # Bản tin trễ được ghi vào Kafka muộn hơn event_time: đọc rộng thêm một phút mỗi đầu rồi lọc theo event_time.
    sent = (spark.read.format("kafka").option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
            .option("subscribe", "sensor.raw")
            .option("startingTimestamp", ms(start - timedelta(minutes=1)))
            .option("endingTimestamp", ms(end + timedelta(minutes=1)))
            .load()
            .select(F.from_json(F.col("value").cast("string"), TRUTH_SCHEMA).alias("m")).select("m.*")
            .where(in_range).cache())
    lake = spark.read.parquet(f"{LAKE_DIR}/readings").where(in_range).cache()

    faults = {r.fault or "none": r["count"] for r in sent.groupBy(F.col("truth.fault").alias("fault")).count().collect()}
    is_masked = F.greatest(*[F.col(c).isNull().cast("int") for c in ALL_SENSORS]) == 1
    masked = lake.where(is_masked).count()
    # Đối chiếu từng dòng: dòng bị xóa giá trị mà đáp án không phải bad_value là bắt nhầm số đo thật;
    # Bản tin bad_value mà dòng kho không bị xóa gì là bỏ sót.
    truth = sent.where(F.col("truth.fault").isNull() | (F.col("truth.fault") != "duplicate")).select(
        "machine_id", "cycle", F.col("truth.fault").alias("fault"))
    joined = lake.withColumn("masked", is_masked).join(truth, ["machine_id", "cycle"])
    false_alarms = joined.where(F.col("masked") & (F.coalesce(F.col("fault"), F.lit("")) != "bad_value")).count()
    missed = joined.where(~F.col("masked") & (F.col("fault") == "bad_value")).count()
    # Bản tin bị mất không bao giờ vào Kafka; bản trùng phải bị khử; Bản tin trễ (≤ late_max_s của bộ giả lập,
    # mặc định 5 giây) nằm trong watermark 10 giây nên vẫn vào kho.
    expected = sent.count() - faults.get("duplicate", 0)
    report = {
        "range": [start.isoformat(), end.isoformat()],
        "kafka_messages": sent.count(),
        "kafka_by_fault": faults,
        "kafka_distinct_machine_cycle": sent.select("machine_id", "cycle").distinct().count(),
        "lake_rows": lake.count(),
        "lake_distinct_machine_cycle": lake.select("machine_id", "cycle").distinct().count(),
        "lake_rows_not_in_kafka": lake.select("machine_id", "cycle").subtract(sent.select("machine_id", "cycle")).count(),
        "expected_lake_rows": expected,
        "lake_rows_with_masked_value": masked,
        "expected_masked": faults.get("bad_value", 0),
        "masked_but_not_bad_value": false_alarms,
        "bad_value_missed": missed,
    }
    # Bắt nhầm vài số đo thật nằm ngoài miền nới 10% (dữ liệu tổng hợp có thể vượt khoảng của nhóm gốc) thì
    # chấp nhận và báo tỷ lệ; bỏ sót lỗi hay sai số dòng kho thì không.
    report["false_alarm_rate"] = false_alarms / max(report["lake_rows"], 1)
    report["ok"] = (report["lake_rows"] == expected and report["lake_distinct_machine_cycle"] == expected
                    and missed == 0 and report["lake_rows_not_in_kafka"] == 0)
    print(json.dumps(report, indent=1))
