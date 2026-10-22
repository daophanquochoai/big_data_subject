"""Lược đồ Bản tin và cách đọc file C-MAPSS."""
import os
from functools import reduce

from pyspark.sql import functions as F

# Gắn cùng đường dẫn vào mọi container Spark, để executor đọc được thứ driver chỉ định.
RAW_DIR = os.environ.get("RAW_DIR", "/data/raw/cmapss")
LAKE_DIR = os.environ.get("LAKE_DIR", "/data/lake")
MODELS_DIR = os.environ.get("MODELS_DIR", "/models")
NORM_DIR = f"{MODELS_DIR}/norm_stats"  # bảng thống kê chuẩn hóa và ba nhóm Máy, cố định qua mọi phiên bản
SUBSETS = ["FD001", "FD002", "FD003", "FD004"]

SETTINGS = ["setting1", "setting2", "setting3"]
ALL_SENSORS = [f"s{i}" for i in range(1, 22)]
# Cảm biến 1, 5, 16, 18, 19 là hằng số trong hầu hết Điều kiện vận hành.
SENSORS = [f"s{i}" for i in (2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 20, 21)]

# Bản tin JSON trên topic sensor.raw. Cố ý không có trường truth (đáp án của bộ giả lập): phân tích theo lược đồ này
# là bỏ đáp án ngay khi đọc, nên mô hình và phần làm sạch không thể dùng nó.
MESSAGE_SCHEMA = "machine_id STRING, cycle INT, event_time TIMESTAMP, " + ", ".join(
    f"{c} DOUBLE" for c in SETTINGS + ALL_SENSORS)


def read_train(spark, subsets=SUBSETS, raw_dir=RAW_DIR, kind="train"):  # kind="test": tập test chính thức
    """Bản tin của các Máy train, cùng dạng với Bản tin trên luồng.

    Số máy trùng nhau giữa các tập nên mã máy ghép từ tên tập và số máy.
    event_time giả = Chu kỳ x 1 giây, để cửa sổ theo thời gian áp được lên dữ liệu file.
    """
    def one(subset):
        # File có dấu cách thừa ở cuối dòng nên tách theo khoảng trắng thay vì đọc CSV.
        v = F.split(F.trim("value"), r"\s+")
        values = [v[i + 2].cast("double").alias(c) for i, c in enumerate(SETTINGS + ALL_SENSORS)]
        return spark.read.text(f"{raw_dir}/{kind}_{subset}.txt").select(
            F.concat(F.lit(f"{subset}-"), F.lpad(v[0], 3, "0")).alias("machine_id"),
            v[1].cast("int").alias("cycle"),
            F.timestamp_seconds(v[1].cast("int")).alias("event_time"),
            *values,
        )

    return reduce(lambda a, b: a.unionByName(b), [one(s) for s in subsets])


if __name__ == "__main__":
    from pdm import spark_session

    spark = spark_session("read-cmapss")
    stats = read_train(spark).groupBy(F.substring("machine_id", 1, 5).alias("subset")).agg(
        F.count("*").alias("readings"), F.countDistinct("machine_id").alias("machines")
    ).orderBy("subset").collect()
    for r in stats:
        print(f"{r.subset}: {r.readings} Bản tin, {r.machines} Máy")
    total = (sum(r.readings for r in stats), sum(r.machines for r in stats))
    print(f"Tổng: {total[0]} Bản tin, {total[1]} Máy")
    assert total == (160359, 709), total
