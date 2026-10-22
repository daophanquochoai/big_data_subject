"""Chuẩn hóa theo Điều kiện vận hành. Đặc trưng cửa sổ nằm ở pdm.windows."""
from pyspark.sql import functions as F

from pdm.schema import SENSORS


def with_condition(readings):
    """Thêm cột Điều kiện vận hành, suy ra bằng cách làm tròn ba cột thiết lập."""
    # Ép về số nguyên để -0.0007 và 0.0003 cùng ra "0", không thành "-0.0" và "0.0".
    parts = [F.round("setting1").cast("int"), F.round(F.col("setting2") * 100).cast("int"),
             F.round("setting3").cast("int")]
    return readings.withColumn("condition", F.concat_ws("_", *parts))


def condition_stats(readings):
    """Bảng thống kê chuẩn hóa: mỗi Điều kiện vận hành một dòng, mỗi cảm biến hai cột."""
    aggs = [f(s).alias(f"{s}_{k}") for s in SENSORS for f, k in ((F.avg, "mu"), (F.stddev_samp, "sigma"))]
    return with_condition(readings).groupBy("condition").agg(*aggs)


def normalize(readings, stats):
    """Thay số đo của 16 cảm biến bằng (số đo - trung bình) / độ lệch chuẩn của điều kiện đó.

    Bản tin có Điều kiện vận hành không nằm trong bảng thống kê bị loại.
    """
    joined = with_condition(readings).join(F.broadcast(stats), "condition")
    scaled = [((F.col(s) - F.col(f"{s}_mu")) / F.col(f"{s}_sigma")).alias(s) for s in SENSORS]
    return joined.select("machine_id", "cycle", "event_time", "condition", *scaled)

