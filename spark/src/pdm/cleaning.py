"""Làm sạch Bản tin đọc từ Kafka."""
from pyspark.sql import functions as F

from pdm.schema import ALL_SENSORS, MESSAGE_SCHEMA


def parse(raw):
    """Cột value của Kafka -> các cột của Bản tin. Lược đồ không có truth nên đáp án bị bỏ ngay ở đây;
    Bản tin không phân tích được cho ra cột rỗng và bị loại, không làm dừng truy vấn."""
    return (raw.select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m")).select("m.*")
            .where(F.col("machine_id").isNotNull() & F.col("cycle").isNotNull() & F.col("event_time").isNotNull()))


MARGIN = 0.1  # nới mỗi phía 10% khoảng [min, max] của nhóm huấn luyện gốc


def valid_ranges(readings):
    """Miền hợp lệ của từng cảm biến, từ Bản tin của nhóm huấn luyện gốc: {cột: (thấp, cao)}."""
    row = readings.agg(*[f(c).alias(f"{c}_{k}") for c in ALL_SENSORS for f, k in ((F.min, "lo"), (F.max, "hi"))]).first()
    out = {}
    for c in ALL_SENSORS:
        lo, hi = row[f"{c}_lo"], row[f"{c}_hi"]
        out[c] = (lo - MARGIN * (hi - lo), hi + MARGIN * (hi - lo))
    return out


def mask_out_of_range(readings, ranges):
    """Số đo ngoài miền hợp lệ thành rỗng (bắt được −9999 và giá trị gấp 10 của bộ giả lập).
    Không bỏ cả Bản tin: các cảm biến khác vẫn đúng, và hàm cửa sổ bỏ qua giá trị rỗng."""
    return readings.select(*[F.when(F.col(c).between(*ranges[c]), F.col(c)).alias(c) if c in ranges else F.col(c)
                             for c in readings.columns])
