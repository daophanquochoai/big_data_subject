"""Nhãn huấn luyện: RUL và Sắp hỏng, gắn cho từng cửa sổ. Chỉ dùng cho Máy đã chạy tới lúc hỏng."""
from pyspark.sql import functions as F

RUL_CAP = 125        # Máy còn khỏe thì cảm biến chưa cho thấy gì, không bắt mô hình phân biệt
FAILING_WITHIN = 60  # Sắp hỏng = hỏng trong N Chu kỳ tới


def with_labels(windows, last_cycles):
    """RUL thật tại cửa sổ = Chu kỳ cuối của Máy − tuổi của cửa sổ.

    Tính theo Chu kỳ cuối chứ không theo từng Bản tin, vì dữ liệu luồng trong kho có thể thiếu Chu kỳ.
    last_cycles: (machine_id, last_cycle).
    """
    true_rul = F.col("last_cycle") - F.col("age")
    return (windows.join(last_cycles, "machine_id")
            .withColumn("rul", F.least(true_rul, F.lit(RUL_CAP)).cast("double"))
            .withColumn("failing", (true_rul <= FAILING_WITHIN).cast("double"))
            .drop("last_cycle"))
