"""Lệnh bảo trì (Mục 8 của v1/docs/architecture.md), tính bằng Chu kỳ vì số Chu kỳ mỗi giây đổi được."""
from pyspark.sql import Window
from pyspark.sql import functions as F

THRESHOLD = 0.5  # xác suất Sắp hỏng từ đây trở lên thì ra lệnh
SAFETY = 20      # Khoảng an toàn, Chu kỳ


def new_orders(scored):
    """Mỗi Máy vượt ngưỡng trong lô: một lệnh tại cửa sổ sớm nhất vượt ngưỡng.
    Chu kỳ đề xuất = tuổi + RUL dự đoán − Khoảng an toàn, không sớm hơn tuổi hiện tại.
    Máy đã có lệnh từ lô trước bị Postgres bỏ qua (ON CONFLICT DO NOTHING)."""
    first = Window.partitionBy("machine_id").orderBy("age")
    return (scored.where(F.col("failing_prob") >= THRESHOLD)
            .withColumn("rank", F.row_number().over(first)).where(F.col("rank") == 1)
            .select("machine_id", F.col("age").alias("cycle"), "rul_pred", "failing_prob",
                    F.greatest(F.col("age"), F.round(F.col("age") + F.col("rul_pred") - SAFETY).cast("int"))
                    .alias("suggested_cycle"),
                    "model_version"))
