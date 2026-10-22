"""Tập huấn luyện: cửa sổ đặc trưng gắn nhãn, từ C-MAPSS gốc hoặc từ kho của luồng."""
from pyspark.sql import functions as F

from pdm.features import normalize
from pdm.labels import with_labels
from pdm.windows import spark_windows


def labeled_windows(readings, stats, last_cycles):
    """Cửa sổ đủ Bản tin của các Máy đã chạy tới lúc hỏng; last_cycles: (machine_id, last_cycle)."""
    return with_labels(spark_windows(normalize(readings, stats)), last_cycles)


def failed_windows(readings, failures, stats, max_failed):
    """Máy ảo đã hỏng trong kho, tối đa max_failed máy hỏng gần nhất.

    Chu kỳ cuối lấy từ sự kiện hỏng, không từ Bản tin cuối trong kho: Bản tin có thể mất hoặc bị loại khi làm sạch.
    Trần số máy giữ thời gian huấn luyện không tăng theo thời gian hệ thống chạy, và không để các bản sao
    của ~142 vòng đời phát lại lấn át nhóm huấn luyện gốc (Mục 10 của v1/docs/architecture.md).
    """
    latest = failures.orderBy(F.col("failed_at").desc()).limit(max_failed).select("machine_id", "last_cycle")
    return labeled_windows(readings.join(latest.select("machine_id"), "machine_id", "left_semi"), stats, latest)
