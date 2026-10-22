import os

from pyspark.sql import SparkSession


def spark_session(name, conf=None):
    """Master lấy từ SPARK_MASTER (ví dụ spark://spark-master:7077), mặc định một tiến trình."""
    master = os.environ.get("SPARK_MASTER", "local[*]")
    return (
        SparkSession.builder.master(master)
        .appName(name)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        # Bật Spark UI khi chạy trên cụm (job luồng 4040, job của Airflow 4041); tắt khi chạy một tiến trình.
        .config("spark.ui.enabled", str(not master.startswith("local")).lower())
        .config(map=conf or {})
        .getOrCreate()
    )
