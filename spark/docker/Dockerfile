FROM python:3.12-slim-bookworm

# Spark 4 cần Java 17; procps vì script khởi động của Spark gọi `ps`.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

# pandas và pyarrow cho hàm cửa sổ theo từng máy (applyInPandas, applyInPandasWithState).
RUN pip install --no-cache-dir pyspark==4.1.3 numpy pandas==2.2.3 pyarrow==18.1.0 pytest

# Thư viện Kafka cho Structured Streaming và driver JDBC của Postgres nằm sẵn trong jars của Spark,
# nên driver và executor không tải gì lúc chạy. Chỉ chép các jar Spark chưa có, tránh hai bản của cùng thư viện.
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.4').getOrCreate().stop()" \
 && cp /root/.ivy2*/jars/*kafka*.jar /root/.ivy2*/jars/*commons-pool2-*.jar /root/.ivy2*/jars/*postgresql-*.jar \
       /usr/local/lib/python3.12/site-packages/pyspark/jars/ \
 && rm -rf /root/.ivy2*

# Airflow trong venv riêng: bộ ràng buộc phiên bản của Airflow ghim pandas, pyarrow... khác bản job Spark dùng.
# DAG chỉ gọi spark-submit của Python hệ thống, nên hai môi trường không cần chung thư viện.
RUN python -m venv /opt/airflow  && /opt/airflow/bin/pip install --no-cache-dir "apache-airflow[postgres]==3.3.2"       --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

ENV PYTHONPATH=/app/src PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
