"""Ghi Postgres. Ghi thêm thường dùng DataFrame.write.jdbc; hàm ở đây cho trường hợp Spark không hỗ trợ:
INSERT ... ON CONFLICT DO NOTHING (mỗi Máy tối đa một dòng), chạy trên driver qua JDBC của JVM."""
import os

JDBC_URL = "jdbc:postgresql://postgres:5432/pdm"
USER = "pdm"
PASSWORD = os.environ.get("POSTGRES_PASSWORD", "pdm")
JDBC_PROPS = {"user": USER, "password": PASSWORD, "driver": "org.postgresql.Driver"}


def insert_ignore(spark, table, columns, rows, casts=None, conflict="machine_id"):
    """rows: các bộ giá trị Python (str, int, float). casts: kiểu Postgres cho từng cột cần ép,
    ví dụ {"failed_at": "timestamptz", "base": "text[]"}: truyền chuỗi rồi ép trong SQL, khỏi đổi kiểu qua py4j."""
    if not rows:
        return
    casts = casts or {}
    marks = ", ".join(f"?::{casts[c]}" if c in casts else "?" for c in columns)
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks}) ON CONFLICT ({conflict}) DO NOTHING"
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD)
    try:
        ps = conn.prepareStatement(sql)
        for row in rows:
            for i, value in enumerate(row, 1):
                ps.setObject(i, value)
            ps.addBatch()
        ps.executeBatch()
    finally:
        conn.close()
