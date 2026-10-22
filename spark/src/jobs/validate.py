"""Cổng kiểm định: đo ứng viên và Mô hình đang dùng trên cùng nhóm kiểm định.

Dùng: validate.py [phiên_bản_ứng_viên]   (mặc định: phiên bản mới nhất trong Thư mục mô hình)
Chỉ quyết định và ghi kết quả; không đổi con trỏ Mô hình đang dùng.
"""
import glob
import json
import os
import sys
from datetime import datetime, timezone

from pdm import spark_session
from pdm.db import JDBC_PROPS, JDBC_URL
from pdm.gate import accepts
from pdm.schema import MODELS_DIR
from train import group_dataset, measure

if __name__ == "__main__":
    versions = sorted(os.path.basename(p) for p in glob.glob(f"{MODELS_DIR}/v*"))
    candidate = sys.argv[1] if len(sys.argv) > 1 else versions[-1]
    assert candidate in versions, f"không có phiên bản {candidate}; hiện có: {versions}"
    with open(f"{MODELS_DIR}/current.json") as f:
        current = json.load(f)["version"]

    spark = spark_session("validate")
    val = group_dataset(spark, "validation").cache()
    result = {
        "validated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "candidate": {"version": candidate, **measure(candidate, val)},
        "current": {"version": current, **measure(current, val)},
    }
    result["accepted"] = accepts(result["candidate"], result["current"])

    c, cur = result["candidate"], result["current"]
    spark.createDataFrame(
        [(candidate, c["rul_rmse"], c["failing_f1"], current, cur["rul_rmse"], cur["failing_f1"], result["accepted"])],
        "candidate STRING, candidate_rmse DOUBLE, candidate_f1 DOUBLE, current STRING, current_rmse DOUBLE, "
        "current_f1 DOUBLE, accepted BOOLEAN").write.jdbc(JDBC_URL, "model_validation", "append", JDBC_PROPS)
    # Bản file cạnh ứng viên: bước rẽ nhánh của DAG đọc nó.
    with open(f"{MODELS_DIR}/{candidate}/validation.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1)
    print(json.dumps(result, indent=1))
    print("ĐƯỢC NHẬN" if result["accepted"] else "KHÔNG NHẬN")
