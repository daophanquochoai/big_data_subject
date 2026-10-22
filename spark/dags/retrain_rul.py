"""Huấn luyện lại (Mục 10 của v1/docs/architecture.md):
build_training_set >> train >> validate >> gate >> promote | keep_current.

Mỗi bước nặng là một spark-submit lên cụm, dùng 2 core mà job luồng chừa lại.
Chạy tay với tham số: max_failed (số Máy ảo đã hỏng tối đa), fraction (< 1 để cố ý học ít, thử một lần bị từ chối).
"""
import json
import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, dag, task

MODELS = "/models"
SUBMIT = ("cd /app && spark-submit --master spark://spark-master:7077 --driver-memory 2g "
          "--conf spark.cores.max=2 --conf spark.executor.cores=1 --conf spark.ui.port=4041 ")
VERSION = "v{{ dag_run.run_after.strftime('%Y%m%dT%H%M%S') }}"
TRAINING_SET = "/data/lake/training_sets/" + VERSION


@dag(schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1, tags=["pdm"],
     params={"max_failed": Param(500, type="integer", minimum=1),
             "fraction": Param(1.0, type="number", exclusiveMinimum=0, maximum=1)})
def retrain_rul():
    build = BashOperator(task_id="build_training_set", bash_command=(
        SUBMIT + "src/jobs/build_training_set.py " + TRAINING_SET + " --max-failed {{ params.max_failed }}"))
    train = BashOperator(task_id="train", bash_command=(
        SUBMIT + "src/jobs/train.py {{ params.fraction }} --training-set " + TRAINING_SET + " --version " + VERSION))
    validate = BashOperator(task_id="validate", bash_command=SUBMIT + "src/jobs/validate.py " + VERSION)

    @task.branch
    def gate(version):
        """Cổng kiểm định: validate.py đã quyết và ghi kết quả cạnh ứng viên."""
        with open(f"{MODELS}/{version}/validation.json") as f:
            return "promote" if json.load(f)["accepted"] else "keep_current"

    @task
    def promote(version):
        """Đổi con trỏ bằng file tạm rồi đổi tên, để job luồng không đọc phải file dở dang."""
        tmp = f"{MODELS}/current.json.tmp"
        with open(tmp, "w") as f:
            json.dump({"version": version}, f)
        os.replace(tmp, f"{MODELS}/current.json")

    keep = EmptyOperator(task_id="keep_current")  # ứng viên vẫn nằm trên đĩa để tra cứu
    build >> train >> validate >> gate(VERSION) >> [promote(VERSION), keep]


retrain_rul()
