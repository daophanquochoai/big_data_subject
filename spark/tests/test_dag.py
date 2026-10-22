"""DAG nạp được và có đúng thứ tự bước. Airflow nằm trong venv riêng của image (/opt/airflow), nên chạy qua nó."""
import os
import subprocess

import pytest

AIRFLOW_PYTHON = "/opt/airflow/bin/python"
CHECK = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location("retrain_rul", "/app/dags/retrain_rul.py")
mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
dag = mod.retrain_rul()
order = {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}
assert order == {"build_training_set": ["train"], "train": ["validate"], "validate": ["gate"],
                 "gate": ["keep_current", "promote"], "promote": [], "keep_current": []}, order
assert dag.max_active_runs == 1
print("ok")
"""


@pytest.mark.skipif(not os.path.exists(AIRFLOW_PYTHON), reason="cần image pdm có Airflow")
def test_dag_loads_with_the_designed_task_order():
    out = subprocess.run([AIRFLOW_PYTHON, "-c", CHECK], capture_output=True, text=True)
    assert out.returncode == 0 and out.stdout.strip().endswith("ok"), out.stderr[-2000:]
