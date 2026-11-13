"""Single-host C-MAPSS GPU training, alongside the existing Spark GBT pipeline."""
from datetime import datetime, timedelta, timezone
import sys

from airflow.sdk import dag, task, get_current_context

sys.path.insert(0, '/training')


@dag(dag_id='single_node_rul_training', schedule='* * * * *', start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
     catchup=False, max_active_runs=1, default_args={'retries': 1, 'retry_delay': timedelta(seconds=15)}, tags=['pdm', 'gpu', 'rul'])
def single_node_rul_training():
    @task(execution_timeout=timedelta(hours=1))
    def train():
        from runner import execute_dispatch
        run = get_current_context()['dag_run']
        return execute_dispatch(run.run_id, (run.conf or {}).get('run_id'), run.run_id)
    train()


single_node_rul_training()
