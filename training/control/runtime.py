"""Bounded HTTP/Docker adapters for the existing PDM Airflow and Spark deployment."""
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

DAG = 'single_node_rul_training'
IMAGE = lambda: os.getenv('TRAINING_IMAGE', 'pdm-training:local')
_capability = (0, None)


class Unavailable(RuntimeError):
    def __init__(self, message, uncertain=False):
        super().__init__(message)
        self.uncertain = uncertain


def http(url, body=None, token=None):
    req = Request(url, data=json.dumps(body).encode() if body is not None else None,
                  headers={'Content-Type': 'application/json', **({'Authorization': 'Bearer '+token} if token else {})})
    try:
        with urlopen(req, timeout=8) as response:
            return json.load(response)
    except HTTPError as error:
        raise Unavailable(f'HTTP {error.code}', uncertain=body is not None and error.code >= 500) from None
    except (URLError, OSError, ValueError):
        raise Unavailable('Dịch vụ chưa kết nối được', uncertain=body is not None) from None


def airflow(path, body=None):
    base = os.getenv('AIRFLOW_API_URL', 'http://airflow:8080').rstrip('/')
    token = http(base+'/auth/token').get('access_token')
    if not token:
        raise Unavailable('Airflow chưa cấp token')
    return http(base+'/api/v2'+path, body, token)


def trigger(dag, run_id, conf):
    path = f'/dags/{quote(dag, safe="")}/dagRuns'
    try:
        return airflow(path, {'dag_run_id': run_id, 'logical_date': None, 'conf': conf})
    except Unavailable as error:
        if error.uncertain or str(error) == 'HTTP 409':
            try:
                return airflow(path+'/'+quote(run_id, safe=''))
            except Unavailable:
                pass
        raise


def command(args, timeout=8):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def system():
    global _capability
    stamp, cached = _capability
    if cached is not None and time.monotonic()-stamp < 15:
        return cached
    docker = command(['docker', 'info', '--format', '{{.ServerVersion}}']) is not None
    image_id = command(['docker', 'image', 'inspect', IMAGE(), '--format', '{{.Id}}']) if docker else None
    fields = 'index,name,memory.total,memory.used,utilization.gpu'
    gpu_text = command(['nvidia-smi', '--query-gpu='+fields, '--format=csv,noheader,nounits'])
    if gpu_text is None and docker and image_id:
        gpu_text = command(['docker', 'run', '--rm', '--gpus', 'all', '--entrypoint', 'nvidia-smi', image_id,
                            '--query-gpu='+fields, '--format=csv,noheader,nounits'], timeout=20)
    gpus = []
    for line in (gpu_text or '').splitlines():
        values = [v.strip() for v in line.split(',')]
        try:
            gpus.append({'index': int(values[0]), 'name': values[1], 'memory_total_mb': int(values[2]),
                         'memory_used_mb': int(values[3]) if values[3].isdigit() else None,
                         'utilization_percent': int(values[4]) if values[4].isdigit() else None})
        except (ValueError, IndexError):
            continue
    try:
        dag = airflow('/dags/'+DAG)
        healthy = not dag['is_paused'] and not dag.get('has_import_errors', False)
    except Unavailable:
        healthy = False
    value = {'host': os.getenv('TRAINING_HOST_NAME', socket.gethostname()), 'cpu_count': os.cpu_count() or 1,
             'gpu_count': len(gpus), 'gpus': gpus, 'docker_available': docker, 'image_available': bool(image_id),
             'training_image_id': image_id, 'cuda_available': bool(gpus and image_id), 'multi_gpu_available': len(gpus)>1,
             'airflow': {'status': 'healthy' if healthy else 'unavailable', 'url': os.getenv('AIRFLOW_PUBLIC_URL', 'http://localhost:8088'), 'dag_id': DAG},
             'message': 'Train RUL trên dữ liệu C-MAPSS; CPU/Gloo và GPU/NCCL được phân biệt theo phần cứng thực.'}
    _capability = time.monotonic(), value
    return value


def current_model():
    root = Path(os.getenv('MODELS_DIR', '/models'))
    try:
        current = json.loads((root/'current.json').read_text())['version']
        if not re.fullmatch(r'v[\w-]{1,80}', current):
            raise ValueError('Tên model không hợp lệ')
        return {'version': current, 'metrics': json.loads((root/current/'metrics.json').read_text()), 'framework': 'Spark MLlib GBT'}
    except (OSError, ValueError, KeyError):
        return None


def overview():
    value = {'current_model': current_model(), 'dag': None, 'cluster': None, 'errors': [],
             'airflow_url': os.getenv('AIRFLOW_PUBLIC_URL', 'http://localhost:8088')}
    try:
        value['dag'] = airflow('/dags/retrain_rul')
    except Unavailable as error:
        value['errors'].append('Airflow: '+str(error))
    try:
        value['cluster'] = http(os.getenv('SPARK_MASTER_UI_URL', 'http://spark-master:8080')+'/json/')
    except Unavailable as error:
        value['errors'].append('Spark: '+str(error))
    return value
