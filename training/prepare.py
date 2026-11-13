"""Prepare real PDM features without installing PyTorch in Airflow."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time


class Cancelled(Exception):
    pass


def dataset(run, api):
    root = Path('/data/training')
    root.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(run['run_key'].encode()).hexdigest()[:32]
    manifest = root/f'run-{key}.json'
    if manifest.is_file():
        value = json.loads(manifest.read_text())
        path = Path(value['path'])
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != value['sha256']:
            raise ValueError('Dataset snapshot changed or disappeared; start a new run')
        return value
    digest = hashlib.sha256(str(run.get('fraction', .2)).encode())
    inputs = sorted(Path('/data/raw/cmapss').glob('train_FD*.txt'))
    inputs += [Path('/models/norm_stats/groups.json'), Path('/models/norm_stats/stats.json')]
    inputs += sorted(Path('/app/src/pdm').glob('*.py'))
    inputs += [Path('/app/src/jobs/train.py'), Path('/app/src/jobs/export_torch_data.py'), Path(__file__)]
    for path in inputs:
        digest.update(path.name.encode())
        with path.open('rb') as file:
            for chunk in iter(lambda: file.read(1024*1024), b''):
                digest.update(chunk)
    fingerprint = digest.hexdigest()
    folder = root/fingerprint
    if not (folder/'metadata.json').is_file():
        from runner import log
        api.update(run['id'], {'logs': [log('Spark đang chuẩn bị 33 đặc trưng RUL C-MAPSS từ nhóm train/validation hiện hữu.')]})
        args = ['spark-submit', '--master', os.getenv('SPARK_MASTER', 'spark://spark-master:7077'),
                '--driver-memory', '1g', '--conf', 'spark.cores.max=2', '--conf', 'spark.executor.cores=1',
                '--conf', 'spark.ui.port=4041', '/app/src/jobs/export_torch_data.py', '--output', str(folder),
                '--fraction', str(run.get('fraction', .2)), '--fingerprint', fingerprint]
        log_file = root/f'{key}-prepare.log'
        with log_file.open('wb') as output:
            process = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            started = time.monotonic()
            try:
                while process.poll() is None:
                    current = api.request(f"runs/{run['id']}")
                    if current.get('cancel_requested'):
                        raise Cancelled()
                    if time.monotonic()-started > 900:
                        raise TimeoutError('C-MAPSS preparation exceeded 15 minutes')
                    time.sleep(2)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
            if process.returncode:
                raise RuntimeError('Chuẩn bị C-MAPSS thất bại: '+log_file.read_text(errors='replace')[-1800:])
    path = folder/'dataset.npz'
    if not path.is_file():
        raise ValueError('Không có dataset C-MAPSS đã xuất')
    value = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'fingerprint': fingerprint}
    temporary = manifest.with_suffix('.tmp')
    temporary.write_text(json.dumps(value), encoding='utf-8')
    os.replace(temporary, manifest)
    return value
