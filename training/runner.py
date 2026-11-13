"""Airflow's small Docker boundary; no PyTorch imports or shell commands here."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json
import os
import subprocess
import tarfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class DockerError(RuntimeError):
    def __init__(self, message: str, *, not_found: bool = False):
        super().__init__(message)
        self.not_found = not_found


def docker_command(args: list[str], *, timeout: int = 30) -> str | bytes:
    archive = args[:2] == ['container', 'cp'] and args[-1] == '-'
    result = subprocess.run(['docker', *args], capture_output=True, text=not archive, timeout=timeout)
    if result.returncode:
        message = result.stderr.strip() or result.stdout.strip() or 'Docker command failed'
        if isinstance(message, bytes):
            message = message.decode(errors='replace')
        raise DockerError(message[-2000:], not_found='No such container' in message)
    return result.stdout if archive else result.stdout.strip()


def config_hash(run: dict) -> str:
    immutable = {k: run.get(k) for k in ('device', 'world_size', 'epochs', 'batch_size', 'learning_rate', 'training_image', 'dataset', 'fraction', 'dataset_fingerprint')}
    return hashlib.sha256(json.dumps(immutable, sort_keys=True).encode()).hexdigest()


def artifact_directory(run: dict) -> str:
    return hashlib.sha256(run['run_key'].encode()).hexdigest()[:32]


def container_name(run: dict) -> str:
    return f'pdm-training-{artifact_directory(run)}'


def inspect_container(name: str, docker=docker_command) -> dict | None:
    try:
        return json.loads(docker(['container', 'inspect', name]))[0]
    except DockerError as exc:
        if exc.not_found:
            return None
        raise


def start_container(name, run, docker=docker_command):
    if run.get('dataset_path'):
        docker(['container', 'cp', run['dataset_path'], name+':/data/training.npz'])
    docker(['container', 'start', name])


def ensure_container(run: dict, docker=docker_command, *, image: str | None = None) -> str:
    """Stable name means retry attaches, including when Docker create's reply was lost."""
    name = container_name(run)
    existing = inspect_container(name, docker)
    if existing:
        if (existing['Config']['Labels'].get('pdm.run-key') != run['run_key']
                or existing['Config']['Labels'].get('pdm.config') != config_hash(run)):
            raise DockerError('Container identity does not match the training run')
        if existing['State']['Status'] == 'created':
            start_container(name, run, docker)
        return existing['Id']
    args = ['container', 'create', '--name', name, '--pull=never',
            '--label', f"pdm.run-key={run['run_key']}",
            '--label', f"pdm.run-id={run['id']}",
            '--label', f'pdm.config={config_hash(run)}',
            '--mount', 'type=volume,src=pdm_training_artifacts,dst=/artifacts',
            '--shm-size=1g', '--memory=3g', '--cpus=2',
            '--env', 'OMP_NUM_THREADS=1', '--env', 'PYTHONUNBUFFERED=1']
    if run['device'] == 'cuda':
        args += ['--gpus', 'all']
    args += ['--entrypoint', 'torchrun', image or run.get('training_image') or os.getenv('TRAINING_IMAGE', 'pdm-training:local'),
             '--standalone', '--nnodes=1', f"--nproc-per-node={run['world_size']}", '--max-restarts=0',
             '/app/train.py', '--device', run['device'], '--epochs', str(run['epochs']),
             '--batch-size', str(run['batch_size']), '--learning-rate', str(run['learning_rate']),
             '--output', f'/artifacts/runs/{artifact_directory(run)}', '--fingerprint', run.get('dataset_fingerprint', '')]
    try:
        container_id = docker(args)
    except (DockerError, subprocess.TimeoutExpired):
        # Atomic Docker name uniqueness resolves a simultaneous retry, not check-then-create alone.
        existing = inspect_container(name, docker)
        if (existing is None or existing['Config']['Labels'].get('pdm.run-key') != run['run_key']
                or existing['Config']['Labels'].get('pdm.config') != config_hash(run)):
            raise
        container_id = existing['Id']
        if existing['State']['Status'] != 'created':
            return container_id
    try:
        start_container(name, run, docker)
    except (DockerError, subprocess.TimeoutExpired):
        existing = inspect_container(name, docker)
        if existing is None or existing['State']['Status'] not in ('running', 'exited'):
            raise
    return container_id


def stop_container(name: str, docker=docker_command) -> None:
    docker(['container', 'stop', '--time', '15', name])
    state = inspect_container(name, docker)
    if state is not None and state['State']['Running']:
        raise DockerError('Training container is still running after cancellation')


def terminal_status(state: dict) -> str:
    return 'succeeded' if state.get('ExitCode') == 0 and not state.get('OOMKilled') else 'failed'


def verify_artifacts(run: dict, name: str, docker=docker_command) -> str:
    """Check the stopped container's actual volume content, not telemetry claims."""
    root = f'/artifacts/runs/{artifact_directory(run)}'
    files = {}
    for filename in ('checkpoint.pt', 'result.json'):
        archive = docker(['container', 'cp', f'{name}:{root}/{filename}', '-'])
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            member = next((m for m in tar.getmembers() if m.isfile() and m.name.rsplit('/', 1)[-1] == filename), None)
            if member is None or not 0 < member.size <= 20 * 1024 * 1024:
                raise DockerError('Training artifact is missing, empty or exceeds the candidate artifact limit')
            files[filename] = tar.extractfile(member).read()
    result = json.loads(files['result.json'])
    if any(result.get(k) != run[k] for k in ('epochs', 'world_size', 'device')) or result.get('dataset') != run.get('dataset', 'cmapss-rul-33f-v1'):
        raise DockerError('Training artifact does not match the run manifest')
    if run.get('dataset_fingerprint') and result.get('fingerprint') != run['dataset_fingerprint']:
        raise DockerError('Training artifact has a different dataset fingerprint')
    return f'{root}/checkpoint.pt'


def parse_events(logs: str) -> list[dict]:
    events = []
    decoder = json.JSONDecoder()
    # Frame markers also recover already-recorded logs where print() joined two rank lines.
    for frame in logs.split('TRAINING_EVENT ')[1:]:
        try:
            event, _ = decoder.raw_decode(frame.lstrip())
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


class Api:
    def __init__(self, base_url: str | None = None, token: str | None = None):
        self.base_url = (base_url or os.environ['TRAINING_API_URL']).rstrip('/')
        self.token = token or os.environ['TRAINING_WORKER_TOKEN']

    def request(self, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        request = Request(self.base_url + '/api/training/gpu/internal/' + path, data=data,
                          headers={'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'})
        for attempt in range(3):
            try:
                with urlopen(request, timeout=15) as response:
                    return json.load(response)
            except HTTPError as exc:
                if exc.code < 500 or attempt == 2:
                    raise RuntimeError(f'Training API refused request ({exc.code})') from None
            except (URLError, TimeoutError):
                if attempt == 2:
                    raise RuntimeError('Training API is unavailable; retry will attach to the same container') from None
            time.sleep(attempt + 1)
        raise RuntimeError('Training API is unavailable')

    def update(self, run_id: int, event: dict) -> dict:
        return self.request(f'runs/{run_id}/events', event)


def log(message: str, level: str = 'info') -> dict:
    return {'at': datetime.now(timezone.utc).isoformat(), 'level': level, 'message': message}


def cancel_training(run_id: int, name: str, api: Api, docker=docker_command) -> None:
    existing = inspect_container(name, docker)
    if existing is not None and existing['State']['Running']:
        stop_container(name, docker)
    # No terminal callback until the stable container is absent or confirmed stopped.
    api.update(run_id, {'status': 'cancelled', 'logs': [log('Đã xác nhận toàn bộ container ngừng chạy và trả tài nguyên.')]})


def run_training(run: dict, api: Api, docker=docker_command, *, timeout_seconds: int = 3600) -> None:
    run_id, name = run['id'], container_name(run)
    current = api.request(f'runs/{run_id}')
    if current['status'] in ('cancelled', 'failed', 'succeeded'):
        return
    if current.get('cancel_requested'):
        cancel_training(run_id, name, api, docker)
        return
    claimed = api.update(run_id, {'status': 'starting', 'logs': [log('Airflow đang khởi động hoặc kết nối lại container huấn luyện.')]})
    if claimed['status'] in ('cancelled', 'failed', 'succeeded') or claimed.get('cancel_requested'):
        if claimed['status'] != 'succeeded':
            cancel_training(run_id, name, api, docker)
        return
    try:
        container_id = ensure_container(run, docker)
    except (DockerError, subprocess.TimeoutExpired) as exc:
        # A timeout or transport error can happen after Docker accepted the operation.
        # Keep host ownership active until retry reconciles the stable container identity.
        api.update(run_id, {'error': str(exc)[-2000:], 'logs': [log('Chưa xác định được trạng thái container; Airflow sẽ kết nối lại.', 'error')]})
        raise
    active = api.update(run_id, {'status': 'running', 'container_id': container_id,
                               'logs': [log('Đã kết nối container; huấn luyện RUL trên dữ liệu C-MAPSS 33 đặc trưng.')]})
    if active.get('cancel_requested') or active['status'] in ('cancelled', 'failed'):
        cancel_training(run_id, name, api, docker)
        return
    started = time.monotonic()
    event_count = 0
    completed_epochs = 0
    checkpoint = None
    completed_ranks = {}
    while True:
        current = api.request(f'runs/{run_id}')
        if current.get('cancel_requested'):
            cancel_training(run_id, name, api, docker)
            return
        if time.monotonic() - started > timeout_seconds:
            stop_container(name, docker)
            api.update(run_id, {'status': 'failed', 'error': 'Training exceeded the runtime limit.'})
            raise RuntimeError('Training exceeded the runtime limit')
        inspected = inspect_container(name, docker)
        if inspected is None:
            api.update(run_id, {'status': 'failed', 'error': 'Training container disappeared.'})
            raise DockerError('Training container disappeared')
        events = parse_events(docker(['container', 'logs', '--tail', '4096', name]))
        for event in events[event_count:]:
            # Workload uses the same bounded public event schema as the backend.
            event = {k: v for k, v in event.items() if k in ('completed_epochs', 'metrics', 'ranks', 'logs', 'artifact_path')}
            if event:
                api.update(run_id, event)
            completed_epochs = max(completed_epochs, event.get('completed_epochs', 0))
            checkpoint = event.get('artifact_path') or checkpoint
            for rank in event.get('ranks', []):
                previous = completed_ranks.get(rank['rank'], {})
                if (rank.get('epoch') or 0) >= (previous.get('epoch') or 0):
                    completed_ranks[rank['rank']] = rank
        event_count = len(events)
        state = inspected['State']
        if state['Status'] in ('exited', 'dead'):
            status = terminal_status(state)
            rank_events_complete = all(
                completed_ranks.get(rank, {}).get('epoch') == run['epochs']
                and completed_ranks.get(rank, {}).get('status') == 'succeeded'
                for rank in range(run['world_size'])
            )
            if status == 'succeeded' and (completed_epochs != run['epochs'] or not checkpoint or not rank_events_complete):
                status = 'failed'
                error = 'Container ended without a complete checkpoint, epoch metrics and final telemetry from every rank.'
            elif status == 'succeeded':
                try:
                    checkpoint = verify_artifacts(run, name, docker)
                    error = None
                except (DockerError, tarfile.TarError, ValueError) as exc:
                    status, error = 'failed', str(exc)
            else:
                error = None if status == 'succeeded' else f"Training exited with code {state.get('ExitCode')} (OOM={bool(state.get('OOMKilled'))})."
            api.update(run_id, {'status': status, 'error': error,
                                'artifact_path': checkpoint if status == 'succeeded' else None,
                                'logs': [log('Huấn luyện hoàn tất; checkpoint đã được lưu.' if status == 'succeeded' else error,
                                             'info' if status == 'succeeded' else 'error')]})
            if status == 'failed':
                raise RuntimeError(error)
            return
        time.sleep(1)


def execute_dispatch(dispatch_key: str, run_id: int | None, airflow_run_id: str) -> dict:
    api = Api()
    response = api.request('dispatch', {'dispatch_key': dispatch_key, 'run_id': run_id, 'airflow_run_id': airflow_run_id})
    if response.get('run'):
        run = response['run']
        if run.get('execution_mode', 'local') != 'local':
            raise ValueError('Only local container training is supported')
        from prepare import dataset, Cancelled
        try:
            if not run.get('cancel_requested'):
                snapshot = dataset(run, api)
                run['dataset_path'] = snapshot['path']
                run['dataset_fingerprint'] = snapshot['fingerprint']
            run_training(run, api)
        except Cancelled:
            cancel_training(run['id'], container_name(run), api)
        except (ValueError, RuntimeError, OSError) as error:
            # Only release ownership if no workload exists; uncertain Docker work stays recoverable.
            existing = inspect_container(container_name(run))
            if existing is None or not existing['State']['Running']:
                api.update(run['id'], {'status': 'failed', 'error': str(error)[-3000:]})
            raise
        return {'run_id': run['id']}
    return {'run_id': None}
