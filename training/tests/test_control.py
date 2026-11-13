from datetime import datetime, timezone, timedelta
from uuid import uuid4

import pytest

from control.service import Store, Conflict
from control.schemas import RunIn, EventsIn, ConfigIn

NOW = datetime(2026, 11, 13, 1, 0, tzinfo=timezone.utc)
HW = {'cpu_count': 12, 'gpu_count': 1, 'docker_available': True, 'image_available': True, 'training_image_id': 'sha256:test'}


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / 'training.sqlite3')


def create(store, key=None):
    return store.manual(RunIn(request_id=key or uuid4()), HW, NOW)[0]


def test_manual_request_is_idempotent_and_host_exclusive(store):
    key = uuid4()
    a, first = store.manual(RunIn(request_id=key), HW, NOW)
    b, second = store.manual(RunIn(request_id=key), HW, NOW)
    assert first and not second and a['id'] == b['id']
    with pytest.raises(Conflict):
        create(store)


def test_cancel_before_claim_cannot_start(store):
    run = create(store)
    assert store.cancel(run['id'], NOW)['status'] == 'cancelled'
    assert store.dispatch('tick', run['id'], 'dag', HW, NOW) is None


def test_active_cancel_is_recovered_even_when_schedule_disabled(store):
    run = create(store)
    store.dispatch('manual', run['id'], 'manual', HW, NOW)
    store.cancel(run['id'], NOW)
    recovered = store.dispatch('next-tick', None, 'next-tick', HW, NOW)
    assert recovered['id'] == run['id'] and recovered['cancel_requested']
    with pytest.raises(Conflict):
        create(store)
    store.events(run['id'], EventsIn(status='cancelled'), NOW)
    assert create(store)['id'] != run['id']


def test_terminal_events_cannot_revive_run(store):
    run = create(store)
    store.events(run['id'], EventsIn(status='failed', error='failure'), NOW)
    result = store.events(run['id'], EventsIn(status='running'), NOW)
    assert result['status'] == 'failed'


def test_schedule_waits_for_real_due_time_and_recovers_ownership(store):
    cfg = ConfigIn(enabled=True, schedule='*/5 * * * *')
    saved = store.save_config(cfg, HW, NOW)
    assert datetime.fromisoformat(saved['next_run_at']) == NOW + timedelta(minutes=5)
    assert store.dispatch('early', None, 'early', HW, NOW) is None
    run = store.dispatch('due', None, 'due', HW, NOW + timedelta(minutes=5))
    assert run['source'] == 'airflow' and run['dataset'] == 'cmapss-rul-33f-v1'
    assert store.dispatch('recover', None, 'recover', HW, NOW + timedelta(minutes=6))['id'] == run['id']


def test_gpu_request_cannot_invent_devices(store):
    with pytest.raises(ValueError, match='GPU'):
        store.manual(RunIn(request_id=uuid4(), device='cuda', world_size=2), HW, NOW)


def test_metrics_merge_and_validate_rank_boundaries(store):
    run = create(store)
    body = EventsIn(completed_epochs=1, metrics=[{'epoch': 1, 'loss': .2, 'accuracy': .8, 'elapsed_seconds': 1}])
    store.events(run['id'], body, NOW)
    assert len(store.events(run['id'], body, NOW)['metrics']) == 1
    with pytest.raises(ValueError):
        store.events(run['id'], EventsIn(ranks=[{'rank': 3, 'device': 'cpu', 'status': 'running'}]), NOW)


def test_store_survives_new_instance(store):
    run = create(store)
    assert Store(store.path).get(run['id'])['training_image'] == 'sha256:test'


def test_http_manual_retry_submits_once_and_enforces_capacity(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from control import runtime
    from control.router import router
    monkeypatch.setenv('STATE_DIR', str(tmp_path))
    monkeypatch.setattr(runtime, 'system', lambda: HW)
    submitted = []
    monkeypatch.setattr(runtime, 'trigger', lambda *args: submitted.append(args))
    app = FastAPI()
    app.include_router(router())
    with TestClient(app) as client:
        body = {'request_id': str(uuid4())}
        first = client.post('/api/training/gpu/runs', json=body)
        assert first.status_code == 201
        retry = client.post('/api/training/gpu/runs', json=body)
        assert retry.json()['id'] == first.json()['id'] and len(submitted) == 1
        assert client.post('/api/training/gpu/runs', json={'request_id': str(uuid4())}).status_code == 409
        client.post(f"/api/training/gpu/runs/{first.json()['id']}/cancel")
        invalid = {**body, 'request_id': str(uuid4()), 'device': 'cuda', 'world_size': 2}
        assert client.post('/api/training/gpu/runs', json=invalid).status_code == 422


def test_http_worker_events_require_internal_token(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from control import runtime
    from control.router import router
    monkeypatch.setenv('STATE_DIR', str(tmp_path))
    monkeypatch.setenv('TRAINING_WORKER_TOKEN', 'test-worker-secret')
    monkeypatch.setattr(runtime, 'system', lambda: HW)
    app = FastAPI()
    app.include_router(router())
    with TestClient(app) as client:
        body = {'dispatch_key': 'tick', 'airflow_run_id': 'tick'}
        assert client.post('/api/training/gpu/internal/dispatch', json=body).status_code == 401
        response = client.post('/api/training/gpu/internal/dispatch', json=body, headers={'Authorization': 'Bearer test-worker-secret'})
        assert response.status_code == 200 and response.json()['run'] is None
