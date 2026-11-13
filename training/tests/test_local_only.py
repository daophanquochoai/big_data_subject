import json
from uuid import uuid4

import pytest

from control.schemas import RunIn, ConfigIn
from control.service import Store, Conflict
from migrate_state import migrate

HW = {'cpu_count':12,'gpu_count':1,'docker_available':True,'image_available':True,'training_image_id':'sha256:test'}


def legacy(store, mode):
    with store.transaction() as db:
        cfg=json.loads(db.execute('SELECT value FROM config WHERE id=1').fetchone()[0])
        cfg.update(execution_mode=mode,node_ids=[1,2],ranks_per_node=1,rdzv_port=29500,enabled=True)
        db.execute('UPDATE config SET value=? WHERE id=1',(json.dumps(cfg),))


def test_old_config_becomes_local_without_deleting_history(tmp_path):
    store=Store(tmp_path/'state.sqlite3')
    run,_=store.manual(RunIn(request_id=uuid4()),HW)
    store.cancel(run['id'])
    legacy(store,'local')
    reopened=Store(store.path)
    cfg=reopened.config()
    assert cfg['enabled'] and not {'execution_mode','node_ids','ranks_per_node','rdzv_port'} & cfg.keys()
    assert reopened.get(run['id'])['status']=='cancelled'


def test_old_remote_schedule_is_disabled_and_cannot_dispatch_locally(tmp_path):
    store=Store(tmp_path/'state.sqlite3')
    run,_=store.manual(RunIn(request_id=uuid4()),HW)
    with store.transaction() as db:
        run['execution_mode']='ssh'
        db.execute('UPDATE runs SET value=? WHERE id=?',(json.dumps(run),run['id']))
    legacy(store,'ssh')
    reopened=Store(store.path)
    cfg=reopened.config()
    assert not cfg['enabled'] and cfg['next_run_at'] is None and cfg['device']=='cpu'
    with pytest.raises(Conflict): reopened.dispatch('tick',run['id'],'tick',HW)
    assert reopened.get(run['id'])['status']=='queued'


def test_removed_routes_are_not_exposed_and_remote_config_is_rejected(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from control.main import build
    from control import runtime
    monkeypatch.setenv('STATE_DIR',str(tmp_path))
    monkeypatch.setattr(runtime,'system',lambda:HW)
    with TestClient(build()) as client:
        for path in ('/api/training/nodes','/api/training/gpu/internal/runs/1/nodes/1/credential'):
            assert client.get(path).status_code==404
        assert client.post('/api/training/nodes',json={}).status_code==404
        assert client.put('/api/training/gpu/config',json={**ConfigIn().model_dump(),'execution_mode':'ssh'}).status_code==422


def test_migration_preserves_history_without_overwriting(tmp_path):
    source=Store(tmp_path/'source.sqlite3')
    run,_=source.manual(RunIn(request_id=uuid4()),HW)
    target=tmp_path/'target.sqlite3'
    migrate(source.path,target)
    copied=Store(target)
    assert copied.get(run['id'])['run_key']==run['run_key']
    copied.cancel(run['id'])
    migrate(source.path,target)
    assert copied.get(run['id'])['status']=='cancelled'
