import hmac
import os
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import Field

from . import runtime
from .schemas import ConfigIn, DispatchIn, EventsIn, RunIn, Strict
from .service import Conflict, Store


class PipelineIn(Strict):
    request_id: UUID
    fraction: float = Field(default=1, gt=0, le=1)
    max_failed: int = Field(default=500, ge=1, le=5000)


def worker(authorization: str = Header('')):
    token = os.getenv('TRAINING_WORKER_TOKEN', '')
    if not token:
        raise HTTPException(503, 'Chưa cấu hình bộ thực thi training')
    if not hmac.compare_digest(authorization, 'Bearer '+token):
        raise HTTPException(401, 'Token bộ thực thi không hợp lệ')


def router():
    api = APIRouter(prefix='/api/training', tags=['training'])
    state = None

    def public(run):
        return {**run, 'nodes': [{k:v for k,v in n.items() if k != 'credential_ref'} for n in run.get('nodes', [])]}

    def store():
        nonlocal state
        if state is None:
            state = Store(Path(os.getenv('STATE_DIR', '/state'))/'training.sqlite3')
        return state

    def call(fn, *args):
        try:
            return fn(*args)
        except KeyError as error:
            raise HTTPException(404, str(error)) from None
        except Conflict as error:
            raise HTTPException(409, str(error)) from None
        except ValueError as error:
            raise HTTPException(422, str(error)) from None
        except runtime.Unavailable as error:
            raise HTTPException(503, str(error)) from None

    @api.get('/overview')
    def overview():
        return runtime.overview()

    @api.get('/runs')
    def pipeline_runs():
        return call(runtime.airflow, '/dags/retrain_rul/dagRuns?limit=20&order_by=-run_after')

    @api.post('/runs')
    def pipeline_start(body: PipelineIn):
        return call(runtime.trigger, 'retrain_rul', 'manual__ui_'+str(body.request_id), body.model_dump(mode='json', exclude={'request_id'}))

    @api.get('/runs/{dag_run_id}/tasks')
    def pipeline_tasks(dag_run_id: str):
        from urllib.parse import quote
        return call(runtime.airflow, '/dags/retrain_rul/dagRuns/'+quote(dag_run_id, safe='')+'/taskInstances?limit=100')

    @api.get('/gpu/system')
    def system():
        return runtime.system()

    @api.get('/gpu/config')
    def config():
        return store().config()

    @api.put('/gpu/config')
    def save_config(body: ConfigIn):
        return call(store().save_config, body, runtime.system())

    @api.get('/gpu/runs')
    def runs(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)):
        value = store().list(page, page_size)
        value['items'] = [public(run) for run in value['items']]
        return value

    @api.post('/gpu/runs', status_code=201)
    def create(body: RunIn):
        run, fresh = call(store().manual, body, runtime.system())
        if fresh:
            try:
                runtime.trigger(runtime.DAG, run['airflow_run_id'], {'run_id': run['id']})
            except runtime.Unavailable as error:
                if error.uncertain:
                    store().events(run['id'], EventsIn(error=str(error)))
                else:
                    store().events(run['id'], EventsIn(status='failed', error=str(error)))
                raise HTTPException(503, str(error)) from None
        return public(store().get(run['id']))

    @api.get('/gpu/runs/{run_id}')
    def get_run(run_id: int):
        return public(call(store().get, run_id))

    @api.post('/gpu/runs/{run_id}/cancel')
    def cancel(run_id: int):
        return public(call(store().cancel, run_id))

    @api.post('/gpu/internal/dispatch', dependencies=[Depends(worker)])
    def dispatch(body: DispatchIn):
        return {'run': call(store().dispatch, body.dispatch_key, body.run_id, body.airflow_run_id, runtime.system())}

    @api.get('/gpu/internal/runs/{run_id}', dependencies=[Depends(worker)])
    def internal_get(run_id: int):
        return call(store().get, run_id)

    @api.post('/gpu/internal/runs/{run_id}/events', dependencies=[Depends(worker)])
    def events(run_id: int, body: EventsIn):
        return call(store().events, run_id, body)

    return api
