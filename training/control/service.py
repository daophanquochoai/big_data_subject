"""Durable local training state, independent of Spark model pointers."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

from .schemas import ConfigIn

ACTIVE = ('queued', 'starting', 'running')
TERMINAL = ('succeeded', 'failed', 'cancelled')
VN = timezone(timedelta(hours=7), 'Asia/Ho_Chi_Minh')
FIELDS = ('device', 'world_size', 'epochs', 'batch_size', 'learning_rate', 'fraction')


class Conflict(ValueError):
    pass


def now():
    return datetime.now(timezone.utc)


def next_time(schedule, at):
    local = at.astimezone(VN)
    if schedule == '*/5 * * * *':
        return local.replace(second=0, microsecond=0) + timedelta(minutes=5 - local.minute % 5)
    if schedule == '0 * * * *':
        return local.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    value = local.replace(hour=2, minute=0, second=0, microsecond=0)
    return value if value > local else value + timedelta(days=1)


def capacity(values, hardware):
    if values['device'] == 'cuda' and values['world_size'] > hardware['gpu_count']:
        raise ValueError(f"Máy có {hardware['gpu_count']} GPU, không đủ số rank được yêu cầu")
    if values['device'] == 'cpu' and values['world_size'] > hardware['cpu_count']:
        raise ValueError('Số rank vượt số CPU khả dụng')


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, run_key TEXT UNIQUE NOT NULL, status TEXT NOT NULL, value TEXT NOT NULL)')
            # shortcut: one active job globally; add resource leases when concurrent jobs are required.
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS active_host ON runs ((1)) WHERE status IN ('queued','starting','running')")
            default = {**ConfigIn().model_dump(), 'next_run_at': None}
            db.execute('INSERT OR IGNORE INTO config VALUES (1,?)', (json.dumps(default),))
            old = json.loads(db.execute('SELECT value FROM config WHERE id=1').fetchone()[0])
            config = {key: old.get(key, value) for key, value in default.items()}
            if old.get('execution_mode', 'local') != 'local':
                config.update(enabled=False, next_run_at=None, device='cpu', world_size=2)
            db.execute('UPDATE config SET value=? WHERE id=1', (json.dumps(config),))

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _get(self, db, run_id):
        row = db.execute('SELECT value FROM runs WHERE id=?', (run_id,)).fetchone()
        if row is None:
            raise KeyError('Không tìm thấy lượt huấn luyện')
        return json.loads(row[0])

    def _save(self, db, run):
        run['progress'] = 100 * run['completed_epochs'] / run['epochs']
        db.execute('UPDATE runs SET status=?, value=? WHERE id=?', (run['status'], json.dumps(run, allow_nan=False), run['id']))
        return run

    def _active(self, db):
        row = db.execute("SELECT value FROM runs WHERE status IN ('queued','starting','running') LIMIT 1").fetchone()
        return json.loads(row[0]) if row else None

    def _create(self, db, key, source, cfg, hardware, at):
        capacity(cfg, hardware)
        if not hardware['docker_available'] or not hardware['image_available']:
            raise ValueError('Docker hoặc image huấn luyện chưa sẵn sàng')
        values = {k: cfg[k] for k in FIELDS}
        run = {**values, 'execution_mode': 'local', 'run_key': key, 'name': f"RUL C-MAPSS · {at.astimezone(VN):%d/%m %H:%M}",
               'source': source, 'status': 'queued', 'training_image': hardware['training_image_id'],
               'dataset': 'cmapss-rul-33f-v1', 'completed_epochs': 0, 'progress': 0,
               'created_at': at.isoformat(), 'started_at': None, 'finished_at': None,
               'airflow_run_id': None, 'container_id': None, 'artifact_path': None, 'error': None,
               'cancel_requested': False, 'metrics': [], 'logs': [],
               'ranks': [{'rank': i, 'device': f'cuda:{i}' if cfg['device'] == 'cuda' else 'cpu', 'status': 'queued', 'epoch': 0} for i in range(values['world_size'])]}
        row = db.execute('INSERT INTO runs (run_key,status,value) VALUES (?,?,?)', (key, 'queued', '{}'))
        run['id'] = row.lastrowid
        run['airflow_run_id'] = f"manual__pdm_gpu_{run['id']}" if source == 'manual' else None
        return self._save(db, run)

    def config(self):
        with self.transaction() as db:
            return json.loads(db.execute('SELECT value FROM config WHERE id=1').fetchone()[0])

    def save_config(self, body, hardware, at=None):
        at = at or now()
        values = body.model_dump()
        capacity(values, hardware)
        with self.transaction() as db:
            old = json.loads(db.execute('SELECT value FROM config WHERE id=1').fetchone()[0])
            changed = old['schedule'] != values['schedule'] or not old['enabled']
            values['next_run_at'] = (next_time(values['schedule'], at).isoformat() if changed or not old['next_run_at'] else old['next_run_at']) if values['enabled'] else None
            db.execute('UPDATE config SET value=? WHERE id=1', (json.dumps(values),))
        return values

    def manual(self, body, hardware, at=None):
        at = at or now()
        key = f'manual:{body.request_id}'
        with self.transaction() as db:
            row = db.execute('SELECT value FROM runs WHERE run_key=?', (key,)).fetchone()
            if row:
                return json.loads(row[0]), False
            if self._active(db):
                raise Conflict('Một lượt train đang giữ máy. Chờ hoàn tất hoặc hủy lượt hiện tại.')
            cfg = json.loads(db.execute('SELECT value FROM config WHERE id=1').fetchone()[0])
            cfg.update(body.model_dump(exclude_none=True, exclude={'request_id'}))
            return self._create(db, key, 'manual', cfg, hardware, at), True

    def get(self, run_id):
        with self.transaction() as db:
            return self._get(db, run_id)

    def list(self, page=1, page_size=20):
        with self.transaction() as db:
            rows = db.execute('SELECT value FROM runs ORDER BY id DESC LIMIT ? OFFSET ?', (page_size, (page-1)*page_size)).fetchall()
            return {'items': [json.loads(row[0]) for row in rows], 'total': db.execute('SELECT count(*) FROM runs').fetchone()[0], 'page': page, 'page_size': page_size}

    def dispatch(self, key, run_id, dag_run, hardware, at=None):
        at = at or now()
        with self.transaction() as db:
            if run_id is not None:
                run = self._get(db, run_id)
            else:
                row = db.execute('SELECT value FROM runs WHERE run_key=?', ('airflow:'+key,)).fetchone()
                if row:
                    run = json.loads(row[0])
                else:
                    run = self._active(db)
                    if run is None:
                        cfg = json.loads(db.execute('SELECT value FROM config WHERE id=1').fetchone()[0])
                        if not cfg['enabled'] or not cfg['next_run_at'] or datetime.fromisoformat(cfg['next_run_at']) > at:
                            return None
                        run = self._create(db, 'airflow:'+key, 'airflow', cfg, hardware, at)
                        cfg['next_run_at'] = next_time(cfg['schedule'], at).isoformat()
                        db.execute('UPDATE config SET value=? WHERE id=1', (json.dumps(cfg),))
            if run['status'] in TERMINAL:
                return None
            if run.get('execution_mode', 'local') != 'local':
                raise Conflict('Lượt chạy cũ dùng chế độ đã được gỡ; không thể khởi chạy lại')
            if run['status'] == 'queued':
                run['status'], run['started_at'] = 'starting', at.isoformat()
            run['airflow_run_id'] = dag_run
            return self._save(db, run)

    def cancel(self, run_id, at=None):
        at = at or now()
        with self.transaction() as db:
            run = self._get(db, run_id)
            if run['status'] not in TERMINAL:
                run['cancel_requested'] = True
                if run['status'] == 'queued':
                    run['status'], run['finished_at'] = 'cancelled', at.isoformat()
                    run['ranks'] = [{**rank, 'status': 'cancelled'} for rank in run['ranks']]
            return self._save(db, run)

    def events(self, run_id, body, at=None):
        at = at or now()
        with self.transaction() as db:
            run = self._get(db, run_id)
            if run['status'] in TERMINAL:
                return run
            if (body.completed_epochs or 0) > run['epochs'] or any(m.epoch > run['epochs'] for m in body.metrics):
                raise ValueError('Epoch vượt cấu hình')
            if any(r.rank >= run['world_size'] or (r.epoch or 0) > run['epochs'] for r in body.ranks):
                raise ValueError('Rank không thuộc lượt train')
            if body.container_id:
                if run['container_id'] and run['container_id'] != body.container_id:
                    raise Conflict('Container không khớp lượt train')
                run['container_id'] = body.container_id
            if body.status:
                order = {'queued': 0, 'starting': 1, 'running': 2, 'succeeded': 3, 'failed': 3, 'cancelled': 3}
                status = 'cancelled' if run['cancel_requested'] and body.status in TERMINAL else body.status
                if order[status] >= order[run['status']]:
                    run['status'] = status
                    if status in TERMINAL:
                        run['finished_at'] = at.isoformat()
            run['completed_epochs'] = max(run['completed_epochs'], body.completed_epochs or 0)
            metrics = {m['epoch']: m for m in run['metrics']}
            metrics.update({m.epoch: m.model_dump(mode='json') for m in body.metrics})
            run['metrics'] = [metrics[k] for k in sorted(metrics)]
            ranks = {r['rank']: r for r in run['ranks']}
            for rank in body.ranks:
                if (rank.epoch or 0) >= (ranks.get(rank.rank, {}).get('epoch') or 0):
                    ranks[rank.rank] = rank.model_dump(mode='json')
            if run['status'] in TERMINAL:
                ranks = {k: {**r, 'status': run['status']} for k, r in ranks.items()}
            run['ranks'] = [ranks[k] for k in sorted(ranks)]
            logs = {(v['at'], v['level'], v['message']): v for v in run['logs']}
            for entry in body.logs:
                value = entry.model_dump(mode='json')
                logs[(value['at'], value['level'], value['message'])] = value
            run['logs'] = sorted(logs.values(), key=lambda entry: entry['at'])[-500:]
            if 'error' in body.model_fields_set:
                run['error'] = body.error
            if body.artifact_path:
                run['artifact_path'] = body.artifact_path
            return self._save(db, run)
