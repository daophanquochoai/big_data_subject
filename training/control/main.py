import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from .router import router
from .service import Store


def build():
    state = Path(os.getenv('STATE_DIR', '/state'))/'training.sqlite3'
    Store(state)
    app = FastAPI(title='PDM Training Control')
    app.include_router(router())
    static = Path('/training/static')
    if static.is_dir(): app.mount('/', StaticFiles(directory=static, html=True), name='ui')
    return app
