"""Ứng dụng FastAPI. Chạy: uvicorn --factory app.main:build"""
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api import router
from app.kafka_sink import KafkaSink
from app.runner import Runner
from engine.data import load_dataset
from engine.generators import parametric_model

STATIC = Path(__file__).resolve().parents[1] / "static"  # bản build của FE, chép vào lúc build image


def create_app(runner, run_loop=False):
    @asynccontextmanager
    async def lifespan(_app):
        stop = threading.Event()
        if run_loop:
            threading.Thread(target=runner.loop, args=(stop,), name="runner", daemon=True).start()
        yield
        stop.set()

    app = FastAPI(title="Bộ giả lập cảm biến", lifespan=lifespan)
    app.include_router(router(runner))
    if STATIC.is_dir():
        app.mount("/", StaticFiles(directory=STATIC, html=True), name="ui")
    return app


def build():
    logging.basicConfig(level=logging.INFO)
    # Có bảng chia nhóm của phần Spark thì chỉ phát lại nhóm phát lại, để mô hình không "thấy trước" dữ liệu luồng.
    groups = Path(os.environ.get("GROUPS_FILE", "/models/norm_stats/groups.json"))
    machines = set(json.loads(groups.read_text())["replay"]) if groups.exists() else None
    logging.info("nguồn: %s", "mọi Máy train" if machines is None else f"{len(machines)} Máy của nhóm phát lại")
    ds = load_dataset(os.environ.get("DATA_DIR", "/data/raw/cmapss"), machines)
    parametric_model(ds)  # khớp một lần lúc khởi động, không để máy parametric đầu tiên làm chậm một nhịp
    sink = KafkaSink(os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    sink.ensure_topics()
    return create_app(Runner(ds, sink, os.environ.get("STATE_DIR", "/state")), run_loop=True)
