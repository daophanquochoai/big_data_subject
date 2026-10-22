"""REST và WebSocket điều khiển bộ giả lập."""
import asyncio
import json
import re

import numpy as np
from fastapi import APIRouter, HTTPException, Response, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from engine.config import Config, Method, Subset
from engine.data import COLUMNS, INFORMATIVE
from engine.generators import generate

PAGE_SIZE = 50
SCENARIO_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}")  # chặn đường dẫn lạ trong tên file
SORTABLE = {"id", "method", "cycle", "life", "rul"}


def merge(current, patch):
    """PATCH: trường lồng (faults, method_weights) gộp một tầng, trường khác thay nguyên."""
    out = dict(current)
    for k, v in patch.items():
        out[k] = {**out[k], **v} if isinstance(out.get(k), dict) and isinstance(v, dict) else v
    return out


class PreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: Method
    subset: Subset
    seed: int = Field(0, ge=0)
    noise: float | None = Field(None, ge=0, le=1)  # mặc định: mức nhiễu đang dùng
    warp_range: tuple[float, float] | None = None   # mặc định: khoảng đang dùng


def validated(data):
    """Config từ dict, hoặc 422 kèm từng trường sai để FE tô đỏ đúng ô."""
    try:
        return Config.model_validate(data)
    except ValidationError as e:
        raise HTTPException(422, [{"loc": [str(p) for p in err["loc"]], "msg": err["msg"]} for err in e.errors()])


def router(runner):
    api = APIRouter()

    @api.get("/api/config")
    def get_config():
        return runner.config.model_dump()

    @api.patch("/api/config")
    def patch_config(body: dict):
        with runner.config_lock:
            cfg = validated(merge(runner.config.model_dump(), body))
            runner.update(cfg)
        return cfg.model_dump()

    @api.get("/api/status")
    def status():
        return {"running": runner.running, "kafka_ok": runner.sink.ok}

    @api.post("/api/start")
    def start():
        runner.start()
        return status()

    @api.post("/api/stop")
    def stop():
        runner.stop()
        return status()

    @api.post("/api/reset")
    def reset():
        runner.reset()
        return status()

    @api.get("/api/machines")
    def machines(method: str | None = None, sort: str = "id", desc: bool = False, page: int = 1):
        if sort not in SORTABLE:
            raise HTTPException(422, [{"loc": ["sort"], "msg": f"chỉ sắp được theo {sorted(SORTABLE)}"}])
        items = [m for m in runner.machines() if method is None or m["method"] == method]
        items.sort(key=lambda m: m[sort], reverse=desc)
        start = (max(page, 1) - 1) * PAGE_SIZE
        return {"total": len(items), "page_size": PAGE_SIZE, "items": items[start:start + PAGE_SIZE]}

    @api.get("/api/machines/{machine_id}")
    def machine(machine_id: str):
        m = runner.machine(machine_id)
        if m is None:
            raise HTTPException(404, "Máy ảo không còn sống")
        return {"id": m.id, "method": m.life.method, "base": m.life.base, "params": m.life.params,
                "cycle": m.pos, "columns": COLUMNS, "lifecycle": m.life.data.tolist()}

    @api.post("/api/preview")
    def preview(req: PreviewRequest):
        """Sinh thử một máy bằng nguồn ngẫu nhiên riêng; không đụng đội máy đang chạy."""
        update = {"subsets": (req.subset,)}
        if req.warp_range is not None:
            update["warp_range"] = req.warp_range
        cfg = validated(merge(runner.config.model_dump(), update))
        rng = np.random.default_rng(req.seed)
        life = generate(req.method, runner.ds, rng, cfg)
        data = life.data.copy()
        noise = cfg.noise if req.noise is None else req.noise
        data[:, INFORMATIVE] += rng.standard_normal((len(data), len(INFORMATIVE))) * runner.ds.sigma_rows(data) * noise
        return {"method": life.method, "base": life.base, "params": life.params, "columns": COLUMNS,
                "generated": data.tolist(), "sources": {b: runner.ds.sources[b].tolist() for b in life.base}}

    scenarios = runner.state / "scenarios"
    scenarios.mkdir(exist_ok=True)

    def scenario_path(name, must_exist=True):
        if not SCENARIO_NAME.fullmatch(name):
            raise HTTPException(422, [{"loc": ["name"], "msg": "tên chỉ gồm chữ, số, - và _, tối đa 64 ký tự"}])
        path = scenarios / f"{name}.json"
        if must_exist and not path.exists():
            raise HTTPException(404, "không có kịch bản này")
        return path

    @api.get("/api/scenarios")
    def list_scenarios():
        return sorted(p.stem for p in scenarios.glob("*.json"))

    @api.get("/api/scenarios/{name}")
    def get_scenario(name: str):
        return json.loads(scenario_path(name).read_text(encoding="utf-8"))

    @api.put("/api/scenarios/{name}")
    def save_scenario(name: str):
        """Lưu cấu hình hiện hành."""
        cfg = runner.config.model_dump(mode="json")
        scenario_path(name, must_exist=False).write_text(json.dumps(cfg, indent=1), encoding="utf-8")
        return cfg

    @api.delete("/api/scenarios/{name}", status_code=204)
    def delete_scenario(name: str):
        scenario_path(name).unlink()
        return Response(status_code=204)

    @api.post("/api/scenarios/{name}/load")
    def load_scenario(name: str):
        """Thay cấu hình rồi tạo lại đội máy, để cùng kịch bản cho cùng chuỗi Bản tin."""
        with runner.config_lock:
            cfg = validated(json.loads(scenario_path(name).read_text(encoding="utf-8")))
            runner.update(cfg)
            runner.reset()
        return cfg.model_dump()

    @api.websocket("/ws/machines/{machine_id}")
    async def ws_machine(ws: WebSocket, machine_id: str):
        """Đẩy từng Bản tin của một máy ngay khi phát, kể cả Bản tin bị chèn lỗi."""
        await ws.accept()
        buf, last = runner.watch(machine_id), -1
        try:
            while True:
                fresh = [(seq, m) for seq, m in list(buf) if seq > last]  # chụp một lần, không đọc lại buf
                for seq, m in fresh:
                    await ws.send_json(m)
                    last = seq
                if not fresh and runner.machine(machine_id) is None:
                    break
                await asyncio.sleep(0.2)
            await ws.send_json({"machine_id": machine_id, "gone": True})  # máy đã hỏng hoặc bị bớt
            await ws.close()
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            runner.unwatch(machine_id, buf)

    @api.websocket("/ws/stats")
    async def ws_stats(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                await ws.send_json(runner.stats)
                await asyncio.sleep(1)
        except (WebSocketDisconnect, RuntimeError):
            pass

    return api
