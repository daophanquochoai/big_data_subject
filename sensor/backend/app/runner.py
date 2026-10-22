"""Luồng phát: mỗi 100 ms một nhịp của đội máy, gửi Bản tin, cập nhật số liệu mỗi giây."""
import logging
import itertools
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from engine.config import Config
from engine.fleet import TICKS_PER_SECOND, Fleet

log = logging.getLogger(__name__)
TICK = 1 / TICKS_PER_SECOND


def iso(t):
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Runner:
    def __init__(self, ds, sink, state_dir):
        self.ds, self.sink = ds, sink
        self.state = Path(state_dir)
        self.state.mkdir(parents=True, exist_ok=True)
        self.config = Config()
        self.running = False
        self.lock = threading.Lock()         # giữ đội máy và bộ đếm nhất quán giữa luồng phát và API
        self.config_lock = threading.Lock()  # đọc-gộp-thay cấu hình từ API là một bước, không ghi đè lẫn nhau
        self.stats = {}
        self.watched = {}  # mã máy -> các bộ đệm, mỗi người đang xem một bộ đệm (thứ tự, Bản tin)
        self._seq = itertools.count()
        self.reset()

    # Mã Máy ảo không lặp lại qua các lần tạo lại hay khởi động lại: số kế tiếp nằm trên volume.
    def _read_next_id(self):
        try:
            return int((self.state / "next_id").read_text())
        except FileNotFoundError:
            return 1

    def _save_next_id(self):
        self.saved_id = self.fleet.next_id
        (self.state / "next_id").write_text(str(self.saved_id))

    def update(self, cfg):
        """Thay nguyên đối tượng cấu hình; luồng phát đọc nó ở đầu mỗi nhịp. Số máy áp ngay, kể cả khi đang dừng."""
        with self.lock:
            self.config = cfg
            self.fleet.resize(cfg)
            if self.fleet.next_id != self.saved_id:
                self._save_next_id()

    def start(self):
        self.running = True

    def stop(self):
        self.running = False

    def reset(self):
        with self.lock:
            self.running = False
            cfg = self.config
            self.fleet = Fleet(self.ds, cfg, np.random.default_rng(cfg.seed), self._read_next_id())
            self._save_next_id()
            self.counters = Counter({k: 0 for k in ("sent", "failed", "backpressure_dropped", "overruns")})
            self._last_sample = (time.monotonic(), 0, self.sink.acked, self.sink.errors)
        self._update_stats()

    def step(self, now):
        cfg = self.config
        with self.lock:
            if not self.running:
                self.sink.poll()  # vẫn nhận báo xác nhận của các Bản tin còn trong hàng đợi
                return
            messages, failures = self.fleet.tick(cfg, iso(now))
            counters = self.counters  # tạo lại giữa chừng thì phần còn lại của nhịp này không lọt vào bộ đếm mới
            # Ghi trước khi gửi: máy vừa sinh chưa phát Bản tin nào, nên dừng ở đâu cũng không dùng lại mã.
            if self.fleet.next_id != self.saved_id:
                self._save_next_id()
        for m in messages:
            for buf in tuple(self.watched.get(m["machine_id"], ())):
                buf.append((next(self._seq), m))
        out = [("machine.failed", f) for f in failures] + [("sensor.raw", m) for m in messages]
        for i, (topic, value) in enumerate(out):
            if not self.sink.send(topic, value["machine_id"], value):
                counters["backpressure_dropped"] += len(out) - i
                break
            counters["sent"] += 1  # cả hai topic, để so được với số Kafka xác nhận
        counters["failed"] += len(failures)
        self.sink.poll()

    def _update_stats(self):
        with self.lock:
            now = time.monotonic()
            then, sent, acked, errors = self._last_sample
            dt = max(now - then, 1e-9)
            self._last_sample = (now, self.counters["sent"], self.sink.acked, self.sink.errors)
            self.stats = self._stats(dt, sent, acked, errors)

    def _stats(self, dt, sent, acked, errors):
        return {
            "running": self.running,
            # Thư viện Kafka chỉ báo lỗi kết nối một lần rồi lặng lẽ thử lại: còn Bản tin chờ mà không được
            # xác nhận cái nào trong cả giây thì cũng coi là Kafka hỏng.
            "kafka_ok": self.sink.ok and (self.sink.acked > acked or self.sink.queue_len() == 0),
            "sent_per_s": round((self.counters["sent"] - sent) / dt),
            "acked_per_s": round((self.sink.acked - acked) / dt),
            "alive": len(self.fleet.machines),
            "failed": self.counters["failed"],
            "faults": dict(self.fleet.fault_counts),
            "kafka_errors_per_s": round((self.sink.errors - errors) / dt),  # về 0 khi Kafka trở lại
            "backpressure_dropped": self.counters["backpressure_dropped"],
            "overruns": self.counters["overruns"],
            "queue": self.sink.queue_len(),
        }

    def watch(self, machine_id):
        buf = deque(maxlen=1000)
        self.watched[machine_id] = (*self.watched.get(machine_id, ()), buf)
        return buf

    def unwatch(self, machine_id, buf):
        rest = tuple(b for b in self.watched.get(machine_id, ()) if b is not buf)
        if rest:
            self.watched[machine_id] = rest
        else:
            self.watched.pop(machine_id, None)

    def machine(self, machine_id):
        with self.lock:
            return next((m for m in self.fleet.machines if m.id == machine_id), None)

    def machines(self):
        with self.lock:
            return [{"id": m.id, "method": m.life.method, "base": m.life.base, "params": m.life.params,
                     "cycle": m.pos, "life": len(m.life.data), "rul": len(m.life.data) - m.pos}
                    for m in self.fleet.machines]

    def loop(self, stop_event):
        """Chạy trong luồng riêng: nhịp căn theo đồng hồ, số liệu cập nhật mỗi giây."""
        next_tick, tick = time.monotonic(), 0
        while not stop_event.is_set():
            try:
                self.step(datetime.now(timezone.utc))
            except Exception:
                log.exception("nhịp %d lỗi", tick)
            tick += 1
            if tick % TICKS_PER_SECOND == 0:
                self._update_stats()
            next_tick += TICK
            late = time.monotonic() - next_tick
            if late > 0:
                if self.running:
                    self.counters["overruns"] += 1
                next_tick = time.monotonic()  # không phát dồn để đuổi kịp
            else:
                time.sleep(-late)
