"""Đội Máy ảo: giữ đúng N máy, mỗi nhịp 100 ms phát Bản tin theo tốc độ k, thay máy hết vòng đời."""
from dataclasses import dataclass

import numpy as np

from engine.data import COLUMNS, INFORMATIVE
from engine.faults import FaultInjector
from engine.generators import generate

TICKS_PER_SECOND = 10
FULL = 1000  # bộ đếm tính bằng phần nghìn Chu kỳ, số nguyên để k × 0,1 cộng dồn không sai số


@dataclass
class Machine:
    id: str
    life: object         # generators.Life
    sigma: np.ndarray    # σ của 16 cảm biến mang thông tin ở từng Chu kỳ
    pos: int             # số Chu kỳ đã phát
    acc: int = 0


class Fleet:
    """Nguồn ngẫu nhiên và thời điểm đến từ bên ngoài, để cùng hạt giống cho cùng chuỗi Bản tin."""

    def __init__(self, ds, cfg, rng, first_id):
        self.ds, self.rng, self.next_id = ds, rng, first_id
        self.machines = []  # theo thứ tự sinh: máy mới nhất ở cuối
        self.faults = FaultInjector(TICKS_PER_SECOND)
        self.resize(cfg)

    def _spawn(self, cfg, random_start):
        methods = sorted(cfg.method_weights)
        w = np.array([cfg.method_weights[m] for m in methods])
        life = generate(methods[self.rng.choice(len(methods), p=w / w.sum())], self.ds, self.rng, cfg)
        # Đội ban đầu và máy thêm vào bắt đầu giữa vòng đời để không hỏng đồng loạt; máy thay thế bắt đầu từ Chu kỳ 1.
        start = int(self.rng.integers(len(life.data))) if random_start else 0
        # Pha bộ đếm ngẫu nhiên: nếu mọi máy cùng bắt đầu từ 0 thì ở k ≤ 1 cả đội phát dồn vào cùng một nhịp.
        machine = Machine(f"vm-{self.next_id:06d}", life, self.ds.sigma_rows(life.data), start,
                          int(self.rng.integers(FULL)))
        self.next_id += 1
        return machine

    def resize(self, cfg):
        del self.machines[cfg.machines:]  # bớt máy mới nhất, không coi là hỏng
        while len(self.machines) < cfg.machines:
            self.machines.append(self._spawn(cfg, random_start=True))

    @property
    def fault_counts(self):
        return self.faults.counts

    def tick(self, cfg, event_time):
        """Một nhịp: trả về (Bản tin sau khi chèn lỗi, sự kiện hỏng)."""
        self.resize(cfg)
        step = round(cfg.speed * FULL / TICKS_PER_SECOND)
        emitting = []
        for m in self.machines:
            m.acc += step
            if m.acc >= FULL:  # k ≤ 10 nên tối đa một Bản tin mỗi máy mỗi nhịp
                m.acc -= FULL
                emitting.append(m)
        if not emitting:
            return self.faults.apply([], cfg, self.rng), []

        rows = np.array([m.life.data[m.pos] for m in emitting])
        sigma = np.array([m.sigma[m.pos] for m in emitting])
        # Luôn bốc nhiễu kể cả khi noise = 0, để đổi mức nhiễu không làm lệch phần còn lại của chuỗi ngẫu nhiên.
        rows[:, INFORMATIVE] += self.rng.standard_normal(sigma.shape) * sigma * cfg.noise

        messages, failures, failed = [], [], []
        for m, row in zip(emitting, rows.tolist()):
            m.pos += 1
            length = len(m.life.data)
            messages.append({"machine_id": m.id, "cycle": m.pos, "event_time": event_time, **dict(zip(COLUMNS, row)),
                             "truth": {"rul": length - m.pos, "method": m.life.method, "fault": None}})
            if m.pos == length:
                failed.append(m)
                failures.append({"machine_id": m.id, "failed_at": event_time, "last_cycle": length,
                                 "method": m.life.method, "base": m.life.base, "params": m.life.params})
        for m in failed:
            self.machines.remove(m)
            self.machines.append(self._spawn(cfg, random_start=False))
        return self.faults.apply(messages, cfg, self.rng), failures
