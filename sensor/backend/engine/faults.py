"""Lỗi chèn: mỗi Bản tin bốc đúng một kết cục theo xác suất cấu hình. Đáp án ghi vào truth.fault."""
import heapq
from collections import Counter

from engine.data import COLUMNS, N_SETTINGS

SENSORS = COLUMNS[N_SETTINGS:]
KINDS = ("bad_value", "drop", "late", "duplicate")


class FaultInjector:
    def __init__(self, ticks_per_second):
        self.ticks_per_second = ticks_per_second
        self.tick = 0
        self.held = []  # (nhịp được gửi, thứ tự, Bản tin) của các Bản tin trễ
        self.seq = 0
        self.counts = Counter(dict.fromkeys(KINDS, 0))

    def apply(self, messages, cfg, rng):
        """Bản tin của nhịp này sau khi chèn lỗi, cộng các Bản tin trễ đã tới hạn."""
        self.tick += 1
        p = cfg.faults
        bounds = [p.bad_value, p.bad_value + p.drop, p.bad_value + p.drop + p.late,
                  p.bad_value + p.drop + p.late + p.duplicate]
        draws = rng.random(len(messages))  # luôn bốc, để đổi tỷ lệ không làm lệch phần còn lại của chuỗi
        out = []
        for m, u in zip(messages, draws):
            if u >= bounds[3]:
                out.append(m)
            elif u < bounds[0]:
                sensor = SENSORS[rng.integers(len(SENSORS))]
                m[sensor] = -9999.0 if rng.random() < 0.5 else m[sensor] * 10
                m["truth"]["fault"] = "bad_value"
                out.append(m)
                self.counts["bad_value"] += 1
            elif u < bounds[1]:
                self.counts["drop"] += 1
            elif u < bounds[2]:
                delay = rng.uniform(1, cfg.late_max_s)  # event_time giữ nguyên thời điểm đo
                m["truth"]["fault"] = "late"
                heapq.heappush(self.held, (self.tick + round(delay * self.ticks_per_second), self.seq, m))
                self.seq += 1
                self.counts["late"] += 1
            else:
                out.append(m)
                out.append({**m, "truth": {**m["truth"], "fault": "duplicate"}})
                self.counts["duplicate"] += 1
        while self.held and self.held[0][0] <= self.tick:
            out.append(heapq.heappop(self.held)[2])
        return out
