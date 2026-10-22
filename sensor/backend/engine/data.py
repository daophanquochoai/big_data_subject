"""Đọc vòng đời C-MAPSS, bảng thống kê theo Điều kiện vận hành, chuẩn hóa và đổi ngược."""
from pathlib import Path

import numpy as np

SUBSETS = ["FD001", "FD002", "FD003", "FD004"]
SETTINGS = ["setting1", "setting2", "setting3"]
COLUMNS = SETTINGS + [f"s{i}" for i in range(1, 22)]  # thứ tự cột của mọi mảng vòng đời
N_SETTINGS = len(SETTINGS)
# Chỉ số cột của 16 cảm biến mang thông tin; cảm biến 1, 5, 16, 18, 19 gần như hằng số.
INFORMATIVE = [COLUMNS.index(f"s{i}") for i in (2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 20, 21)]


def condition_key(settings):
    """Mã Điều kiện vận hành: làm tròn ba cột thiết lập như v1, gói vào một số nguyên."""
    r = np.rint(settings * [1, 100, 1]).astype(int)
    return (r[:, 0] * 1000 + r[:, 1]) * 1000 + r[:, 2]


class Dataset:
    """Vòng đời nguồn (mã máy -> mảng Chu kỳ × 24 cột) và bảng thống kê μ, σ theo điều kiện."""

    def __init__(self, sources):
        self.sources = sources
        self.by_subset = {s: sorted(k for k in sources if k.startswith(s)) for s in SUBSETS}
        rows = np.vstack(list(sources.values()))
        keys = condition_key(rows[:, :N_SETTINGS])
        self.keys = np.unique(keys)
        c = np.searchsorted(self.keys, keys)
        informative = rows[:, INFORMATIVE]
        self.mu = np.array([informative[c == i].mean(axis=0) for i in range(len(self.keys))])
        self.sigma = np.array([informative[c == i].std(axis=0, ddof=1) for i in range(len(self.keys))])
        assert (self.sigma > 0).all(), "một cảm biến mang thông tin là hằng số trong một điều kiện"

    def conditions(self, data):
        keys = condition_key(data[:, :N_SETTINGS])
        c = np.searchsorted(self.keys, keys)
        assert (self.keys[np.minimum(c, len(self.keys) - 1)] == keys).all(), "Điều kiện vận hành lạ"
        return c

    def normalize(self, data):
        """Cột mang thông tin thành z theo điều kiện của từng Chu kỳ; các cột khác giữ nguyên."""
        c = self.conditions(data)
        z = data.copy()
        z[:, INFORMATIVE] = (data[:, INFORMATIVE] - self.mu[c]) / self.sigma[c]
        return z

    def denormalize(self, z):
        c = self.conditions(z)
        x = z.copy()
        x[:, INFORMATIVE] = z[:, INFORMATIVE] * self.sigma[c] + self.mu[c]
        return x

    def sigma_rows(self, data):
        """σ của 16 cảm biến mang thông tin ở từng Chu kỳ, dùng cho nhiễu lúc phát."""
        return self.sigma[self.conditions(data)]


def load_dataset(data_dir, machines=None):
    """machines: chỉ giữ các Máy này làm nguồn (ví dụ nhóm phát lại do phần Spark chia); None là mọi Máy."""
    sources = {}
    for subset in SUBSETS:
        a = np.loadtxt(Path(data_dir) / f"train_{subset}.txt")
        for unit in np.unique(a[:, 0]).astype(int):
            rows = a[a[:, 0] == unit]
            assert (rows[:, 1] == np.arange(1, len(rows) + 1)).all(), f"{subset} máy {unit}: Chu kỳ không liền"
            if machines is None or f"{subset}-{unit:03d}" in machines:
                sources[f"{subset}-{unit:03d}"] = rows[:, 2:]
    return Dataset(sources)
