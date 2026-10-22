"""Các phương pháp sinh vòng đời Máy ảo. Mọi biến đổi làm trên z đã chuẩn hóa theo Điều kiện vận hành,
vì FD002 và FD004 đổi điều kiện gần như mỗi Chu kỳ: nội suy hay trộn số đo thô sẽ trộn lẫn các điều kiện."""
from dataclasses import dataclass, field

import numpy as np

from engine.data import INFORMATIVE


@dataclass(frozen=True)
class Life:
    data: np.ndarray  # Chu kỳ × 24 cột, số đo thô chưa nhiễu
    method: str
    base: list[str]   # mã các Máy nguồn
    params: dict = field(default_factory=dict)


def stretch(z, length):
    """Co giãn về độ dài mới: cột mang thông tin nội suy tuyến tính, các cột khác (thiết lập,
    cảm biến hằng số) lấy nguyên ở Chu kỳ nguồn gần nhất để Điều kiện vận hành vẫn có thật."""
    pos = np.linspace(0, len(z) - 1, length)
    out = z[np.rint(pos).astype(int)]
    lo = np.floor(pos).astype(int)
    hi = np.minimum(lo + 1, len(z) - 1)
    w = (pos - lo)[:, None]
    out[:, INFORMATIVE] = z[lo][:, INFORMATIVE] * (1 - w) + z[hi][:, INFORMATIVE] * w
    return out


def pick_subset(ds, rng, cfg):
    """Tập con theo tỷ lệ số Máy nguồn của các tập con đang bật."""
    sizes = np.array([len(ds.by_subset[s]) for s in cfg.subsets], dtype=float)
    return cfg.subsets[rng.choice(len(sizes), p=sizes / sizes.sum())]


def pick_source(ds, rng, cfg, subset=None):
    machines = ds.by_subset[subset or pick_subset(ds, rng, cfg)]
    return machines[rng.integers(len(machines))]


def gauss(ds, rng, cfg):
    sid = pick_source(ds, rng, cfg)
    return Life(ds.sources[sid], "gauss", [sid])


def warp(ds, rng, cfg):
    sid = pick_source(ds, rng, cfg)
    src = ds.sources[sid]
    factor = rng.uniform(*cfg.warp_range)
    length = max(2, round(len(src) * factor))
    return Life(ds.denormalize(stretch(ds.normalize(src), length)), "warp", [sid], {"factor": round(factor, 4)})


def blend(ds, rng, cfg):
    subset = pick_subset(ds, rng, cfg)
    machines = ds.by_subset[subset]
    i, j = rng.choice(len(machines), size=2, replace=False)
    a, b = machines[i], machines[j]
    w = rng.uniform()
    length = max(2, round(w * len(ds.sources[a]) + (1 - w) * len(ds.sources[b])))
    za, zb = (stretch(ds.normalize(ds.sources[m]), length) for m in (a, b))
    z = za if w >= 0.5 else zb  # thiết lập và cảm biến hằng số theo máy có trọng số lớn hơn
    z[:, INFORMATIVE] = w * za[:, INFORMATIVE] + (1 - w) * zb[:, INFORMATIVE]
    return Life(ds.denormalize(z), "blend", [a, b], {"weight": round(w, 4)})


C_GRID = np.arange(0.5, 10.001, 0.25)
MIN_LIFE = 30


def fit_exponential(z):
    """Khớp mỗi cột z(τ) = a + b·e^(c·τ), τ đều trong [0, 1]: duyệt c trên lưới, a và b giải đóng bằng
    bình phương tối thiểu. Trả về a, b, c và độ lệch chuẩn phần dư r, mỗi cái một giá trị cho mỗi cột."""
    n = len(z)
    e = np.exp(C_GRID[:, None] * np.linspace(0, 1, n)[None, :])          # lưới × n
    se, see = e.sum(1), (e * e).sum(1)
    sy, sey, syy = z.sum(0), e @ z, (z * z).sum(0)                       # cột; lưới × cột; cột
    det = (n * see - se ** 2)[:, None]
    b = (n * sey - se[:, None] * sy) / det
    a = (sy - b * se[:, None]) / n
    sse = syy - a * sy - b * sey
    best = sse.argmin(0)
    cols = np.arange(z.shape[1])
    return a[best, cols], b[best, cols], C_GRID[best], np.sqrt(np.maximum(sse[best, cols], 0) / n)


def parametric_model(ds):
    """Mỗi tập con: trung bình và nhân tử Cholesky của hiệp phương sai của vectơ (L, z0, z1, c, r của 16 cảm biến)
    trên các máy nguồn; tính sẵn nhân tử vì phân rã lại mỗi lần sinh tốn vài ms.

    Mỗi đường lưu bằng giá trị đầu z0 = a + b, giá trị cuối z1 = a + b·e^c và độ cong c thay vì (a, b, c):
    b và c phụ thuộc nhau rất phi tuyến (c lớn thì b rất nhỏ), nên bốc chúng từ phân bố chuẩn ra đường mũ
    bùng nổ; còn z0, z1 có miền hữu hạn và đường luôn nằm giữa hai đầu mút.
    Khớp một lần rồi giữ trên ds; tính lại cũng cho cùng kết quả nên không cần khóa giữa các luồng."""
    if not hasattr(ds, "_parametric"):
        model = {}
        for subset, machines in ds.by_subset.items():
            rows = []
            for m in machines:
                a, b, c, r = fit_exponential(ds.normalize(ds.sources[m])[:, INFORMATIVE])
                rows.append(np.concatenate([[len(ds.sources[m])], a + b, a + b * np.exp(c), c, r]))
            rows = np.array(rows)
            cov = np.cov(rows, rowvar=False) + 1e-6 * np.eye(rows.shape[1])
            model[subset] = (rows.mean(0), np.linalg.cholesky(cov))
        ds._parametric = model
    return ds._parametric


def parametric(ds, rng, cfg):
    subset = pick_subset(ds, rng, cfg)
    mean, chol = parametric_model(ds)[subset]
    theta = mean + chol @ rng.standard_normal(len(mean))
    k = len(INFORMATIVE)
    length = max(MIN_LIFE, round(theta[0]))
    z0, z1, c, r = (theta[1 + i * k:1 + (i + 1) * k] for i in range(4))
    c = np.clip(c, C_GRID[0], C_GRID[-1])
    b = (z1 - z0) / np.expm1(c)
    a = z0 - b
    tau = np.linspace(0, 1, length)[:, None]
    # Thiết lập và cảm biến hằng số mượn chuỗi của một máy nguồn cùng tập con, co về độ dài mới.
    sid = pick_source(ds, rng, cfg, subset)
    z = stretch(ds.normalize(ds.sources[sid]), length)
    z[:, INFORMATIVE] = a + b * np.exp(c * tau) + rng.standard_normal((length, k)) * np.maximum(r, 0)
    return Life(ds.denormalize(z), "parametric", [sid])


GENERATORS = {"gauss": gauss, "warp": warp, "blend": blend, "parametric": parametric}


def generate(method, ds, rng, cfg):
    return GENERATORS[method](ds, rng, cfg)
