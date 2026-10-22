"""Đặc trưng cửa sổ trượt theo Chu kỳ: 30 Chu kỳ gần nhất, bước 5 Chu kỳ.

Tính theo Chu kỳ chứ không theo giây vì bộ giả lập đổi được số Chu kỳ mỗi giây khi đang chạy.
Một hàm lõi pandas dùng cho cả huấn luyện (applyInPandas) lẫn luồng (applyInPandasWithState),
nên đặc trưng lúc chấm điểm không lệch với lúc huấn luyện.
"""
import warnings

import numpy as np
import pandas as pd

from pdm.schema import SENSORS

WINDOW = 30
SLIDE = 5
MIN_READINGS = 20  # bên gọi đo trên tập test chính thức tự bỏ ngưỡng này
WAIT = 5           # luồng: phát cửa sổ e khi đã thấy Chu kỳ ≥ e + WAIT, để Bản tin trễ hay đảo thứ tự kịp vào
KEEP = WINDOW - SLIDE  # luồng: các cửa sổ sau cửa sổ cuối đã phát chỉ cần Chu kỳ > cửa sổ cuối − KEEP

FEATURES = ["age"] + [f"{s}_{k}" for s in SENSORS for k in ("mean", "std")]
# window_end_us: event_time lớn nhất trong cửa sổ, micro giây. Hàm pandas trả một khối số thực cho nhanh
# (dựng từng cột tốn gấp bốn); lớp bọc Spark đổi nó thành cột thời điểm window_end.
COLUMNS = ["machine_id", "age", "n_readings", "window_end_us", *FEATURES[1:]]
OUTPUT_SCHEMA = ("machine_id STRING, age INT, n_readings INT, window_end_us BIGINT, "
                 + ", ".join(f"{c} DOUBLE" for c in FEATURES[1:]))
EMPTY = pd.DataFrame(columns=COLUMNS)  # dựng sẵn: DataFrame rỗng 36 cột tốn tới 2 ms mỗi lần; chỉ đọc


def _micros(series):
    """Thời điểm -> số nguyên micro giây (độ chính xác của Spark)."""
    return pd.to_datetime(series).to_numpy(dtype="datetime64[us]").astype("int64")


def _windows(machine, cycles, times, values, ends, min_readings):
    """Lõi numpy: mỗi Chu kỳ kết thúc e một dòng, trên các Chu kỳ e − 29 … e; giá trị rỗng bỏ qua."""
    rows = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # cột toàn rỗng hoặc chỉ một giá trị: ra NaN là đúng
        for e in ends:
            m = (cycles > e - WINDOW) & (cycles <= e)
            n = int(m.sum())
            if n < min_readings:
                continue
            v = values[m]
            stats = np.column_stack([np.nanmean(v, axis=0), np.nanstd(v, axis=0, ddof=1)]).ravel()
            rows.append(np.concatenate([[e, n, times[m].max()], stats]))  # micro giây ~1,8e15 < 2^53: số thực vẫn đúng
    if not rows:
        return EMPTY
    out = pd.DataFrame(np.array(rows), columns=COLUMNS[1:])
    out.insert(0, "machine_id", machine)
    return out


def _arrays(pdf):
    """Cột của Bản tin -> mảng, bỏ Bản tin trùng Chu kỳ (giữ bản đến trước)."""
    cycles = pdf["cycle"].to_numpy(dtype="int64")
    _, first = np.unique(cycles, return_index=True)
    keep = np.sort(first)
    return cycles[keep], _micros(pdf["event_time"])[keep], pdf[SENSORS].to_numpy(dtype=float)[keep]


def static_windows(pdf, min_readings=MIN_READINGS):
    """Chế độ tĩnh: mọi cửa sổ kết thúc ở bội số của 5. Trùng Chu kỳ thì giữ bản đầu."""
    if pdf.empty:
        return EMPTY
    cycles, times, values = _arrays(pdf)
    order = np.argsort(cycles)
    return _windows(pdf["machine_id"].iloc[0], cycles[order], times[order], values[order],
                    range(SLIDE, int(cycles.max()) + 1, SLIDE), min_readings)


def last_window(pdf):
    """Cửa sổ kết thúc đúng ở Chu kỳ cuối của Máy, không cần đủ 20 Bản tin: đo trên tập test chính thức
    (Máy test bị cắt ở Chu kỳ bất kỳ, có máy chỉ 19 Chu kỳ)."""
    if pdf.empty:
        return EMPTY
    cycles, times, values = _arrays(pdf)
    order = np.argsort(cycles)
    return _windows(pdf["machine_id"].iloc[0], cycles[order], times[order], values[order], [int(cycles.max())], 1)


def advance(state, new):
    """Chế độ luồng, một bước cho một máy: (trạng thái cũ, Bản tin mới) -> (trạng thái mới, các cửa sổ phát ra).

    Trạng thái: các mảng cycles, times (micro giây), values của những Bản tin còn cần, và last = cửa sổ cuối đã phát.
    """
    machine = new["machine_id"].iloc[0]
    c, t, v = _arrays(new)
    if state is None:
        # Cửa sổ đầu tiên có thể phát là bội số của 5 không nhỏ hơn Chu kỳ đầu tiên thấy được.
        state = {"cycles": c[:0], "times": t[:0], "values": v[:0], "last": -(-int(c.min()) // SLIDE) * SLIDE - SLIDE}
    last = state["last"]
    # Bỏ Bản tin trùng với cái đã có, và Bản tin quá trễ không còn cửa sổ nào trong tương lai cần tới.
    ok = ~np.isin(c, state["cycles"]) & (c > last - KEEP)
    cycles = np.concatenate([state["cycles"], c[ok]])
    order = np.argsort(cycles, kind="stable")
    cycles = cycles[order]
    times = np.concatenate([state["times"], t[ok]])[order]
    values = np.concatenate([state["values"], v[ok]])[order]
    ends = range(last + SLIDE, int(cycles.max()) - WAIT + 1, SLIDE) if len(cycles) else range(0)
    out = _windows(machine, cycles, times, values, ends, MIN_READINGS)
    if len(ends):
        last = ends[-1]
    m = cycles > last - KEEP
    return {"cycles": cycles[m], "times": times[m], "values": values[m], "last": last}, out


# --- Bọc cho Spark ---------------------------------------------------------------------------------

TIMEOUT_MS = 120_000  # máy không có Bản tin trong 2 phút (đã hỏng hoặc bị bớt): xóa trạng thái
STATE_SCHEMA = "cycles ARRAY<INT>, times ARRAY<BIGINT>, values ARRAY<DOUBLE>, last INT"  # times: micro giây


def _to_state(state):
    return (state["cycles"].tolist(), state["times"].tolist(), state["values"].ravel().tolist(), int(state["last"]))


def _from_state(saved):
    cycles, times, values, last = saved
    return {"cycles": np.array(cycles, dtype="int64"), "times": np.array(times, dtype="int64"),
            "values": np.array(values, dtype=float).reshape(len(cycles), len(SENSORS)), "last": last}


def _with_window_end(df):
    from pyspark.sql import functions as F
    return df.withColumn("window_end", F.timestamp_micros("window_end_us")).drop("window_end_us")


def spark_windows(df, min_readings=MIN_READINGS):
    """Chế độ tĩnh trên DataFrame Spark (machine_id, cycle, event_time, 16 cảm biến đã chuẩn hóa)."""
    return _with_window_end(df.groupBy("machine_id").applyInPandas(lambda pdf: static_windows(pdf, min_readings),
                                                                   OUTPUT_SCHEMA))


def stream_windows(df):
    """Chế độ luồng: trạng thái theo từng máy, chế độ append."""
    from pyspark.sql.streaming.state import GroupStateTimeout

    def step(key, pdfs, state):
        if state.hasTimedOut:
            state.remove()
            return
        current = _from_state(state.get) if state.exists else None
        for pdf in pdfs:
            current, out = advance(current, pdf)
            if len(out):
                yield out
        state.update(_to_state(current))
        state.setTimeoutDuration(TIMEOUT_MS)

    return _with_window_end(df.groupBy("machine_id").applyInPandasWithState(
        step, OUTPUT_SCHEMA, STATE_SCHEMA, "append", GroupStateTimeout.ProcessingTimeTimeout))
