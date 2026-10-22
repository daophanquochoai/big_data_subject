import numpy as np
import pandas as pd
import pytest

from pdm.schema import SENSORS
from pdm.windows import FEATURES, MIN_READINGS, advance, static_windows

START = pd.Timestamp("2026-10-15T09:00:00")  # không múi giờ, như Spark đưa vào hàm pandas


def life(machine="m1", cycles=range(1, 74), seed=0):
    """Bản tin đã chuẩn hóa của một máy: s2 = Chu kỳ, các cảm biến khác ngẫu nhiên cố định theo hạt giống."""
    cycles = np.array(list(cycles))
    rng = np.random.default_rng(seed)
    pdf = pd.DataFrame(rng.normal(size=(len(cycles), len(SENSORS))), columns=SENSORS)
    pdf["s2"] = cycles.astype(float)
    pdf.insert(0, "event_time", START + pd.to_timedelta(cycles * 0.5, unit="s"))
    pdf.insert(0, "cycle", cycles)
    pdf.insert(0, "machine_id", machine)
    return pdf


def by_age(df):
    return {int(r.age): r for r in df.itertuples()}


def test_window_statistics_match_hand_computed_values():
    rows = by_age(static_windows(life(cycles=range(1, 41))))
    # Cửa sổ kết thúc ở 30: Chu kỳ 1..30, trung bình 15,5, phương sai mẫu 30·31/12.
    assert rows[30].n_readings == 30 and rows[30].s2_mean == pytest.approx(15.5)
    assert rows[30].s2_std == pytest.approx(np.sqrt(30 * 31 / 12))
    # Cửa sổ kết thúc ở 35: Chu kỳ 6..35.
    assert rows[35].s2_mean == pytest.approx(20.5)
    assert rows[35].window_end_us == pd.Timestamp(START + pd.Timedelta(seconds=17.5)).value // 1000
    # Dưới 20 Bản tin thì bỏ; đúng 20 thì giữ.
    assert 15 not in rows and rows[20].n_readings == MIN_READINGS


def test_missing_values_are_ignored_not_zeroed():
    pdf = life(cycles=range(1, 41))
    pdf.loc[pdf.cycle == 30, "s2"] = np.nan
    rows = by_age(static_windows(pdf))
    assert rows[30].n_readings == 30
    assert rows[30].s2_mean == pytest.approx(np.mean([c for c in range(1, 31) if c != 30]))


def test_feature_columns():
    out = static_windows(life())
    assert list(out.columns) == ["machine_id", "age", "n_readings", "window_end_us", *FEATURES[1:]]
    assert FEATURES[0] == "age" and len(FEATURES) == 33


def feed(batches):
    state, out = None, []
    for b in batches:
        state, rows = advance(state, b)
        out.append(rows)
    return state, pd.concat(out, ignore_index=True)


def expected(pdf, last_cycle):
    """Chế độ tĩnh, chỉ các cửa sổ mà luồng đã kịp phát: e + 5 ≤ Chu kỳ cuối đã thấy."""
    s = static_windows(pdf)
    return s[s.age + 5 <= last_cycle].reset_index(drop=True)


def same(a, b):
    a, b = a.sort_values("age").reset_index(drop=True), b.sort_values("age").reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b, check_dtype=False)


def test_stream_in_order_matches_static():
    pdf = life()
    _, out = feed([pdf.iloc[i:i + 7] for i in range(0, len(pdf), 7)])
    same(out, expected(pdf, 73))


def test_stream_with_shuffled_duplicated_odd_batches_matches_static():
    pdf = life()
    rng = np.random.default_rng(1)
    rows = []
    for i in range(0, len(pdf), 5):  # đảo thứ tự trong từng khối 5 Chu kỳ
        block = pdf.iloc[i:i + 5]
        rows.append(block.iloc[rng.permutation(len(block))])
    shuffled = pd.concat(rows)
    dup = shuffled.iloc[::7].copy()
    dup["s3"] = 999.0  # bản trùng đến sau mang giá trị khác: phải bị bỏ, giữ bản đến trước
    stream = pd.concat([shuffled, dup]).sort_index(kind="stable")
    cuts = [0, 3, 4, 20, 21, 50, 60, len(stream)]
    _, out = feed([stream.iloc[a:b] for a, b in zip(cuts, cuts[1:])])
    same(out, expected(pdf, 73))


def test_machine_first_seen_mid_life_starts_from_there():
    pdf = life(cycles=range(37, 101))
    _, out = feed([pdf.iloc[i:i + 10] for i in range(0, len(pdf), 10)])
    same(out, expected(pdf, 100))
    assert out.age.min() >= 60  # cần 20 Bản tin: Chu kỳ 37..56 trở đi


def test_late_reading_for_an_emitted_window_is_dropped():
    pdf = life(cycles=range(1, 61))
    state, first = feed([pdf])
    late = pdf[pdf.cycle == 30].copy()
    late["s2"] = -1.0
    _, again = advance(state, late)
    assert again.empty and 30 in set(first.age)


def test_state_stays_small():
    pdf = life(cycles=range(1, 501))
    state, _ = feed([pdf.iloc[i:i + 13] for i in range(0, len(pdf), 13)])
    assert len(state["cycles"]) <= 40


def test_state_round_trip_keeps_event_times_exactly():
    from pdm.windows import _from_state, _to_state
    pdf = life(cycles=range(1, 31))
    # Mốc mili giây như của bộ giả lập; số giây kiểu số thực làm lệch tới nano giây và Arrow từ chối ghi.
    pdf["event_time"] = pd.to_datetime("2026-10-14T17:41:38.569") + pd.to_timedelta(pdf.cycle * 0.137, unit="s")
    pdf["event_time"] = pdf["event_time"].dt.floor("ms")
    state, _ = advance(None, pdf)
    back = _from_state(_to_state(state))
    assert back["last"] == state["last"]
    for k in ("cycles", "times", "values"):
        assert (back[k] == state[k]).all()
    # Cửa sổ phát ra sau khi khôi phục mang đúng event_time gốc, không lệch nano giây.
    _, out = advance(back, life(cycles=range(31, 41)).assign(event_time=pd.Timestamp("2026-10-14T17:42")))
    at30 = pdf.loc[pdf.cycle == 30, "event_time"].iloc[0]
    assert out.loc[out.age == 30, "window_end_us"].iloc[0] == at30.value // 1000


def test_last_window_ends_at_the_last_cycle_even_with_few_readings():
    from pdm.windows import last_window
    out = last_window(life(cycles=range(1, 20)))  # 19 Chu kỳ như máy ngắn nhất của FD004 test
    assert list(out.age) == [19] and list(out.n_readings) == [19]
    assert out.s2_mean.iloc[0] == pytest.approx(10.0)
