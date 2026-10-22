from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.kafka_sink import MemorySink
from app.main import create_app
from app.runner import Runner

NOW = datetime(2026, 10, 15, 9, 30, 12, 400000, tzinfo=timezone.utc)


@pytest.fixture
def runner(ds, tmp_path):
    return Runner(ds, MemorySink(), tmp_path)


@pytest.fixture
def client(runner):
    return TestClient(create_app(runner))


@pytest.mark.parametrize("body", [
    {"machines": 0}, {"machines": 10_001},
    {"speed": 0.05}, {"speed": 11},
    {"noise": -0.1}, {"noise": 1.1},
    {"subsets": []}, {"subsets": ["FD005"]}, {"subsets": ["FD001", "FD001"]},
    {"method_weights": {"gauss": -1}}, {"method_weights": {"gauss": 0, "warp": 0, "blend": 0, "parametric": 0}},
    {"method_weights": {"magic": 1}},
    {"warp_range": [2, 1]}, {"warp_range": [0.2, 1]}, {"warp_range": [1, 3.5]},
    {"faults": {"drop": 1.5}}, {"faults": {"drop": 0.6, "late": 0.6}},
    {"late_max_s": 0.5}, {"late_max_s": 61},
    {"seed": -1},
    {"colour": "red"},
])
def test_invalid_config_is_rejected_with_a_reason(client, body):
    before = client.get("/api/config").json()
    r = client.patch("/api/config", json=body)
    assert r.status_code == 422
    assert r.json()["detail"] and all(e["msg"] for e in r.json()["detail"])
    assert client.get("/api/config").json() == before


def test_patch_changes_only_the_given_fields(client):
    r = client.patch("/api/config", json={"speed": 2.5, "faults": {"drop": 0.1}})
    assert r.status_code == 200
    cfg = client.get("/api/config").json()
    assert cfg["speed"] == 2.5 and cfg["machines"] == 500
    assert cfg["faults"] == {"bad_value": 0, "drop": 0.1, "late": 0, "duplicate": 0}


def test_start_stop_reset(client, runner):
    assert client.get("/api/status").json()["running"] is False
    client.post("/api/start")
    assert client.get("/api/status").json()["running"] is True
    for _ in range(20):
        runner.step(NOW)
    assert runner.sink.messages
    client.post("/api/stop")
    sent = len(runner.sink.messages)
    runner.step(NOW)
    assert len(runner.sink.messages) == sent

    client.post("/api/start")
    client.post("/api/reset")
    assert client.get("/api/status").json()["running"] is False


def test_machine_ids_never_repeat_across_resets_and_restarts(ds, tmp_path):
    first = Runner(ds, MemorySink(), tmp_path)
    ids = {m.id for m in first.fleet.machines}
    first.reset()
    again = {m.id for m in first.fleet.machines}
    restarted = {m.id for m in Runner(ds, MemorySink(), tmp_path).fleet.machines}
    assert not ids & again and not (ids | again) & restarted


def test_machine_list_pages_filters_and_sorts(client):
    client.patch("/api/config", json={"machines": 120})
    client.post("/api/reset")
    page = client.get("/api/machines", params={"sort": "rul"}).json()
    assert page["total"] == 120 and len(page["items"]) == 50
    ruls = [m["rul"] for m in page["items"]]
    assert ruls == sorted(ruls)
    m = page["items"][0]
    assert set(m) == {"id", "method", "base", "params", "cycle", "life", "rul"} and m["rul"] == m["life"] - m["cycle"]
    assert len(client.get("/api/machines", params={"page": 3}).json()["items"]) == 20

    warp = client.get("/api/machines", params={"method": "warp"}).json()
    assert 0 < warp["total"] < 120 and all(m["method"] == "warp" for m in warp["items"])


def test_messages_and_failures_reach_their_topics(client, runner):
    client.patch("/api/config", json={"machines": 50, "speed": 10})
    client.post("/api/reset")
    client.post("/api/start")
    for _ in range(400):
        runner.step(NOW)
    topics = {t for t, _, _ in runner.sink.messages}
    assert topics == {"sensor.raw", "machine.failed"}
    assert all(key == value["machine_id"] for _, key, value in runner.sink.messages)


def test_full_send_queue_drops_the_rest_of_the_tick_instead_of_crashing(ds, tmp_path):
    runner = Runner(ds, MemorySink(capacity=30), tmp_path)
    runner.update(runner.config.model_copy(update={"machines": 100, "speed": 10}))
    runner.reset()
    runner.start()
    runner.step(NOW)
    # Mỗi máy một Bản tin (k = 10), cộng các sự kiện hỏng của nhịp; phần quá 30 bị bỏ và được đếm.
    assert len(runner.sink.messages) == 30
    assert runner.counters["backpressure_dropped"] == 100 + runner.counters["failed"] - 30


def test_stats_socket_pushes_counters(client, runner):
    with client.websocket_connect("/ws/stats") as ws:
        stats = ws.receive_json()
    assert {"sent_per_s", "acked_per_s", "alive", "failed", "faults", "kafka_errors_per_s",
            "backpressure_dropped", "overruns", "queue"} <= set(stats)
    assert stats["alive"] == 500


@pytest.mark.parametrize("method", ["gauss", "warp", "blend", "parametric"])
def test_preview_returns_generated_and_source_lifecycles_without_touching_the_fleet(client, runner, method):
    before = runner.machines()
    r = client.post("/api/preview", json={"method": method, "subset": "FD002", "seed": 3})
    assert r.status_code == 200
    p = r.json()
    assert p["method"] == method and p["columns"][:3] == ["setting1", "setting2", "setting3"]
    assert set(p["sources"]) == set(p["base"]) and all(b.startswith("FD002") for b in p["base"])
    assert len(p["generated"][0]) == len(p["columns"]) == 24
    assert client.post("/api/preview", json={"method": method, "subset": "FD002", "seed": 3}).json() == p
    assert runner.machines() == before


@pytest.mark.parametrize("body", [{"method": "magic", "subset": "FD001"}, {"method": "warp", "subset": "FD009"},
                                  {"method": "warp", "subset": "FD001", "noise": 2}])
def test_bad_preview_is_rejected(client, body):
    assert client.post("/api/preview", json=body).status_code == 422


def test_machine_detail_has_the_whole_lifecycle(client, runner):
    m = runner.machines()[0]
    d = client.get(f"/api/machines/{m['id']}").json()
    assert d["id"] == m["id"] and d["cycle"] == m["cycle"] and len(d["lifecycle"]) == m["life"]
    assert client.get("/api/machines/vm-999999").status_code == 404


def test_scenarios_save_list_load_delete(client, runner):
    client.patch("/api/config", json={"machines": 77, "speed": 3, "seed": 5})
    assert client.put("/api/scenarios/slow-77").status_code == 200
    assert client.get("/api/scenarios").json() == ["slow-77"]
    assert client.get("/api/scenarios/slow-77").json()["machines"] == 77

    client.patch("/api/config", json={"machines": 10})
    client.post("/api/start")
    r = client.post("/api/scenarios/slow-77/load")
    assert r.status_code == 200 and r.json()["machines"] == 77
    assert client.get("/api/config").json()["machines"] == 77
    assert len(runner.fleet.machines) == 77 and runner.running is False  # nạp thì tạo lại đội máy

    assert client.delete("/api/scenarios/slow-77").status_code == 204
    assert client.get("/api/scenarios").json() == []
    assert client.get("/api/scenarios/slow-77").status_code == 404
    assert client.post("/api/scenarios/slow-77/load").status_code == 404


@pytest.mark.parametrize("name", ["..", "a b", "x" * 65, "tên"])
def test_odd_scenario_names_are_rejected(client, name):
    assert client.put(f"/api/scenarios/{name}").status_code in (404, 422)


def strip(sink):
    return [(t, {k: v for k, v in m.items() if k not in ("machine_id", "event_time")}) for t, _, m in sink.messages]


def test_loading_a_scenario_twice_replays_the_same_stream(client, runner):
    client.patch("/api/config", json={"machines": 60, "speed": 5, "seed": 9,
                                      "faults": {"bad_value": 0.05, "late": 0.05, "duplicate": 0.05, "drop": 0.05}})
    client.put("/api/scenarios/replay")
    runs = []
    for _ in range(2):
        client.post("/api/scenarios/replay/load")
        runner.sink.messages.clear()
        client.post("/api/start")
        for _ in range(80):
            runner.step(NOW)
        runs.append(strip(runner.sink))
    assert runs[0] and runs[0] == runs[1]


def test_machine_socket_streams_that_machine_only(client, runner):
    client.patch("/api/config", json={"machines": 20, "speed": 10})
    client.post("/api/reset")
    client.post("/api/start")
    target = runner.machines()[0]["id"]
    with client.websocket_connect(f"/ws/machines/{target}") as ws:
        for _ in range(3):
            runner.step(NOW)
        got = [ws.receive_json() for _ in range(3)]
    assert all(m["machine_id"] == target for m in got)
    assert [m["cycle"] for m in got] == sorted(m["cycle"] for m in got)


def test_machine_socket_says_goodbye_when_the_machine_is_gone(client, runner):
    target = runner.machines()[-1]["id"]
    with client.websocket_connect(f"/ws/machines/{target}") as ws:
        client.patch("/api/config", json={"machines": 1})
        client.post("/api/start")
        runner.step(NOW)  # bớt máy mới nhất
        assert ws.receive_json() == {"machine_id": target, "gone": True}


def test_two_viewers_of_one_machine_both_keep_receiving(client, runner):
    client.patch("/api/config", json={"machines": 5, "speed": 10})
    client.post("/api/reset")
    client.post("/api/start")
    target = runner.machines()[0]["id"]
    with client.websocket_connect(f"/ws/machines/{target}") as a:
        with client.websocket_connect(f"/ws/machines/{target}") as b:
            runner.step(NOW)
            assert a.receive_json()["machine_id"] == b.receive_json()["machine_id"] == target
        runner.step(NOW)
        assert a.receive_json()["machine_id"] == target


def test_changing_the_fleet_size_while_stopped_applies_at_once(client, runner):
    client.patch("/api/config", json={"machines": 33})
    assert len(runner.machines()) == 33


def test_kafka_is_not_ok_while_messages_wait_without_any_ack(ds, tmp_path):
    class Stuck(MemorySink):
        def queue_len(self):
            return 5
    runner = Runner(ds, Stuck(), tmp_path)
    runner._update_stats()
    assert runner.stats["kafka_ok"] is False
