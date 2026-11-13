import { useEffect, useState } from "react";
import { api, type Config, type Stats } from "./api";
import { Controls } from "./Controls";
import { MachineTable, StatTiles, type Point } from "./Monitor";
import { MachineDrawer } from "./MachineDrawer";
import { PreviewTab } from "./PreviewTab";
import { Scenarios } from "./Scenarios";
import { useSocket } from "./useSocket";

export function App() {
  const [config, setConfig] = useState<Config | null>(null);
  const [stats, setStats] = useState<Stats | null>(null);
  const [history, setHistory] = useState<Point[]>([]);
  const [tab, setTab] = useState<"monitor" | "preview">('monitor');
  const [watching, setWatching] = useState<string | null>(null);
  const selectTab = (value: "monitor" | "preview") => {
    setTab(value);
    const url = new URL(location.href);
    url.searchParams.delete('tab');
    window.history.replaceState(null, '', url);
  };

  useEffect(() => {
    if (new URLSearchParams(location.search).get('tab') === 'training') location.replace('http://localhost:8001/'+location.search);
    api.config().then(setConfig);
  }, []);

  const connected = useSocket<Stats>("/ws/stats", (s) => {
    setStats(s);
    setHistory((h) => [...h.slice(-59), { sent: s.sent_per_s, acked: s.acked_per_s }]);
  });

  const run = (action: () => Promise<unknown>) => () => action().catch((e) => alert(e.message));

  return (
    <div className="app">
      <header>
        <h1>Bộ giả lập cảm biến</h1>
        <span className={stats?.running ? "badge ok" : "badge"}>{stats?.running ? "● Đang chạy" : "○ Dừng"}</span>
        <span className={stats?.kafka_ok ? "badge ok" : "badge bad"}>Kafka: {stats?.kafka_ok ? "OK" : "lỗi"}</span>
        {!connected && <span className="badge bad">Mất kết nối</span>}
        <div className="spacer" />
        <button onClick={run(api.start)}>Chạy</button>
        <button onClick={run(api.stop)}>Dừng</button>
        <button onClick={run(api.reset)} title="Sinh lại đội máy từ hạt giống">Tạo lại</button>
        <Scenarios onLoaded={setConfig} />
      </header>
      <div className="body">
        {config && <Controls config={config} onSaved={setConfig} />}
        <main>
          <nav className="tabs">
            <button className={tab === "monitor" ? "on" : ""} onClick={() => selectTab("monitor")}>Theo dõi</button>
            <button className={tab === "preview" ? "on" : ""} onClick={() => selectTab("preview")}>Xem trước</button>
            <a className="training-link" href="http://localhost:8001/">Huấn luyện ↗</a>
          </nav>
          {tab === "monitor" && (
            <>
              <StatTiles stats={stats} history={history} />
              <MachineTable onSelect={setWatching} />
            </>
          )}
          {tab === "preview" && config && <PreviewTab config={config} />}
        </main>
        {watching && <MachineDrawer id={watching} onClose={() => setWatching(null)} />}
      </div>
    </div>
  );
}
