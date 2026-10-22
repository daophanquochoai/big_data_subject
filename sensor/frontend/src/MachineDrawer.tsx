import { useEffect, useState } from "react";
import { api, type MachineDetail } from "./api";
import { DEFAULT_SENSORS, SensorCharts, SensorPicker } from "./SensorCharts";
import { useSocket } from "./useSocket";

interface Live {
  machine_id: string;
  cycle: number;
  truth?: { rul: number; method: string; fault: string | null };
  gone?: boolean;
  [column: string]: unknown;
}

const MAX_POINTS = 1000;

// Đường mảnh: trọn vòng đời đã sinh (chưa nhiễu). Chấm: Bản tin thật vừa phát, có nhiễu và lỗi.
export function MachineDrawer({ id, onClose }: { id: string; onClose: () => void }) {
  const [detail, setDetail] = useState<MachineDetail | null>(null);
  const [live, setLive] = useState<Live[]>([]);
  const [gone, setGone] = useState(false);
  const [sensors, setSensors] = useState(DEFAULT_SENSORS);

  useEffect(() => {
    setDetail(null);
    setLive([]);
    setGone(false);
    api.machine(id).then(setDetail).catch(() => setGone(true));
  }, [id]);

  useSocket<Live>(`/ws/machines/${encodeURIComponent(id)}`, (m) => {
    if (m.gone) setGone(true);
    else setLive((l) => [...l.slice(-(MAX_POINTS - 1)), m]);
  });

  const last = live[live.length - 1];
  const faults = live.filter((m) => m.truth?.fault).length;

  return (
    <aside className="drawer">
      <div className="row">
        <h2>{id}</h2>
        <div className="spacer" />
        <button onClick={onClose}>Đóng</button>
      </div>
      {gone && <div className="error">Máy đã hỏng hoặc bị bớt khỏi đội; biểu đồ dừng ở Bản tin cuối.</div>}
      {detail && (
        <p className="muted">
          {detail.method} từ {detail.base.join(" + ")} · đời {detail.lifecycle.length} Chu kỳ
          {last && ` · Chu kỳ ${last.cycle}, RUL thật ${last.truth?.rul}`}
          {` · ${live.length} Bản tin đã nhận, ${faults} có lỗi chèn`}
        </p>
      )}
      <SensorPicker value={sensors} onChange={setSensors} />
      {detail && (
        <SensorCharts columns={detail.columns} sensors={sensors} current={last?.cycle ?? detail.cycle}
          series={[{ name: "vòng đời đã sinh", rows: detail.lifecycle }]}
          points={live.map((m) => ({ cycle: m.cycle, values: m as unknown as Record<string, number> }))} />
      )}
    </aside>
  );
}
