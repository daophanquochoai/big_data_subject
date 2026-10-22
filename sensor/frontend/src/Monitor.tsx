import { useEffect, useState } from "react";
import { Line, LineChart, ResponsiveContainer, YAxis } from "recharts";
import { api, METHODS, type MachineRow, type Stats } from "./api";

export interface Point {
  sent: number;
  acked: number;
}

const FAULT_LABELS = { bad_value: "Sai", drop: "Mất", late: "Trễ", duplicate: "Trùng" } as const;

export function StatTiles({ stats, history }: { stats: Stats | null; history: Point[] }) {
  if (!stats) return <div className="tiles">Đang chờ số liệu…</div>;
  const n = (x: number) => x.toLocaleString("vi");
  return (
    <div className="tiles">
      <div className="tile wide">
        <div className="label">Gửi / Kafka xác nhận (events/giây)</div>
        <div className="value">{n(stats.sent_per_s)} / {n(stats.acked_per_s)}</div>
        <ResponsiveContainer width="100%" height={40}>
          <LineChart data={history}>
            <YAxis hide domain={[0, "auto"]} />
            <Line dataKey="sent" dot={false} isAnimationActive={false} stroke="var(--accent)" />
            <Line dataKey="acked" dot={false} isAnimationActive={false} stroke="var(--ok)" strokeDasharray="3 3" />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <Tile label="Máy đang sống" value={n(stats.alive)} />
      <Tile label="Đã hỏng" value={n(stats.failed)} />
      <Tile label="Lỗi chèn" wide small value={Object.entries(FAULT_LABELS)
        .map(([k, l]) => `${l} ${n(stats.faults[k as keyof typeof FAULT_LABELS])}`).join(" · ")} />
      <Tile label="Nhịp quá hạn" value={n(stats.overruns)} warn={stats.overruns > 0} />
      <Tile label="Lỗi Kafka/giây · bỏ do nghẽn" value={`${n(stats.kafka_errors_per_s)} · ${n(stats.backpressure_dropped)}`}
        warn={stats.kafka_errors_per_s + stats.backpressure_dropped > 0} />
      <Tile label="Hàng đợi gửi" value={n(stats.queue)} />
    </div>
  );
}

function Tile({ label, value, warn, wide, small }: {
  label: string; value: string; warn?: boolean; wide?: boolean; small?: boolean;
}) {
  return (
    <div className={["tile", warn && "warn", wide && "wide"].filter(Boolean).join(" ")}>
      <div className="label">{label}</div>
      <div className={small ? "value small" : "value"}>{value}</div>
    </div>
  );
}

const COLUMNS: [keyof MachineRow, string][] = [
  ["id", "Mã"], ["method", "Phương pháp"], ["cycle", "Chu kỳ"], ["life", "Đời"], ["rul", "RUL thật"],
];

function origin(m: MachineRow) {
  const params = Object.entries(m.params).map(([k, v]) => `${k}=${v}`).join(", ");
  return params ? `${m.base.join(" + ")} (${params})` : m.base.join(" + ");
}

export function MachineTable({ onSelect }: { onSelect?: (id: string) => void }) {
  const [method, setMethod] = useState("");
  const [sort, setSort] = useState<keyof MachineRow>("id");
  const [desc, setDesc] = useState(false);
  const [page, setPage] = useState(1);
  const [data, setData] = useState<{ total: number; page_size: number; items: MachineRow[] } | null>(null);

  useEffect(() => {
    let alive = true;
    const load = () => api.machines({ method: method || undefined, sort, desc, page })
      .then((d) => alive && setData(d)).catch(() => undefined);
    load();
    const t = window.setInterval(load, 2000);
    return () => {
      alive = false;
      window.clearInterval(t);
    };
  }, [method, sort, desc, page]);

  const pages = data ? Math.max(1, Math.ceil(data.total / data.page_size)) : 1;
  const sortBy = (k: keyof MachineRow) => {
    if (k === sort) setDesc(!desc);
    else { setSort(k); setDesc(false); }
  };

  return (
    <section className="machines">
      <div className="row">
        <h2>Máy ảo ({data?.total ?? 0})</h2>
        <select value={method} onChange={(e) => { setMethod(e.target.value); setPage(1); }}>
          <option value="">mọi phương pháp</option>
          {METHODS.map((m) => <option key={m}>{m}</option>)}
        </select>
      </div>
      <table>
        <thead>
          <tr>
            {COLUMNS.map(([k, label]) => (
              <th key={k} onClick={() => sortBy(k)}>{label}{k === sort ? (desc ? " ▼" : " ▲") : ""}</th>
            ))}
            <th>Nguồn</th>
          </tr>
        </thead>
        <tbody>
          {data?.items.map((m) => (
            <tr key={m.id} onClick={() => onSelect?.(m.id)} className={onSelect ? "clickable" : undefined}>
              <td>{m.id}</td><td>{m.method}</td><td>{m.cycle}</td><td>{m.life}</td><td>{m.rul}</td>
              <td className="muted">{origin(m)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="row">
        <button disabled={page <= 1} onClick={() => setPage(page - 1)}>‹</button>
        <span>Trang {page}/{pages}</span>
        <button disabled={page >= pages} onClick={() => setPage(page + 1)}>›</button>
      </div>
    </section>
  );
}
