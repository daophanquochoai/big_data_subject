import { CartesianGrid, ComposedChart, Legend, Line, ReferenceLine, ResponsiveContainer, Scatter, Tooltip, XAxis, YAxis } from "recharts";

export const DEFAULT_SENSORS = ["s2", "s4", "s11", "s15"];
export const INFORMATIVE = ["s2", "s3", "s4", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14", "s15", "s17", "s20", "s21"];
const COLORS = ["var(--accent)", "#8a93a6", "#c08a2d", "#7a5cc4"];

export interface Series {
  name: string;
  rows: number[][]; // Chu kỳ × cột, Chu kỳ 1 ở dòng 0
  dashed?: boolean;
}

// Chọn cảm biến để vẽ.
export function SensorPicker({ value, onChange }: { value: string[]; onChange: (v: string[]) => void }) {
  return (
    <div className="row chips">
      {INFORMATIVE.map((s) => (
        <label key={s} className={value.includes(s) ? "chip on" : "chip"}>
          <input type="checkbox" checked={value.includes(s)}
            onChange={() => onChange(value.includes(s) ? value.filter((x) => x !== s) : [...value, s])} />
          {s}
        </label>
      ))}
    </div>
  );
}

// Mỗi cảm biến một biểu đồ, trục hoành là Chu kỳ; các đường là các chuỗi (máy sinh, máy nguồn).
export function SensorCharts({ columns, series, sensors, points, current }: {
  columns: string[];
  series: Series[];
  sensors: string[];
  points?: { cycle: number; values: Record<string, number> }[]; // Bản tin thật vừa phát
  current?: number;
}) {
  const length = Math.max(...series.map((s) => s.rows.length), ...(points ?? []).map((p) => p.cycle));
  return (
    <div className="charts">
      {sensors.map((sensor) => {
        const col = columns.indexOf(sensor);
        const data = Array.from({ length }, (_, i) => {
          const row: Record<string, number | undefined> = { cycle: i + 1 };
          for (const s of series) row[s.name] = s.rows[i]?.[col];
          return row;
        });
        for (const p of points ?? []) {
          const row = data[p.cycle - 1];
          if (row) row.live = p.values[sensor];
        }
        return (
          <div key={sensor} className="chart">
            <div className="label">{sensor}</div>
            <ResponsiveContainer width="100%" height={180}>
              <ComposedChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="var(--line)" />
                <XAxis dataKey="cycle" type="number" domain={[1, length]} tick={{ fontSize: 11 }} />
                <YAxis domain={["auto", "auto"]} tick={{ fontSize: 11 }} width={56}
                  tickFormatter={(v: number) => v.toFixed(v > 100 ? 0 : 2)} />
                <Tooltip isAnimationActive={false} formatter={(v: number) => v.toFixed(3)} />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                {series.map((s, i) => (
                  <Line key={s.name} dataKey={s.name} dot={false} isAnimationActive={false} connectNulls
                    stroke={COLORS[i % COLORS.length]} strokeWidth={i === 0 ? 1.5 : 1}
                    strokeDasharray={s.dashed ? "4 3" : undefined} />
                ))}
                {points && <Scatter dataKey="live" name="Bản tin vừa phát" fill="var(--bad)" isAnimationActive={false} />}
                {current !== undefined && <ReferenceLine x={current} stroke="var(--bad)" strokeDasharray="3 3" />}
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        );
      })}
    </div>
  );
}
