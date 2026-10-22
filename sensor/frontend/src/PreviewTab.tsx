import { useState } from "react";
import { api, METHODS, SUBSETS, ValidationFailed, type Config, type Method, type Preview } from "./api";
import { DEFAULT_SENSORS, SensorCharts, SensorPicker } from "./SensorCharts";

// Sinh thử một máy cạnh máy nguồn, không đụng đội máy đang chạy.
export function PreviewTab({ config }: { config: Config }) {
  const [method, setMethod] = useState<Method>("warp");
  const [subset, setSubset] = useState<string>("FD002");
  const [seed, setSeed] = useState(0);
  const [sensors, setSensors] = useState(DEFAULT_SENSORS);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [error, setError] = useState("");

  const generate = async (s: number) => {
    setSeed(s);
    setError("");
    try {
      setPreview(await api.preview({ method, subset, seed: s, noise: config.noise, warp_range: config.warp_range }));
    } catch (e) {
      setError(e instanceof ValidationFailed || e instanceof Error ? e.message : String(e));
    }
  };

  const params = preview && Object.entries(preview.params).map(([k, v]) => `${k} = ${v}`).join(", ");

  return (
    <section className="panel">
      <div className="row">
        <select value={method} onChange={(e) => setMethod(e.target.value as Method)}>
          {METHODS.map((m) => <option key={m}>{m}</option>)}
        </select>
        <select value={subset} onChange={(e) => setSubset(e.target.value)}>
          {SUBSETS.map((s) => <option key={s}>{s}</option>)}
        </select>
        <label className="row">Hạt giống
          <input type="number" min={0} value={seed} style={{ width: 80 }}
            onChange={(e) => setSeed(e.target.valueAsNumber || 0)} />
        </label>
        <button onClick={() => generate(seed)}>Sinh thử</button>
        <button onClick={() => generate(seed + 1)}>Sinh thử khác</button>
        <span className="muted">nhiễu {config.noise} σ, co giãn {config.warp_range.join("–")} (theo bảng điều khiển)</span>
      </div>
      {error && <div className="error">{error}</div>}
      <SensorPicker value={sensors} onChange={setSensors} />
      {preview && (
        <>
          <p className="muted">
            Máy sinh: {preview.generated.length} Chu kỳ · nguồn: {preview.base.map((b) =>
              `${b} (${preview.sources[b].length} Chu kỳ)`).join(", ")}{params ? ` · ${params}` : ""}
          </p>
          <SensorCharts columns={preview.columns} sensors={sensors}
            series={[{ name: `sinh (${preview.method})`, rows: preview.generated },
              ...preview.base.map((b) => ({ name: b, rows: preview.sources[b], dashed: true }))]} />
        </>
      )}
    </section>
  );
}
