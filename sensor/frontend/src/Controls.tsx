import { useEffect, useRef, useState, type ReactNode } from "react";
import { api, METHODS, SUBSETS, ValidationFailed, type Config, type FaultType } from "./api";

const FAULTS: [FaultType, string][] = [
  ["bad_value", "Giá trị sai"], ["drop", "Mất"], ["late", "Trễ"], ["duplicate", "Trùng"],
];

// Sửa tới đâu gửi PATCH tới đó sau 300 ms ngừng gõ; ô sai đổi đỏ và hiện lý do BE trả về.
export function Controls({ config, onSaved }: { config: Config; onSaved: (c: Config) => void }) {
  const [draft, setDraft] = useState(config);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const pending = useRef<Partial<Config>>({});
  const timer = useRef<number>();

  // Cấu hình đổi từ nơi khác (ví dụ nạp kịch bản) thì hiện theo, trừ khi đang có sửa chưa gửi.
  useEffect(() => {
    if (Object.keys(pending.current).length === 0) setDraft(config);
  }, [config]);

  const flush = async () => {
    const patch = pending.current;
    pending.current = {};
    try {
      const saved = await api.patchConfig(patch);
      setErrors((e) => Object.fromEntries(Object.entries(e).filter(([k]) => !(k in patch))));
      onSaved(saved);
    } catch (e) {
      if (!(e instanceof ValidationFailed)) {
        // Lỗi mạng hay lỗi BE: giữ lại chỗ sửa để lần sửa kế tiếp gửi lại, và báo ở ô đầu tiên.
        pending.current = { ...patch, ...pending.current };
        setErrors((old) => ({ ...old, [Object.keys(patch)[0]]: `Chưa lưu được: ${(e as Error).message}` }));
        return;
      }
      setErrors((old) => {
        const next = { ...old };
        for (const err of e.errors) next[err.loc[0] in patch ? err.loc[0] : Object.keys(patch)[0]] = err.msg;
        return next;
      });
    }
  };

  const set = <K extends keyof Config>(key: K, value: Config[K]) => {
    setDraft((d) => ({ ...d, [key]: value }));
    pending.current = { ...pending.current, [key]: value };
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(flush, 300);
  };

  const totalWeight = METHODS.reduce((s, m) => s + (draft.method_weights[m] ?? 0), 0);

  return (
    <aside className="controls">
      <h2>Tốc độ và quy mô</h2>
      <Field label="Số Máy ảo N" error={errors.machines}>
        <input type="number" min={1} max={10000} value={draft.machines}
          onChange={(e) => set("machines", e.target.valueAsNumber)} />
      </Field>
      <Field label={`Chu kỳ/giây mỗi máy k = ${draft.speed}`} error={errors.speed}>
        <input type="range" min={0.1} max={10} step={0.1} value={draft.speed}
          onChange={(e) => set("speed", e.target.valueAsNumber)} />
      </Field>
      <p className="hint">Tải ≈ {Math.round(draft.machines * draft.speed).toLocaleString("vi")} events/giây</p>

      <h2>Sinh dữ liệu</h2>
      <Field label="Tập con" error={errors.subsets}>
        <div className="row">
          {SUBSETS.map((s) => (
            <label key={s} className="check">
              <input type="checkbox" checked={draft.subsets.includes(s)}
                onChange={(e) => set("subsets", e.target.checked
                  ? SUBSETS.filter((x) => x === s || draft.subsets.includes(x))
                  : draft.subsets.filter((x) => x !== s))} />
              {s}
            </label>
          ))}
        </div>
      </Field>
      <Field label="Tỷ lệ trộn phương pháp" error={errors.method_weights}>
        {METHODS.map((m) => {
          const w = draft.method_weights[m] ?? 0;
          return (
            <label key={m} className="slider">
              <span>{m}</span>
              <input type="range" min={0} max={10} step={1} value={w}
                onChange={(e) => set("method_weights", { ...draft.method_weights, [m]: e.target.valueAsNumber })} />
              <span>{totalWeight ? Math.round((100 * w) / totalWeight) : 0}%</span>
            </label>
          );
        })}
      </Field>
      <Field label={`Mức nhiễu = ${draft.noise} σ`} error={errors.noise}>
        <input type="range" min={0} max={1} step={0.01} value={draft.noise}
          onChange={(e) => set("noise", e.target.valueAsNumber)} />
      </Field>
      <Field label="Khoảng co giãn (warp)" error={errors.warp_range}>
        <div className="row">
          <input type="number" step={0.05} value={draft.warp_range[0]}
            onChange={(e) => set("warp_range", [e.target.valueAsNumber, draft.warp_range[1]])} />
          <span>–</span>
          <input type="number" step={0.05} value={draft.warp_range[1]}
            onChange={(e) => set("warp_range", [draft.warp_range[0], e.target.valueAsNumber])} />
        </div>
      </Field>

      <h2>Lỗi chèn</h2>
      {FAULTS.map(([kind, label]) => (
        <Field key={kind} label={`${label}: ${(draft.faults[kind] * 100).toFixed(1)}% Bản tin`}
          error={errors.faults}>
          <input type="range" min={0} max={0.2} step={0.001} value={draft.faults[kind]}
            onChange={(e) => set("faults", { ...draft.faults, [kind]: e.target.valueAsNumber })} />
        </Field>
      ))}
      <Field label={`Bản tin trễ: giữ lại tối đa ${draft.late_max_s} giây`} error={errors.late_max_s}>
        <input type="range" min={1} max={60} step={1} value={draft.late_max_s}
          onChange={(e) => set("late_max_s", e.target.valueAsNumber)} />
      </Field>

      <h2>Hạt giống</h2>
      <Field label="Áp dụng ở lần Tạo lại kế tiếp" error={errors.seed}>
        <input type="number" min={0} value={draft.seed} onChange={(e) => set("seed", e.target.valueAsNumber)} />
      </Field>
    </aside>
  );
}

function Field({ label, error, children }: { label: string; error?: string; children: ReactNode }) {
  return (
    <div className={error ? "field invalid" : "field"}>
      <div className="label">{label}</div>
      {children}
      {error && <div className="error">{error}</div>}
    </div>
  );
}
