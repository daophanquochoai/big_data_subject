import { useEffect, useState } from "react";
import { api, type Config } from "./api";

// Lưu cấu hình hiện hành thành kịch bản có tên; nạp thì thay cấu hình và tạo lại đội máy.
export function Scenarios({ onLoaded }: { onLoaded: (c: Config) => void }) {
  const [names, setNames] = useState<string[]>([]);
  const [selected, setSelected] = useState("");

  const refresh = () => api.scenarios().then(setNames).catch(() => undefined);
  useEffect(() => {
    refresh();
  }, []);

  const fail = (e: unknown) => alert(e instanceof Error ? e.message : String(e));

  const save = async () => {
    const name = prompt("Tên kịch bản (chữ, số, - và _):", selected || "");
    if (!name) return;
    try {
      await api.saveScenario(name);
      setSelected(name);
      await refresh();
    } catch (e) {
      fail(e);
    }
  };

  const load = () => selected && api.loadScenario(selected).then(onLoaded).catch(fail);

  const remove = async () => {
    if (!selected || !confirm(`Xóa kịch bản ${selected}?`)) return;
    try {
      await api.deleteScenario(selected);
      setSelected("");
      await refresh();
    } catch (e) {
      fail(e);
    }
  };

  return (
    <div className="row">
      <select value={selected} onChange={(e) => setSelected(e.target.value)}>
        <option value="">— kịch bản —</option>
        {names.map((n) => <option key={n}>{n}</option>)}
      </select>
      <button disabled={!selected} onClick={load} title="Thay cấu hình và tạo lại đội máy">Nạp</button>
      <button onClick={save}>Lưu</button>
      <button disabled={!selected} onClick={remove}>Xóa</button>
    </div>
  );
}
