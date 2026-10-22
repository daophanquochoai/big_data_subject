// Kiểu và lời gọi tới BE. Kiểu cấu hình khớp engine/config.py.

export type Method = "gauss" | "warp" | "blend" | "parametric";
export const METHODS: Method[] = ["gauss", "warp", "blend", "parametric"];
export const SUBSETS = ["FD001", "FD002", "FD003", "FD004"] as const;
export type FaultType = "bad_value" | "drop" | "late" | "duplicate";

export interface Config {
  machines: number;
  speed: number;
  noise: number;
  subsets: string[];
  method_weights: Partial<Record<Method, number>>;
  warp_range: [number, number];
  faults: Record<FaultType, number>;
  late_max_s: number;
  seed: number;
}

export interface Stats {
  running: boolean;
  kafka_ok: boolean;
  sent_per_s: number;
  acked_per_s: number;
  alive: number;
  failed: number;
  faults: Record<FaultType, number>;
  kafka_errors_per_s: number;
  backpressure_dropped: number;
  overruns: number;
  queue: number;
}

export interface MachineRow {
  id: string;
  method: Method;
  base: string[];
  params: Record<string, number>;
  cycle: number;
  life: number;
  rul: number;
}

export interface Preview {
  method: Method;
  base: string[];
  params: Record<string, number>;
  columns: string[];
  generated: number[][];
  sources: Record<string, number[][]>;
}

export interface MachineDetail {
  id: string;
  method: Method;
  base: string[];
  params: Record<string, number>;
  cycle: number;
  columns: string[];
  lifecycle: number[][];
}

export interface FieldError {
  loc: string[];
  msg: string;
}

// Lỗi 422 của BE mang danh sách trường sai, để ô tương ứng đổi đỏ.
export class ValidationFailed extends Error {
  constructor(public errors: FieldError[]) {
    super(errors.map((e) => e.msg).join("; "));
  }
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const r = await fetch(path, {
    method,
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await r.json().catch(() => null);
  if (r.status === 422) throw new ValidationFailed(data?.detail ?? []);
  if (!r.ok) throw new Error(`${method} ${path}: ${r.status}`);
  return data as T;
}

export const api = {
  config: () => call<Config>("GET", "/api/config"),
  patchConfig: (patch: Partial<Config>) => call<Config>("PATCH", "/api/config", patch),
  start: () => call("POST", "/api/start"),
  stop: () => call("POST", "/api/stop"),
  reset: () => call("POST", "/api/reset"),
  preview: (req: { method: Method; subset: string; seed: number; noise?: number; warp_range?: [number, number] }) =>
    call<Preview>("POST", "/api/preview", req),
  scenarios: () => call<string[]>("GET", "/api/scenarios"),
  saveScenario: (name: string) => call<Config>("PUT", `/api/scenarios/${encodeURIComponent(name)}`),
  loadScenario: (name: string) => call<Config>("POST", `/api/scenarios/${encodeURIComponent(name)}/load`),
  deleteScenario: (name: string) => call("DELETE", `/api/scenarios/${encodeURIComponent(name)}`),
  machine: (id: string) => call<MachineDetail>("GET", `/api/machines/${encodeURIComponent(id)}`),
  machines: (q: { method?: string; sort: string; desc: boolean; page: number }) => {
    const p = new URLSearchParams({ sort: q.sort, desc: String(q.desc), page: String(q.page) });
    if (q.method) p.set("method", q.method);
    return call<{ total: number; page_size: number; items: MachineRow[] }>("GET", `/api/machines?${p}`);
  },
};
