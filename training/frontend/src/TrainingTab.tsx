import { useEffect, useRef, useState, type FormEvent } from 'react';
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import './training.css';
import { api } from './api';

type Metric = { epoch: number; loss: number; accuracy: number; rul_rmse?: number; failing_f1?: number };
type Run = { nodes?:{id:number;name:string;state:string;container_id:string|null}[]; id: number; name: string; status: string; source: string; device: string; world_size: number; epochs: number; completed_epochs: number; progress: number; cancel_requested: boolean; created_at: string; airflow_run_id: string | null; container_id: string | null; artifact_path: string | null; error: string | null; dataset: string; metrics: Metric[]; ranks: { rank: number; device: string; status: string; epoch?: number }[]; logs: { at: string; level: string; message: string }[] };
type Settings = { enabled: boolean; schedule: string; timezone: string; device: string; world_size: number; epochs: number; batch_size: number; learning_rate: number; fraction: number; next_run_at?: string | null };
type System = { host: string; cpu_count: number; gpu_count: number; gpus: { name: string; memory_total_mb: number }[]; docker_available: boolean; image_available: boolean; airflow: { status: string; url: string; dag_id: string } };
type Overview = { current_model: { version: string; framework: string; metrics: { rul_rmse: number; failing_f1: number } } | null; dag: { dag_id: string; is_paused: boolean } | null; cluster: { workers?: { id: string; host: string; cores: number; coresused?: number; state: string }[]; activeapps?: { id: string; name: string }[] } | null; airflow_url: string; errors: string[] };
type NativeRun = { dag_run_id: string; state: string; start_date: string | null };
type Task = { task_id: string; state: string | null };
const active = (run: Run | null) => !!run && ['queued', 'starting', 'running'].includes(run.status);
const labels: Record<string, string> = { queued: 'Đang chờ', starting: 'Chuẩn bị dữ liệu', running: 'Đang train', succeeded: 'Hoàn thành', failed: 'Thất bại', cancelled: 'Đã dừng', success: 'Hoàn thành', skipped: 'Bỏ qua', scheduled: 'Đã lên lịch' };
const number = (v: number | undefined, digits = 3) => v == null ? '—' : v.toLocaleString('vi-VN', { maximumFractionDigits: digits });
const when = (v: string | null | undefined) => v ? new Date(v).toLocaleString('vi-VN', { timeZone: 'Asia/Ho_Chi_Minh' }) : '—';

function Badge({ status }: { status: string }) { return <span className={`training-badge ${status}`}>{labels[status] ?? status}</span>; }

export function TrainingTab() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [system, setSystem] = useState<System | null>(null);
  const [config, setConfig] = useState<Settings | null>(null);
  const [history, setHistory] = useState<{ items: Run[]; total: number; page: number; page_size: number } | null>(null);
  const [nativeRuns, setNativeRuns] = useState<NativeRun[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [selected, setSelected] = useState<number | null>(() => Number(new URLSearchParams(location.search).get('run')) || null);
  const [run, setRun] = useState<Run | null>(null);
  const [page, setPage] = useState(1);
  const [error, setError] = useState<string | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const lock = useRef(false);
  const pending = useRef<number | null>(null);
  const requestId = useRef(crypto.randomUUID());
  const [view, setView] = useState<'gpu' | 'spark'>('gpu');
  useEffect(() => {
    let live = true;
    let loading = false;
    async function load() {
      if (loading || document.visibilityState === 'hidden') return;
      loading = true;
      const results = await Promise.allSettled([api<Overview>('/overview'), api<System>('/gpu/system'), api<Settings>('/gpu/config'), api<{ items: Run[]; total: number; page: number; page_size: number }>(`/gpu/runs?page=${page}`), api<{ dag_runs: NativeRun[] }>('/runs')]);
      if (!live) return;
      if (results[0].status === 'fulfilled') setOverview(results[0].value);
      if (results[1].status === 'fulfilled') setSystem(results[1].value);
      if (results[2].status === 'fulfilled') setConfig(results[2].value);
      if (results[3].status === 'fulfilled') {
        setHistory(results[3].value);
        const id = selected ?? results[3].value.items.find(active)?.id ?? results[3].value.items[0]?.id;
        if (id) {
          try {
            const detail = await api<Run>(`/gpu/runs/${id}`);
            if (live) { setRun(detail); if (pending.current === detail.id && !active(detail)) pending.current = null; }
          } catch (e) { if (live) setError((e as Error).message); }
        }
      }
      if (results[4].status === 'fulfilled') {
        setNativeRuns(results[4].value.dag_runs);
        const last = results[4].value.dag_runs[0];
        if (last) {
          try { const detail = await api<{ task_instances: Task[] }>(`/runs/${encodeURIComponent(last.dag_run_id)}/tasks`); if (live) setTasks(detail.task_instances); } catch { /* Overview reports service errors separately. */ }
        }
      }
      const failed = results.filter(r => r.status === 'rejected');
      setPollError(failed.length ? (failed[0] as PromiseRejectedResult).reason.message : null);
      loading = false;
    }
    void load();
    const timer = setInterval(() => void load(), 4000);
    const visible = () => { if (!document.hidden) void load(); };
    document.addEventListener('visibilitychange', visible);
    return () => { live = false; clearInterval(timer); document.removeEventListener('visibilitychange', visible); };
  }, [selected, page, refresh]);
  const ready = system?.airflow.status === 'healthy' && system.docker_available && system.image_available;
  const occupied = history?.items.some(active) || active(run) || pending.current != null;
  async function action(fn: () => Promise<void>) {
    if (lock.current) return;
    lock.current = true; setBusy(true); setError(null);
    try { await fn(); setRefresh(v => v+1); } catch (e) { setError((e as Error).message); }
    finally { lock.current = false; setBusy(false); }
  }
  const launch = () => action(async () => {
    if (pending.current != null) return;
    const created = await api<Run>('/gpu/runs', { request_id: requestId.current }, 'POST');
    pending.current = created.id; setSelected(created.id); setRun(created); requestId.current = crypto.randomUUID();
  });
  const metric = run?.metrics.at(-1);
  const archivedContainers = run?.nodes ?? [];
  return <div className="training">
    <div className="training-title"><div><h1>Huấn luyện mô hình RUL</h1><p>Airflow điều phối · C-MAPSS thật · Theo dõi tài nguyên và kết quả thực thi</p></div><div className="row"><a href={overview?.airflow_url ?? 'http://localhost:8088'} target="_blank" rel="noreferrer">Mở Airflow ↗</a><button onClick={() => setRefresh(v => v+1)}>Làm mới</button></div></div>
    <div className="training-current"><span>Mô hình đang phục vụ: <strong>{overview?.current_model?.version ?? 'Đang kiểm tra…'}</strong> · Spark GBT</span><span>RUL RMSE <b>{number(overview?.current_model?.metrics.rul_rmse)}</b> · F1 <b>{number(overview?.current_model?.metrics.failing_f1)}</b></span></div>
    <nav className="tabs" aria-label="Chọn đường huấn luyện"><button className={view === 'gpu' ? 'on' : ''} onClick={() => setView('gpu')}>PyTorch DDP</button><button className={view === 'spark' ? 'on' : ''} onClick={() => setView('spark')}>Pipeline Spark đang dùng</button></nav>
    {(error||pollError) && <div role="alert" className="training-error">{error||pollError}</div>}
    {view === 'spark' ? <>
      <section className="panel"><div className="training-section-title"><h2>Airflow → Spark → cổng kiểm định</h2><button disabled={busy || nativeRuns.some(r => ['queued','running'].includes(r.state))} onClick={() => action(async () => { await api('/runs', { request_id: crypto.randomUUID(), fraction: 1, max_failed: 500 }, 'POST'); })}>Huấn luyện lại GBT</button></div><p>DAG <b>retrain_rul</b> · hằng ngày · {overview?.dag ? overview.dag.is_paused ? 'Đang tạm dừng lịch' : 'Lịch đang bật' : 'Chưa kết nối DAG'}</p>
        <div className="training-stages">{['build_training_set','train','validate','gate','promote','keep_current'].map(name => <div key={name}><strong>{({ build_training_set:'Tạo tập train',train:'Huấn luyện',validate:'Kiểm định',gate:'Cổng chất lượng',promote:'Thay model',keep_current:'Giữ model' } as Record<string,string>)[name]}</strong><Badge status={tasks.find(t => t.task_id === name)?.state ?? 'Chờ lượt chạy'} /></div>)}</div>
        <div className="training-ranks">{overview?.cluster?.workers?.map(w => <div key={w.id}><strong>{w.id}</strong><span>{w.host} · {w.state}</span><span>{w.cores} core · đang dùng {w.coresused ?? 0}</span></div>)}</div>
        {overview?.errors.map(value => <p className="error" key={value}>{value}</p>)}
        <p className="muted">Mô hình GBT hiện tại chỉ thay đổi khi ứng viên qua cổng kiểm định. Checkpoint PyTorch bên cạnh không ghi vào current.json.</p>
      </section>
      <section className="panel"><h2>Các lượt pipeline Spark</h2>{nativeRuns.length ? nativeRuns.map(r => <div className="training-history-row" key={r.dag_run_id}><span>{r.dag_run_id}</span><Badge status={r.state} /><time>{when(r.start_date)}</time></div>) : <p>Chưa có lượt pipeline để hiển thị.</p>}</section>
    </> : <>
      <div className="training-health">{[['Docker',system?.docker_available],['Image PyTorch',system?.image_available],['DAG Airflow',system?.airflow.status === 'healthy']].map(([name, ok]) => <span key={String(name)} className={ok ? 'ok' : ''}>● {name}: {ok ? 'Sẵn sàng' : 'Chưa sẵn sàng'}</span>)}</div>
      {!archivedContainers.length && system && system.gpu_count < 2 && <p className="training-note">Máy có {system.gpu_count} GPU. {system.gpu_count === 1 ? 'CUDA chạy được một rank; chưa kiểm chứng nhiều GPU vật lý.' : 'Có thể dùng CPU khi không có GPU.'} CPU/Gloo hai rank là kiểm chứng phân tán thật, không phải hai GPU.</p>}
      <div className="training-layout"><div className="training-main">
        <section className="panel"><h2>{archivedContainers.length?'Topology đã lưu của lượt chạy cũ':'Đường thực thi trên một máy'}</h2><div className="training-topology"><div><strong>Airflow</strong><span>single_node_rul_training</span><small>{run?.source === 'airflow' ? 'Kích hoạt theo lịch' : 'Điều phối yêu cầu chạy'}</small></div><span className="training-arrow">→</span><div><strong>{archivedContainers.length?`${archivedContainers.length} container đã lưu`:system?.host ?? 'Đang đọc host…'}</strong><span>{archivedContainers.length?`${run?.world_size} rank`:`1 máy · ${system?.cpu_count ?? '—'} CPU · ${system?.gpu_count ?? '—'} GPU`}</span><small>{run?.device === 'cuda' ? 'CUDA / NCCL' : 'CPU / Gloo'} · {run?.world_size ?? config?.world_size ?? '—'} rank</small></div></div>
          {!!archivedContainers.length && <div className="training-containers">{archivedContainers.map((item,index)=><div key={item.id}><strong>Container {index}: {item.name}</strong><span>{item.state}</span><code>{item.container_id??'—'}</code></div>)}</div>}
          <div className="training-ranks">{(run?.ranks ?? []).map(rank => <div key={rank.rank}><strong>Rank {rank.rank} <small>{rank.device}</small></strong><Badge status={rank.status} /><span>Gần nhất: epoch {rank.epoch ?? 0}/{run?.epochs}</span></div>)}</div>
          {!archivedContainers.length&&system?.gpus.map((gpu, i) => <p key={i} className="muted">{gpu.name} · {number(gpu.memory_total_mb/1024,1)} GB VRAM</p>)}
        </section>
        <section className="panel"><div className="training-section-title"><h2>{run?.name ?? 'Chưa có lượt RUL PyTorch'}</h2>{run && <Badge status={run.status} />}</div>
          {!run ? <p>Lưu cấu hình rồi bấm Chạy ngay. Spark chuẩn bị cùng 33 đặc trưng C-MAPSS, sau đó các rank cùng học một ứng viên RUL.</p> : <>
            <div className="training-section-title"><span>{when(run.created_at)} · {run.source === 'airflow' ? 'Theo lịch' : 'Chạy tay'}</span>{active(run) && <button disabled={busy || run.cancel_requested} onClick={() => action(async () => { await api(`/gpu/runs/${run.id}/cancel`, {}, 'POST'); })}>{run.cancel_requested ? 'Đang yêu cầu dừng…' : 'Dừng lượt train'}</button>}</div>
            <div className="training-progress-label"><strong>Epoch {run.completed_epochs}/{run.epochs}</strong><span>{number(run.progress,0)}%</span></div><progress max={100} value={run.progress} aria-label="Tiến độ huấn luyện" />
            <div className="training-metrics"><div><span>Loss</span><b>{number(metric?.loss,4)}</b></div><div><span>RUL RMSE</span><b>{number(metric?.rul_rmse)}</b></div><div><span>F1 sắp hỏng</span><b>{number(metric?.failing_f1)}</b></div><div><span>Epoch</span><b>{run.completed_epochs}/{run.epochs}</b></div></div>
            {run.error && <p role="alert" className="training-error">{run.error}</p>}
            {run.metrics.length > 0 && <div className="training-chart"><ResponsiveContainer width="100%" height={240} minWidth={0}><LineChart data={run.metrics}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="epoch" /><YAxis width={45} /><Tooltip /><Legend /><Line name="Loss" dataKey="loss" stroke="#2f6fde" dot={false} isAnimationActive={false} /></LineChart></ResponsiveContainer></div>}
            <details><summary>Metric từng epoch</summary><div className="training-table-wrap"><table><thead><tr><th>Epoch</th><th>Loss</th><th>RUL RMSE</th><th>F1</th></tr></thead><tbody>{run.metrics.map(m => <tr key={m.epoch}><td>{m.epoch}</td><td>{number(m.loss,4)}</td><td>{number(m.rul_rmse)}</td><td>{number(m.failing_f1)}</td></tr>)}</tbody></table></div></details>
            <div className="training-trace"><span>Airflow run</span><code>{run.airflow_run_id ?? 'Đang chờ'}</code><span>Container</span><code>{run.container_id ?? archivedContainers[0]?.container_id ?? 'Chưa tạo'}</code>{run.artifact_path && <><span>Checkpoint</span><code>{run.artifact_path}</code></>}</div>
          </>}
          <p className="muted">Ứng viên PyTorch · C-MAPSS thật · 33 đặc trưng · tách nhóm train/validation/replay. Không tự thay model Spark đang phục vụ.</p>
        </section>
        {run && <section className="panel"><h2>Nhật ký thực thi</h2><ol className="training-logs">{run.logs.map((entry,i) => <li key={entry.at+i}><time>{when(entry.at)}</time><span className={entry.level === 'error' ? 'error' : ''}>{entry.message}</span></li>)}</ol></section>}
      </div><aside>{config && <ConfigForm config={config} system={system} busy={busy} onSave={body => action(async () => { setConfig(await api<Settings>('/gpu/config', body, 'PUT')); })} />}
        <button className="training-primary" disabled={!ready || occupied || busy || !!pollError} onClick={launch}>{busy ? 'Đang gửi…' : occupied ? 'Máy đang có lượt train' : 'Chạy ngay qua Airflow'}</button>
      </aside></div>
      <section className="panel"><h2>Lịch sử ứng viên PyTorch RUL</h2>{history?.items.length ? history.items.map(value => <button className={`training-history-row ${run?.id === value.id ? 'selected' : ''}`} key={value.id} onClick={() => setSelected(value.id)}><span>{value.name}<small>{value.device === 'cuda' ? 'GPU' : 'CPU'} · {value.world_size} rank · {value.source === 'airflow' ? 'Theo lịch' : 'Chạy tay'}</small></span><Badge status={value.status} /><span>{when(value.created_at)}</span></button>) : <p>Chưa có lượt train trong dự án này.</p>}<div className="row"><button disabled={page === 1} onClick={() => setPage(v => v-1)}>Trước</button><span>Trang {page} · {history?.total ?? 0} lượt</span><button disabled={!history || page*history.page_size >= history.total} onClick={() => setPage(v => v+1)}>Sau</button></div></section>
    </>}
  </div>;
}

function ConfigForm({ config, system, busy, onSave }: { config: Settings; system: System | null; busy: boolean; onSave: (body: Settings) => void }) {
  const [draft, setDraft] = useState(config);
  const [dirty, setDirty] = useState(false);
  useEffect(() => { if (!dirty) setDraft(config); }, [config, dirty]);
  function field(key: keyof Settings, value: string | number | boolean ) { setDirty(true); setDraft(v => ({ ...v, [key]: value, ...(key === 'device' ? { world_size: value === 'cuda' ? Math.max(1,Math.min(2,system?.gpu_count ?? 1)) : 2 } : {}) })); }
  function submit(event: FormEvent) { event.preventDefault(); const { next_run_at: _, ...value } = draft; onSave(value); setDirty(false); }
  return <form className="panel training-config" onSubmit={submit}><h2>Cấu hình lượt tiếp theo</h2><p className="muted">Lưu trước khi chạy. Lượt đang thực thi giữ nguyên snapshot.</p>
    <p className="muted">Chạy bằng Docker trên máy hiện tại.</p>
    <label>Thiết bị<select value={draft.device} onChange={e => field('device',e.target.value)}><option value="cpu">CPU / Gloo</option><option value="cuda" disabled={!system?.gpu_count}>GPU / CUDA</option></select></label>
    <div className="training-fields">{([['world_size','Số rank',1,draft.device === 'cuda' ? system?.gpu_count ?? 1 : 32,1],['epochs','Số epoch',1,100,1],['batch_size','Batch size',8,4096,1],['learning_rate','Learning rate',.000001,1,'any'],['fraction','Tỷ lệ máy train',.01,1,.01]] as const).map(([key,label,min,max,step]) => <label key={key}>{label}<input required type="number" min={min} max={max} step={step} value={draft[key]} onChange={e => field(key,e.target.valueAsNumber)} /></label>)}</div>
    <label className="row"><input type="checkbox" checked={draft.enabled} onChange={e => field('enabled',e.target.checked)} />Bật lịch tự động</label><label>Chu kỳ<select value={draft.schedule} onChange={e => field('schedule',e.target.value)}><option value="*/5 * * * *">Mỗi 5 phút</option><option value="0 * * * *">Mỗi giờ</option><option value="0 2 * * *">Hằng ngày 02:00</option></select></label>
    <p className="muted">Giờ Việt Nam · {config.enabled ? 'Lượt tới: '+when(config.next_run_at) : 'Lịch đang tắt'}</p><button disabled={busy}>Lưu cấu hình</button>
  </form>;
}
