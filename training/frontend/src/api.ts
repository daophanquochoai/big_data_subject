export async function api<T>(path: string, body?: unknown, method = 'GET'): Promise<T> {
  const response = await fetch('/api/training'+path, { method, headers: body ? { 'Content-Type':'application/json' } : {}, body: body ? JSON.stringify(body) : undefined });
  if (response.status === 204) return undefined as T;
  const data = await response.json();
  if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : Array.isArray(data.detail) ? data.detail.map((e: { msg: string }) => e.msg).join('; ') : `Không kết nối được (${response.status})`);
  return data as T;
}
