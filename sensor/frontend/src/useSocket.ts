import { useEffect, useRef, useState } from "react";

// Nhận từng tin JSON từ một WebSocket; rớt kết nối thì tự nối lại sau 2 giây.
export function useSocket<T>(path: string | null, onMessage: (data: T) => void) {
  const [connected, setConnected] = useState(false);
  const handler = useRef(onMessage);
  handler.current = onMessage; // luôn gọi hàm mới nhất mà không phải mở lại kết nối

  useEffect(() => {
    if (!path) return;
    let ws: WebSocket | null = null;
    let retry: number | undefined;
    let closed = false;

    const open = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${location.host}${path}`);
      ws.onopen = () => setConnected(true);
      ws.onmessage = (e) => handler.current(JSON.parse(e.data));
      ws.onclose = () => {
        setConnected(false);
        if (!closed) retry = window.setTimeout(open, 2000);
      };
    };
    open();
    return () => {
      closed = true;
      window.clearTimeout(retry);
      ws?.close();
    };
  }, [path]);

  return connected;
}
