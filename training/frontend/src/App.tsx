import { useEffect } from 'react';
import { TrainingTab } from './TrainingTab';

export function App() {
  useEffect(() => {
    const url = new URL(location.href);
    url.searchParams.delete('page');
    window.history.replaceState(null, '', url);
  }, []);
  return <div className="control-shell"><header><div><strong>PDM Training Control</strong><span>Điều phối huấn luyện mô hình</span></div><a href="http://localhost:8000/">Về bộ giả lập cảm biến ↗</a></header>
    <main className="control-main"><TrainingTab /></main></div>;
}
