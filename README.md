# Nền tảng bảo trì dự đoán, v2

Thiết kế gốc: [`../v1/docs/architecture.md`](../v1/docs/architecture.md). Spec của v2:
[bộ giả lập cảm biến](.scratch/gia-lap-cam-bien/spec.md), [phần xử lý Spark](.scratch/xu-ly-spark/spec.md).
Số đo cho báo cáo: [`docs/do-dac.md`](docs/do-dac.md).

## Dựng

Cần Docker (cấp ít nhất 10 GB RAM: `%UserProfile%\.wslconfig` đặt `[wsl2]` `memory=12GB`) và dữ liệu C-MAPSS trong `data/raw/cmapss/`.

```powershell
docker compose up -d --build
```

Lần đầu, `init` chia 709 Máy thành ba nhóm, tính bảng thống kê chuẩn hóa và huấn luyện phiên bản đầu trên 20% nhóm gốc (khoảng 2 phút); bộ giả lập và job luồng chờ nó xong. Các lần sau `init` thấy `models/current.json` và thoát ngay. Bộ giả lập khởi động ở trạng thái dừng: mở giao diện của nó rồi bấm Chạy.

## Giao diện

| Địa chỉ | Là gì |
|---|---|
| http://localhost:8000 | Bộ giả lập: chỉnh số máy, tốc độ, cách sinh dữ liệu, Lỗi chèn |
| http://localhost:3000/d/pdm | Grafana, dashboard "Bảo trì dự đoán" (tài khoản `admin` / `$GRAFANA_PASSWORD`, mặc định `admin`) |
| http://localhost:4040/StreamingQuery/ | Spark UI của job luồng: tốc độ nhận từ Kafka, tốc độ xử lý, thời gian mỗi lô của từng truy vấn |
| http://localhost:8080 | Spark master: 2 worker, ứng dụng đang chạy |
| http://localhost:8081, :8082 | Hai Spark worker và executor của chúng |
| http://localhost:8085 | Kafka UI: topic, partition, Bản tin (Spark giữ offset trong checkpoint nên không có lag ở đây; xem offset từng lô ở Spark UI) |
| http://localhost:8088 | Airflow: DAG `retrain_rul` (huấn luyện lại), không cần đăng nhập. Chạy tay với tham số `fraction` < 1 để thử một lần bị Cổng kiểm định từ chối |
| http://localhost:4041 | Spark UI của job huấn luyện do Airflow gửi lên cụm (chỉ có khi job đang chạy) |
| `localhost:5432` | Postgres, database `pdm`, người dùng `pdm` / `$POSTGRES_PASSWORD` (mặc định `pdm`) |
| `localhost:9094` | Kafka cho client chạy trên máy |

Mọi cổng chỉ mở trên `127.0.0.1` vì Spark UI, Kafka và Kafka UI không có đăng nhập.

## Kiểm tra

```powershell
# Mỗi Máy có dự đoán mỗi 5 Chu kỳ: cửa sổ rơi đúng bội số của 5, hai cửa sổ liên tiếp cách nhau 5; độ trễ đầu-cuối
docker compose exec postgres psql -U pdm -c "WITH r AS (SELECT * FROM predictions WHERE written_at > now() - interval '90 seconds'), g AS (SELECT window_end_cycle - lag(window_end_cycle) OVER (PARTITION BY machine_id ORDER BY window_end_cycle) AS gap FROM r) SELECT (SELECT count(DISTINCT machine_id) FROM r) AS machines, (SELECT count(*) FILTER (WHERE window_end_cycle % 5 <> 0) FROM r) AS not_multiple_of_5, (SELECT count(*) FILTER (WHERE gap <> 5) FROM g) AS bad_gaps, (SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM written_at - window_end))::numeric, 1) FROM r) AS latency_p50_s"

# RAM từng container
docker stats --no-stream
```

## Kiểm thử

```powershell
# Phần Spark (trong image của nó)
docker run --rm -v "${PWD}/spark:/app" pdm-v2 pytest -q tests

# Bộ giả lập
cd sensor/backend; py -3.12 -m venv .venv; .venv/Scripts/python -m pip install -r requirements-dev.txt; .venv/Scripts/python -m pytest -q tests
```
