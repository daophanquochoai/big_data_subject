# Spec: Phần xử lý Spark của v2

Status: draft, chờ duyệt

Thuật ngữ in hoa (Máy, Máy ảo, Bản tin, Chu kỳ, RUL, Sắp hỏng, Điều kiện vận hành, Lệnh bảo trì, Khoảng an toàn, Mô hình đang dùng, Cổng kiểm định, Lỗi chèn) theo Mục 15 của `v1/docs/architecture.md`. Bộ giả lập là `v2/sensor` (spec: `v2/.scratch/gia-lap-cam-bien/spec.md`).

## Problem Statement

v2 đã có bộ giả lập cảm biến đẩy Bản tin vào Kafka, nhưng chưa có gì đọc chúng. Thiết kế đầy đủ của hệ thống (làm sạch, đặc trưng, hai mô hình GBT, Lệnh bảo trì, kho, huấn luyện lại bằng Airflow, Grafana) nằm ở `v1/docs/architecture.md`, và mã Spark của v1 đã chạy được trên cụm cho phần chấm điểm. Nhưng mã đó không dùng được nguyên cho v2:

- cửa sổ trượt tính theo giây, trong khi bộ giả lập v2 đổi được số Chu kỳ mỗi giây k ngay khi đang chạy;
- Bản tin v2 mang trường `truth` (đáp án) mà phần xử lý không được dùng;
- bộ giả lập lấy nguồn từ mọi máy train của C-MAPSS, nên mô hình huấn luyện trên chính các máy đó sẽ "thấy trước" dữ liệu luồng;
- v1 tắt Spark UI và không mở giao diện nào ra ngoài ngoài master và Grafana, nên không thấy được Spark nhận và xử lý dữ liệu Kafka ra sao.

## Solution

Mang mã v1 sang `v2/spark/` rồi sửa. Toàn bộ thiết kế v1 được dựng, chia năm lát chạy được đầu-cuối. Đặc trưng cửa sổ tính theo Chu kỳ bằng trạng thái theo từng máy (`applyInPandasWithState`), cùng một hàm pandas dùng cho huấn luyện. Bước khởi tạo chia 709 máy thành ba nhóm như v1 và bộ giả lập chỉ lấy nguồn từ nhóm phát lại. Mọi giao diện (Spark master, worker, Spark UI của job luồng, Airflow, Kafka UI, Grafana, bộ giả lập) mở ra cổng trên máy. Dashboard Grafana thiết kế lại quanh câu hỏi "bao nhiêu thiết bị cần kiểm tra gấp".

## User Stories

1. Là thành viên nhóm, tôi muốn một lệnh `docker compose up` trong `v2/` dựng toàn bộ: Kafka, bộ giả lập, cụm Spark, job luồng, Postgres, Grafana, Airflow, Kafka UI.
2. Là thành viên nhóm, tôi muốn hệ thống tự chia nhóm Máy, tính bảng thống kê chuẩn hóa và huấn luyện phiên bản đầu khi chưa có Mô hình đang dùng, như v1.
3. Là người viết báo cáo, tôi muốn bộ giả lập chỉ phát lại vòng đời của nhóm phát lại, để số đo trên luồng không bị thổi phồng vì mô hình đã thấy dữ liệu.
4. Là thành viên nhóm, tôi muốn đặc trưng cửa sổ luôn là 30 Chu kỳ gần nhất, bước 5 Chu kỳ, dù k là bao nhiêu, để đổi tốc độ bộ giả lập không làm sai mô hình.
5. Là thành viên nhóm, tôi muốn đặc trưng lúc huấn luyện và lúc chấm điểm tính bằng cùng một hàm, để không lệch nhau.
6. Là thành viên nhóm, tôi muốn trường `truth` bị bỏ ngay khi Spark đọc Bản tin, để mô hình và phần làm sạch không thể dùng đáp án.
7. Là thành viên nhóm, tôi muốn Bản tin trùng, trễ, đảo thứ tự và giá trị sai được xử lý trước khi tính đặc trưng.
8. Là thành viên nhóm, tôi muốn mỗi Máy ảo có một dự đoán RUL và xác suất Sắp hỏng mỗi 5 Chu kỳ, ghi vào Postgres kèm phiên bản mô hình và thời điểm.
9. Là người vận hành, tôi muốn một Lệnh bảo trì được tạo khi Máy Sắp hỏng, với Chu kỳ đề xuất bảo trì, mỗi Máy tối đa một lệnh.
10. Là thành viên nhóm, tôi muốn Bản tin đã làm sạch và sự kiện hỏng được lưu vào kho Parquet, để huấn luyện lại.
11. Là thành viên nhóm, tôi muốn DAG Airflow huấn luyện lại trên nhóm gốc cộng các Máy ảo đã hỏng, qua Cổng kiểm định, và job luồng tự dùng mô hình mới mà không khởi động lại.
12. Là thành viên nhóm, tôi muốn xem Spark nhận dữ liệu từ Kafka và xử lý ra sao: tốc độ nhận, tốc độ xử lý, thời gian mỗi lô, số trạng thái, offset, trên Spark UI.
13. Là thành viên nhóm, tôi muốn xem topic, partition, Bản tin trên một giao diện Kafka. (Độ trễ đọc của Spark không hiện ở Kafka UI vì Spark tự giữ offset trong checkpoint, không commit vào consumer group; xem offset từng lô ở tab Structured Streaming của Spark UI.)
14. Là thành viên nhóm, tôi muốn mở được Spark master, từng worker, Airflow, Grafana, Kafka UI, bộ giả lập và kết nối Postgres từ máy, không phải vào trong container.
15. Là người vận hành, tôi muốn dashboard cho biết tổng số thiết bị, bao nhiêu cần kiểm tra gấp, trung bình, bình thường.
16. Là người vận hành, tôi muốn chọn nhiều thiết bị và xem đường RUL dự đoán của chúng theo thời gian.
17. Là người vận hành, tôi muốn danh sách thiết bị cần kiểm tra, gấp nhất lên đầu, kèm Lệnh bảo trì của chúng.
18. Là người viết báo cáo, tôi muốn thấy Lệnh bảo trì có ra kịp trước khi hỏng không, sai số RUL theo phương pháp sinh dữ liệu, thông lượng và độ trễ, lịch sử Cổng kiểm định.
19. Là người viết báo cáo, tôi muốn RMSE trên tập test chính thức của C-MAPSS (gộp và từng tập con), precision và recall.
20. Là người viết báo cáo, tôi muốn chứng minh phần làm sạch đúng bằng cách đối chiếu kho với đáp án trong `truth`.

## Implementation Decisions

### Triển khai

Mọi dịch vụ nằm trong `v2/docker-compose.yml`. Mọi cổng chỉ mở trên `127.0.0.1` (Spark UI, Kafka, Kafka UI không có đăng nhập); muốn mở cho mạng LAN thì đổi thành `0.0.0.0`.

| Dịch vụ | Image | Cổng trên máy | Ghi chú |
|---|---|---|---|
| `kafka` | `apache/kafka:4.2.2` | 9094 | Thêm listener `EXTERNAL` quảng bá `localhost:9094` cho client ngoài Docker; trong mạng Docker vẫn `kafka:9092` |
| `sensor` | có sẵn | 8000 | Gắn thêm `models/norm_stats` chỉ đọc |
| `kafka-ui` | `kafbat/kafka-ui:v1.1.0` | 8085 | Topic, partition, xem Bản tin, lag của consumer |
| `postgres` | `postgres:17` | 5432 | Database `pdm` và `airflow`; mật khẩu từ biến môi trường có mặc định |
| `grafana` | `grafana/grafana:13.2.3` | 3000 | Nguồn dữ liệu và dashboard khai báo trong repo |
| `spark-master` | `pdm` | 8080 | |
| `spark-worker-1`, `spark-worker-2` | `pdm` | 8081, 8082 | Mỗi worker 2 core, 2 GB; cổng UI riêng |
| `init` | `pdm` | — | Chạy một lần rồi thoát |
| `spark` | `pdm` | 4040 | Driver của job luồng, Spark UI bật |
| `airflow` | `pdm` | 8088 | `standalone`, chạy thường trực |

- `SPARK_PUBLIC_DNS=localhost` ở master, worker và driver, để link giữa các trang Spark mở được từ trình duyệt trên máy. Driver của các job do Airflow gửi dùng cổng UI 4041 (mở ra máy).
- Image `pdm`, `v2/spark/docker/Dockerfile`: lấy từ `v1/docker/Dockerfile` (Python 3.12, Java 17, PySpark 4.1.3, jar Kafka và JDBC Postgres có sẵn), thêm `pandas==2.2.3`, `pyarrow==18.1.0`; lát 3 thêm Airflow 3.3.2. Một image cho mọi container Spark và Airflow, nên driver, executor, Airflow cùng bản Python và Spark.
- Thư mục gắn cùng đường dẫn vào mọi container Spark và Airflow: `v2/spark` → `/app` (mã), `v2/data` → `/data` (`raw/` dữ liệu gốc, `lake/` kho), `v2/models` → `/models`. Checkpoint trên volume `checkpoints` gắn vào driver và hai worker.
- Job luồng: `spark.cores.max=2`, mỗi executor 1 core; job của Airflow dùng 2 core còn lại.
- `spark` chờ `init` thoát thành công, Kafka và Postgres khỏe. `sensor` cũng chờ `init`: nếu không, ở lần dựng đầu bộ giả lập khởi động trước khi có `groups.json` và phát lại mọi máy.
- RAM dự kiến 8,5–9 GB trên 11,7 GB Docker được cấp (`.wslconfig` đặt `memory=12GB`); lát 1 đo thật bằng `docker stats`.

### Thay đổi ở bộ giả lập

- Đọc `/models/norm_stats/groups.json` lúc khởi động. Có file: chỉ dùng máy của nhóm `replay` làm nguồn cho mọi phương pháp, và mô hình tham số khớp trên nhóm đó. Không có file: dùng mọi máy như hiện nay. Đường dẫn qua biến `GROUPS_FILE`.
- Bảng thống kê chuẩn hóa của bộ giả lập (dùng cho nhiễu và cho biến đổi) vẫn tính từ các máy nguồn của nó; không phụ thuộc bảng của Spark.

### Khởi tạo

Giữ `prepare_data.py` và `train.py` của v1: chia 709 máy theo máy, hạt giống cố định, 70% huấn luyện gốc / 10% kiểm định / 20% phát lại; bảng thống kê chuẩn hóa từ nhóm gốc (6 điều kiện × 16 cảm biến); phiên bản đầu học 20% nhóm gốc. Ghi `models/norm_stats/{groups.json, stats.json}`, `models/<phiên bản>/`, `models/current.json`. `init` bỏ qua nếu `current.json` đã có. Miền hợp lệ (min, max từng cột của nhóm gốc) không lưu file: job luồng tính lại từ C-MAPSS lúc khởi động (vài giây), nên luôn khớp với `groups.json`.

### Đặc trưng cửa sổ theo Chu kỳ

- Chuẩn hóa bằng API DataFrame như v1: suy Điều kiện vận hành, ghép bảng thống kê kiểu broadcast, z = (x − μ)/σ cho 16 cảm biến.
- Hàm lõi pandas (một mô-đun, dùng chung) nhận các Bản tin đã chuẩn hóa của một máy và một danh sách Chu kỳ kết thúc e, trả về mỗi e một dòng: `machine_id`, `age` = e, `n_readings`, và trung bình, độ lệch chuẩn mẫu của 16 cảm biến trên các Chu kỳ e − 29 … e (giá trị rỗng bỏ qua). Cửa sổ có dưới 20 Bản tin bị bỏ (bên đo trên tập test bỏ ngưỡng này). 33 cột đặc trưng như v1: `age` cộng 32 thống kê.
- Huấn luyện: `groupBy("machine_id").applyInPandas` với mọi e chia hết cho 5.
- Luồng: `applyInPandasWithState` gom theo `machine_id`, chế độ append, `GroupStateTimeout.ProcessingTimeTimeout`.
  - Trạng thái mỗi máy: các Bản tin của 35 Chu kỳ gần nhất (khóa theo `cycle`), Chu kỳ lớn nhất đã thấy, cửa sổ cuối đã phát.
  - Bản tin trùng `cycle` giữ bản đến trước.
  - Phát cửa sổ e (e chia hết cho 5, lớn hơn cửa sổ cuối đã phát) khi Chu kỳ lớn nhất đã thấy ≥ e + 5. Bản tin trễ vẫn được nhận nếu còn cửa sổ tương lai cần nó (Chu kỳ > cửa sổ cuối đã phát − 25); trễ hơn thì bỏ. Cửa sổ đã phát không phát lại.
  - Máy không có Bản tin trong 120 giây thì xóa trạng thái.
  - Hệ quả: 5–9 Chu kỳ cuối đời của mỗi máy không được chấm.
  - Giới hạn: độ chờ tính bằng Chu kỳ còn độ trễ của Bản tin tính bằng giây. Ở k Chu kỳ/giây, Bản tin trễ d giây là trễ k·d Chu kỳ; quá 25 Chu kỳ thì không vào cửa sổ chấm điểm (kho vẫn nhận nếu trễ ≤ 10 giây). Ở k = 1 và độ trễ tối đa mặc định 5 giây thì không mất gì.
  - Bộ giả lập dừng quá 2 phút thì trạng thái mọi máy bị xóa; chạy lại thì mỗi máy cần 20 Bản tin mới trước dự đoán đầu tiên.
- Mỗi dòng đặc trưng mang thêm `window_end` = `event_time` lớn nhất trong cửa sổ. Bên trong hàm pandas, nó là số nguyên micro giây và đầu ra là một khối số thực (dựng DataFrame theo từng cột tốn gấp bốn); lớp bọc Spark đổi lại thành cột thời điểm. Trạng thái lưu thời điểm bằng số nguyên micro giây: số giây kiểu số thực làm lệch tới nano giây và Arrow từ chối ghi.

### Làm sạch

1. Phân tích JSON theo lược đồ cố định khai báo cạnh lược đồ đọc file C-MAPSS, không có `truth`. Bản tin thiếu `machine_id`, `cycle` hoặc `event_time` bị loại.
2. Miền hợp lệ: số đo ngoài [min − 10% khoảng, max + 10% khoảng] của nhóm gốc thành rỗng (bắt được −9999 và ×10).
3. Khử trùng: phía chấm điểm trong trạng thái; phía kho `withWatermark("event_time", "10 seconds").dropDuplicatesWithinWatermark(["machine_id", "cycle"])`.

Bước 2 và khử trùng phía kho thuộc lát 4; lát 1–3 chạy với Lỗi chèn tắt.

### Job luồng (`spark`)

Một ứng dụng, ba truy vấn, mỗi truy vấn một thư mục checkpoint; `spark.sql.shuffle.partitions=2` (khóa số kho trạng thái; đổi phải xóa checkpoint).

| Truy vấn | Đọc | Ghi | Nhịp | Lát |
|---|---|---|---|---|
| Chấm điểm | `sensor.raw` | `predictions`, `maintenance_orders` | 5 giây | 1 (lệnh: 2) |
| Lưu kho | `sensor.raw` | `/data/lake/readings/`, Parquet phân vùng theo ngày | 60 giây | 2 |
| Sự kiện hỏng | `machine.failed` | bảng `failures` | 2 giây (dashboard loại máy đã hỏng; nhịp dài làm các ô số lệch nhau) | 1 |
| Kho sự kiện hỏng | `machine.failed` | `/data/lake/failures/`, file sink | 60 giây | 1 |

`foreachBatch` của truy vấn chấm điểm:

1. Đọc `current.json`; phiên bản khác lần trước thì nạp hai mô hình, giữ trong driver.
2. Áp mô hình hồi quy rồi phân lớp; xác suất Sắp hỏng là phần tử 1 của vectơ xác suất. Ghi `predictions` (JDBC, ít nhất một lần).
3. Máy có xác suất ≥ 0,5: ghi `maintenance_orders` với `ON CONFLICT (machine_id) DO NOTHING`. Chu kỳ đề xuất = `age` + RUL dự đoán − 20, làm tròn, không nhỏ hơn `age`.

Không có UDF Python ngoài hàm cửa sổ; Spark UI bật trên cổng 4040.

### Postgres (`pdm`)

| Bảng | Cột | Khóa, chỉ mục |
|---|---|---|
| `predictions` | `machine_id`, `window_end` (timestamptz), `window_end_cycle`, `rul_pred`, `failing_prob`, `model_version`, `written_at` (mặc định now()) | chỉ mục (`window_end`), (`machine_id`, `window_end`) |
| `maintenance_orders` | `machine_id`, `created_at`, `cycle`, `rul_pred`, `failing_prob`, `suggested_cycle`, `model_version` | khóa chính `machine_id` |
| `failures` | `machine_id`, `failed_at`, `last_cycle`, `method`, `base` (text[]) | khóa chính `machine_id`, ghi `ON CONFLICT DO NOTHING` |
| `model_validation` | `validated_at`, `candidate`, `candidate_rmse`, `candidate_f1`, `current`, `current_rmse`, `current_f1`, `accepted` | |

Bảng tạo bằng script khởi tạo của Postgres. Database `airflow` cũng tạo ở đó.

### Mô hình, huấn luyện lại, Airflow

- Hai `PipelineModel` GBT như v1 (RUL chặn 125; Sắp hỏng = RUL ≤ 60; tham số như v1); thư mục phiên bản và `current.json` ghi bằng file tạm rồi đổi tên.
- `train.py` thêm `--training-set <thư mục Parquet>`; không truyền thì học trên nhóm gốc với tham số tỷ lệ (mặc định 1,0).
- `build_training_set.py`: ghép `lake/readings` với `lake/failures`; RUL thật = `last_cycle` − `cycle`; lấy tối đa `--max-failed` (mặc định 500) máy hỏng gần nhất; tính đặc trưng bằng hàm chung; gộp với đặc trưng của cả nhóm gốc; ghi `/data/lake/training_sets/<run_id>/`. Không có máy nào của nhóm kiểm định.
- `validate.py`: đo ứng viên và Mô hình đang dùng trên nhóm kiểm định; ghi một dòng `model_validation`. Quy tắc Cổng kiểm định như v1 (RMSE thấp hơn hẳn và F1 không thấp hơn).
- DAG `retrain_rul`: `build_training_set >> train >> validate >> branch >> [promote, keep_current]`; mỗi bước `spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2`; lịch hằng ngày, chạy tay được; tham số khi chạy tay: `max_failed`, `fraction` (để tạo một lần bị từ chối).
- `evaluate_test.py`: học một phiên bản trên cả nhóm gốc; mỗi Máy của tập test chính thức lấy cửa sổ cuối (bỏ ngưỡng 20 Bản tin); RMSE gộp và từng tập con so với `RUL_FD00x.txt`; precision, recall của mô hình phân lớp. Ghi kết quả ra `models/<phiên bản>/test_metrics.json`.
- Airflow 3.3.2 cài trong venv riêng `/opt/airflow` của image `pdm` (bộ ràng buộc phiên bản của Airflow ghim `pandas`, `pyarrow` khác bản job Spark dùng); venv nằm cuối `PATH` để `spark-submit` vẫn dùng Python hệ thống. Không đăng nhập (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`), vì cổng chỉ mở trên localhost.
- DAG tự đặt tên phiên bản theo `run_after` của lần chạy và truyền vào `train.py --version`, `validate.py`; bước rẽ nhánh đọc `models/<phiên bản>/validation.json`. `max_active_runs=1` để hai lần huấn luyện không tranh 2 core.

### Grafana

Một dashboard "Bảo trì dự đoán", làm mới mỗi 5 giây, mặc định 15 phút gần nhất. Mỗi máy được xếp mức theo dự đoán mới nhất của nó: Gấp khi RUL dự đoán ≤ `$urgent` (mặc định 30), Trung bình khi ≤ `$medium` (mặc định 60), Bình thường còn lại.

Biến: `machine` (chọn nhiều; danh sách các máy có dự đoán trong 15 phút, máy RUL thấp lên đầu; mục đầu "Tự động" là 5 máy RUL thấp nhất và là mặc định), `urgent`, `medium` (ô nhập số). Không có biến `method`: ô sai số đặt cả bốn phương pháp cạnh nhau.

| Ô | Loại | Nguồn | Lát |
|---|---|---|---|
| Tổng thiết bị (có dự đoán trong 2 phút và chưa hỏng; 2 phút để ở k thấp, khi mỗi máy chỉ có dự đoán vài chục giây một lần, vẫn đếm đủ) | stat | `predictions` | 1 |
| Kiểm tra gấp / Trung bình / Bình thường | 3 stat, đỏ / cam / xanh | dự đoán mới nhất mỗi máy | 1 |
| RUL dự đoán theo thời gian của các máy đã chọn | time series, dải nền đỏ < 30, cam < 60 (cố định: ngưỡng tô nền của Grafana không nhận biến) | `predictions` | 1 |
| Số thiết bị theo mức theo thời gian | time series xếp chồng | `predictions` | 1 |
| Lệnh bảo trì đang mở | stat | `maintenance_orders` trừ máy đã hỏng | 2 |
| Đã hỏng trong khoảng đang xem | stat | `failures` | 2 |
| Thiết bị cần kiểm tra: mã, mức, RUL, xác suất, Chu kỳ, Chu kỳ đề xuất và còn bao nhiêu Chu kỳ, phiên bản; gấp trước; link thêm máy vào biến `machine` | bảng | `predictions` ghép `maintenance_orders` | 2 |
| Lệnh bảo trì có kịp không: tỷ lệ máy hỏng đã có lệnh; phân bố số Chu kỳ từ lúc ra lệnh tới hỏng; số lệnh quá sớm (ra hơn 120 Chu kỳ, tức gấp đôi ngưỡng Sắp hỏng, trước khi hỏng) | stat + histogram | `failures` ghép `maintenance_orders` | 2 |
| Sai số RUL theo phương pháp sinh: RMSE trên máy đã hỏng, RUL thật = `last_cycle` − `window_end_cycle`, chặn ở 125 như nhãn huấn luyện (mô hình cố ý không phân biệt RUL trên 125) | bar | `predictions` ghép `failures` | 2 |
| Dự đoán mỗi giây và độ trễ p50, p95 (`written_at` − `window_end`) | time series | `predictions` | 2 |
| Lịch sử Cổng kiểm định | bảng | `model_validation` | 3 |

### Cấu trúc mã

```
v2/
  docker-compose.yml          thêm các dịch vụ trên
  models/                     Thư mục mô hình (bỏ khỏi git)
  data/lake/                  kho Parquet (bỏ khỏi git)
  spark/
    docker/Dockerfile
    docker/postgres/          script tạo bảng và database airflow
    docker/grafana/           nguồn dữ liệu, dashboard
    src/pdm/                  schema, features (chuẩn hóa + hàm cửa sổ pandas), cleaning, labels, gate, orders
    src/jobs/                 prepare_data, train, validate, stream, build_training_set, evaluate_test, evaluate_cleaning
    dags/retrain_rul.py
    tests/
```

## Testing Decisions

Kiểm hành vi thấy được từ bên ngoài. Kiểm thử tự động chạy bằng pytest trong image `pdm` (Spark một tiến trình).

1. **Hàm cửa sổ** (lát 1):
   - Một máy dựng tay: trung bình, độ lệch chuẩn khớp số tính tay; cửa sổ dưới 20 Bản tin bị bỏ; giá trị rỗng bỏ qua.
   - Logic trạng thái tách thành hàm thuần (trạng thái cũ + Bản tin mới → trạng thái mới + các dòng phát ra) và kiểm trực tiếp: đưa cùng một vòng đời theo thứ tự, đảo thứ tự trong 5 Chu kỳ, có Bản tin trùng, chia thành nhiều lô bất kỳ → các dòng phát ra giống hệt kết quả của chế độ tĩnh trên các cửa sổ có e + 5 ≤ Chu kỳ cuối.
   - Một kiểm thử chạy thật `applyInPandasWithState` với nguồn file và bảng trong bộ nhớ cho cùng kết quả với chế độ tĩnh.
2. **Bỏ `truth`** (lát 1): Bản tin có `truth` qua bước phân tích không còn cột nào chứa đáp án.
3. **Lệnh bảo trì** (lát 2): Chu kỳ đề xuất đúng công thức và không nhỏ hơn `age`; máy dưới ngưỡng không có lệnh.
4. **Tập huấn luyện lại** (lát 3): RUL thật đúng; tối đa `max_failed` máy hỏng gần nhất; không có máy nhóm kiểm định; DAG nạp được và có đúng thứ tự bước.
5. **Làm sạch** (lát 4): giá trị ngoài miền thành rỗng, trong miền giữ nguyên; −9999 và ×10 của mọi cột bị bắt.
6. **Cổng kiểm định**: giữ kiểm thử của v1.
7. **Bộ giả lập**: có `groups.json` thì mọi máy sinh ra có `base` thuộc nhóm phát lại; không có thì như cũ.
8. **Nghiệm thu bằng tay** mỗi lát, theo cột "Xong khi" dưới đây.

## Các lát

| Lát | Nội dung | Xong khi |
|---|---|---|
| 1 Đường chấm điểm | Image `pdm`; cụm; `init`; bộ giả lập đọc `groups.json`; truy vấn chấm điểm với cửa sổ theo trạng thái; `predictions`; Spark UI 4040, Kafka UI, mọi cổng; Grafana: bốn ô số và hai biểu đồ của lát 1 | Một lệnh dựng từ đầu (không `models/`, không volume); ở N = 500 với k = 1 rồi k = 5, mỗi máy có dự đoán mỗi 5 Chu kỳ và `window_end_cycle` chia hết cho 5; đổi k khi đang chạy không làm sai; mọi giao diện mở được từ trình duyệt; tab Structured Streaming hiện tốc độ nhận và xử lý; đo RAM bằng `docker stats` |
| 2 Lệnh bảo trì và kho | Truy vấn lưu kho và sự kiện hỏng; `failures`; Lệnh bảo trì; các ô Grafana của lát 2 | Máy gần hỏng có Lệnh bảo trì trước sự kiện hỏng; mỗi máy tối đa một lệnh; số Bản tin trong kho khớp số đã phát khi Lỗi chèn tắt; chạy 30 phút ở 5.000 events/giây không lỗi, độ trễ không tăng dần |
| 3 Huấn luyện lại bằng Airflow | Airflow; `build_training_set`; `train --training-set`; `validate` ghi Postgres; DAG; ô Cổng kiểm định | Một lần chạy DAG được nhận, `current.json` đổi và cột phiên bản trong `predictions` đổi theo không khởi động lại job; một lần (tham số học rất ít) bị từ chối, `current.json` không đổi |
| 4 Làm sạch | Miền hợp lệ; khử trùng phía kho; `evaluate_cleaning.py` đọc lại topic gốc (có `truth`) và đối chiếu với kho | Bật cả bốn Lỗi chèn 5%, chạy 30 phút: dashboard vẫn đúng, độ trễ không tăng dần; số Bản tin trong kho = số đã phát − số bị loại, khớp đáp án |
| 5 Đo | `evaluate_test.py`; đo thông lượng, độ trễ, RAM, thời gian huấn luyện 1 và 2 worker; cặp `shuffle.partitions` 200 và 2; tắt rồi bật container `spark` | Bảng số liệu cho báo cáo, mỗi phép đo 3 lần lấy trung vị (như Mục 1 điều 7 của `v1/docs/plan.md`) |

## Out of Scope

- Đăng nhập cho Spark UI, Kafka UI; mở cổng ra mạng LAN.
- Cụm nhiều máy vật lý, HDFS, Delta Lake.
- Random Forest, so sánh nhiều mô hình.
- Báo cáo, slide, video (lát 6 của kế hoạch v1, làm riêng).

## Further Notes

- Bài học v1 áp dụng: checkpoint trên volume; jar trong image; job luồng giới hạn 2 core; không ghi file mỗi giây lên thư mục gắn từ Windows.
- Rủi ro chính mới so với v1: thông lượng của `applyInPandasWithState` ở 5.000 events/giây trên 2 core. Nếu không theo kịp: tăng nhịp chấm điểm lên 10 giây và đo lại; nếu vẫn không, dừng để quyết định (gợi ý: tăng số core của job luồng, hoặc chuyển sang `transformWithStateInPandas` của Spark 4).
- Chưa thử: kéo image `kafbat/kafka-ui:v1.1.0` (lát 1) và cài Airflow 3.3.2 bằng pip chung với PySpark 4.1.3 trong image `pdm` (lát 3).
