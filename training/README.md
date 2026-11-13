# PDM Training Control — DDP trên một máy

Tài liệu giải thích cơ chế, so sánh train tập trung, batch size và quy trình benchmark: [Hướng dẫn train DDP](../docs/huong-dan-train-ddp.md).

Ứng dụng training riêng: <http://localhost:8001/>. Sensor ở cổng 8000 chỉ liên kết sang training. Airflow: <http://localhost:8088>; Spark master: <http://localhost:8080>.

Đã bỏ tab Máy train/SSH, API quản lý máy, thông tin đăng nhập và runner SSH theo yêu cầu. Hiện mọi lượt mới chạy bằng Docker trên máy hiện tại. Lịch sử và checkpoint cũ vẫn được giữ để xem lại.

## Khởi chạy

Từ thư mục gốc repository trên Windows có Docker Desktop:

```powershell
powershell -ExecutionPolicy Bypass -File .\training\Start-Training.ps1
```

Launcher build Spark/Airflow, workload PyTorch 2.6/CUDA 12.4, Training Control và sensor; tạo token nội bộ trong `.env.training` (gitignore), chuẩn bị dữ liệu làm việc và bật DAG `single_node_rul_training`. Không xóa volume hoặc model đang phục vụ. Service `training-state-init` giữ lịch sử từ sensor khi volume training chưa có; không ghi đè state đã tồn tại.

Sau khi build:

```powershell
docker compose --env-file .env.training up -d
docker compose --env-file .env.training logs --tail 60 airflow training-control
```

## Sử dụng

1. Mở Training Control, kiểm tra Docker, image và DAG.
2. Chọn CPU/Gloo hoặc CUDA/NCCL, số rank, epoch, batch size, learning rate và tỷ lệ máy trong nhóm train. Lưu cấu hình rồi bấm **Chạy ngay qua Airflow**.
3. Xem tiến độ, từng rank, loss, RUL RMSE, F1, log, container và checkpoint. Có nút dừng và lịch sử.
4. Bật lịch mỗi 5 phút, mỗi giờ hoặc 02:00 giờ Việt Nam. Lịch hiện lưu là 02:00 hằng ngày, CPU hai rank. Mỗi run giữ snapshot riêng.
5. Tab **Pipeline Spark đang dùng** hiển thị DAG GBT hiện hữu, worker và cho phép chạy pipeline qua cổng kiểm định.

## Cơ chế thực thi

Airflow kiểm tra lịch mỗi phút, Spark chuẩn bị dataset C-MAPSS rồi runner tạo container local. `torchrun` khởi chạy các process DDP trong container. Mỗi rank học một phần dữ liệu và đồng bộ gradient với các rank còn lại. Hiện đường local dùng **một container cho một run**, với nhiều process/rank trong container.

Tên container và hash cấu hình ổn định giúp retry tìm lại workload. UUID request chống gửi lặp. Toàn ứng dụng chỉ cho một run queued/starting/running cùng lúc. Khi hủy, chỉ giải phóng lượt sau khi xác nhận container đã dừng. Chỉ báo thành công khi đủ telemetry cuối của mọi rank và checkpoint hợp lệ. Chưa có resume sang container mới sau lỗi.

Máy hiện tại có 12 CPU logic và **một GTX 1650 Max-Q 4 GB**. Đã kiểm chứng CPU/Gloo hai rank và CUDA một rank; không coi hai rank CPU là hai GPU. Container local giới hạn 2 CPU/3 GB RAM. Muốn chạy nhiều GPU hoặc workload lớn cần điều chỉnh giới hạn và kiểm chứng trên phần cứng tương ứng.

## Dữ liệu và model

- C-MAPSS thực, tái sử dụng nhóm train/validation và logic cửa sổ Spark; loại nhóm replay khỏi train/validation.
- 33 đặc trưng: tuổi máy, mean/std của 16 sensor. MLP dự đoán RUL cap 125 và hỏng trong 60 chu kỳ. Chuẩn hóa đầu vào bằng thống kê train, lưu cùng checkpoint.
- Xuất tối đa 50.000 cửa sổ mỗi split theo thứ tự hash ổn định. Dataset cache dựa trên hash dữ liệu, thống kê, code tạo đặc trưng và tỷ lệ; mỗi run ghim checksum.
- File raw gốc không bị sửa. `.local/cmapss` là bản làm việc được kiểm tra đủ 26 cột số; chỉ bỏ backtick định dạng khi token sau đó là số hợp lệ. Mọi sửa định dạng ghi trong manifest. FD003 gốc hiện có một token như vậy.
- Checkpoint PyTorch là ứng viên riêng. Model Spark GBT đang phục vụ và `models/current.json` chỉ đổi theo pipeline kiểm định GBT hiện hữu.

## State và vận hành

State: volume `v2_training-state`, file `training.sqlite3`. Artifact local: volume `pdm_training_artifacts`, thư mục `runs/<hash>/checkpoint.pt` và `result.json`. Dataset/cache/log chuẩn bị: `data/training/`. Container kết thúc được giữ để kiểm tra/reconcile; không xóa volume khi khởi chạy lại.

Các run cũ vẫn đọc được; metadata topology cũ chỉ hiển thị như lịch sử. Cấu hình cũ được bỏ trường quản lý máy; lịch dùng chế độ đã gỡ được tắt để không vô tình chuyển thành lượt local. API không còn endpoint cấp credential hay nhận cấu hình SSH.

Ứng dụng phục vụ cục bộ qua các cổng bind `127.0.0.1`, kế thừa cách truy cập không đăng nhập. Docker socket có quyền điều khiển Docker host; không mở cấu hình này ra mạng dùng chung. Token nội bộ không nằm trong frontend.

## Kiểm chứng

32 test training đạt, gồm runner, API, giữ lịch sử, chuyển cấu hình cũ và từ chối các route/chế độ đã gỡ. Bộ sensor trước đó đạt 76 test. Từ thư mục gốc repository, với môi trường dev có requirements, pytest và httpx:

Sau khi gỡ SSH, run 9 khởi chạy từ giao diện qua Airflow và hoàn tất 12 epoch CPU/Gloo hai rank. Lịch sử tăng từ 8 lên 9 lượt, không mất run cũ. Kiểm tra Edge tại 1440px và 375px không lỗi JavaScript, không tràn ngang; API quản lý máy trả 404.

```powershell
.\sensor\backend\.venv\Scripts\python.exe -m pip install -r training/requirements.txt pytest httpx
.\sensor\backend\.venv\Scripts\python.exe -m pytest training/tests -q
```

Thiết kế tham khảo ba hướng: [một máy, Kubernetes, Slurm](../docs/airflow-distributed-training-deployment.md). Các lượt thử nghiệm SSH trước đây được lưu trong [tài liệu lịch sử](../docs/ssh-training-plan.md); tính năng đã gỡ khỏi ứng dụng.
