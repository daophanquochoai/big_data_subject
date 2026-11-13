# Chuyển chức năng huấn luyện về đúng dự án PDM

## Mục tiêu

Theo dõi và điều khiển huấn luyện ngay trong giao diện `sensor`, dùng Airflow hiện có của dự án. Gỡ các thay đổi đã gắn nhầm vào Golden Star sau khi sao lưu đầy đủ.

## Quyết định

- Giữ pipeline Spark GBT `retrain_rul`, dữ liệu C-MAPSS, `models/current.json` và luồng inference hiện tại.
- Hiển thị DAG, các bước, Spark worker và chất lượng model đang dùng trên tab Huấn luyện.
- Chuyển đường train một máy nhiều GPU sang cùng tab, sử dụng dữ liệu/33 đặc trưng C-MAPSS thực; checkpoint PyTorch là ứng viên riêng, không tự thay Spark PipelineModel.
- Tái sử dụng runner Docker đã kiểm chứng, chuyển state sang SQLite trong volume `/state` của sensor; không mang tài khoản, phân quyền hay database Golden Star sang đây.
- Dùng Airflow `:8088`, sensor `:8000`, Spark master `:8080` của PDM. Không tạo thêm web Golden Star hay Airflow độc lập.
- Máy hiện có một GPU: kiểm chứng CUDA một rank và CPU/gloo hai rank; không giả lập nhiều GPU.

## Các bước

1. Sao lưu code/patch và baseline hai repo; bảo toàn mọi thay đổi có sẵn của người dùng.
2. Tích hợp API theo dõi pipeline Spark và state/điều khiển training GPU vào sensor.
3. Thêm exporter C-MAPSS, workload PyTorch RUL, DAG cùng Airflow và launcher đúng thư mục.
4. Thêm tab Huấn luyện theo giao diện PDM, với topology, metric, log và lịch chạy thật.
5. Gỡ phần gắn nhầm tại Golden Star; giữ backup và volume, không xóa dữ liệu người dùng.
6. Chạy kiểm thử, build và kiểm chứng trực tiếp tại `http://localhost:8000/?tab=training`.

## Tiến độ

- Đã đọc cấu trúc v2, model GBT, DAG và giao diện hiện hữu.
- Đã lưu code/patch Golden Star và baseline đích vào `.local/migration/`.
- Đã tích hợp API, exporter C-MAPSS, workload DDP, DAG và tab Huấn luyện vào PDM; launcher chạy trọn vẹn thành công.
- Đã khôi phục các file Golden Star bị sửa bởi chức năng training, chuyển file mới vào backup, hạ riêng migration training sau khi dump bảng và dừng các tiến trình/container của bản triển khai nhầm. Không xóa volume. Nội dung `backend/app/models.py` khớp hash HEAD dù Git trên máy còn báo thay đổi metadata.
- Raw FD003 có một token số chứa backtick từ trước. Giữ nguyên raw; tạo bản làm việc có manifest, được mọi Spark node và sensor dùng chung.
- 86 test sensor đạt trên bản dữ liệu làm việc, gồm 10 test điều khiển/API training; 18 test runner đạt. 23 test Spark hiện hữu (DAG, features/gate/labels và windows) đạt trong image mới.
- UI build đạt; kiểm tra Edge tại 1440px và 375px không có lỗi JavaScript, không tràn ngang. Tab Spark hiển thị đủ sáu bước DAG.

## Kiểm chứng chạy thật ngày 13/11/2026

| Run | Nguồn / thiết bị | Kết quả |
|---|---|---|
| 1 | Chạy tay qua Airflow, CPU/Gloo hai rank | Thành công 4 epoch; 4.407 train / 2.817 validation / 33 đặc trưng |
| 2 | Bấm chạy trên UI, CUDA một rank | Thành công 6 epoch; checkpoint và metric đầy đủ |
| 3 | CPU hai rank, yêu cầu 100 epoch | Dừng từ UI khi container đang chạy; kết thúc cancelled sau 19 epoch, Docker xác nhận Running=false |
| 4 | Schedule Airflow 09:45 giờ Việt Nam, CPU hai rank | Tự tạo và hoàn tất 4 epoch; `scheduled__2026-11-13T02:45:00+00:00` |

Cả ba checkpoint hoàn tất đã nạp lại bằng PyTorch, kiểm tra chiều đầu vào 33, epoch, thống kê chuẩn hóa và fingerprint khớp kết quả. Đây là kiểm chứng cơ chế thực thi, chưa phải kết luận ứng viên MLP tốt hơn GBT.

Lịch hiện lưu **02:00 hằng ngày, Asia/Ho_Chi_Minh**, CPU hai rank, 12 epoch; lượt tiếp theo `2026-11-14T02:00:00+07:00`. Model phục vụ vẫn `v20261015T000000`. Máy chỉ có một GPU, chưa kiểm chứng nhiều GPU vật lý; Kubernetes và Slurm mới dừng ở tài liệu thiết kế.

Giao diện: <http://localhost:8000/?tab=training>. Cách chạy lại: [training/README](../training/README.md). Evidence cục bộ: `.local/training-verification.json`, ảnh `training-desktop.png`, `training-mobile.png`, `training-spark.png` và backup `.local/migration/`.
