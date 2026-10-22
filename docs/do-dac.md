# Số đo của v2 (lát 5)

Đo ngày 15–16/10/2026. Mỗi số trong bảng là **trung vị của 3 lần** trừ khi ghi khác.

**Cấu hình chung.** Laptop Windows 11, 16 GB RAM, 12 CPU logic. Docker Desktop trên WSL2 được cấp 11,7 GB (`.wslconfig` đặt `memory=12GB`). Cụm Spark 4.1.3 standalone: 1 master và 2 worker, mỗi worker 2 core và 2 GB. Job luồng dùng tối đa 2 core (mỗi executor 1 core) và `spark.sql.shuffle.partitions = 2`. Kafka 4.2.2 một broker, topic `sensor.raw` 3 partition. Bộ giả lập phát N = 500 Máy ảo; tải = N × k Chu kỳ mỗi giây. Lỗi chèn tắt, trừ khi ghi khác.

Script đo: `spark/measure/throughput.sh` (thông lượng, độ trễ, RAM), `spark/measure/train_time.sh` (thời gian huấn luyện), `spark/src/jobs/evaluate_test.py`, `spark/src/jobs/evaluate_cleaning.py`.

## 1. Mô hình trên tập test chính thức C-MAPSS

Phiên bản `vfull-base` học cả nhóm huấn luyện gốc (496 Máy). Mỗi Máy test: cửa sổ 30 Chu kỳ kết thúc ở Chu kỳ cuối, bỏ ngưỡng 20 Bản tin (Máy ngắn nhất có 19 Chu kỳ), so với `RUL_FD00x.txt`. Một lần đo (mô hình và dữ liệu cố định, kết quả không đổi giữa các lần).

| Tập | Số Máy | RMSE (RUL thật) | RMSE (RUL thật chặn 125) | Precision Sắp hỏng | Recall Sắp hỏng |
|---|---|---|---|---|---|
| Gộp | 707 | 26,6 | 17,8 | 0,88 | 0,86 |
| FD001 | 100 | 19,8 | 18,6 | 0,89 | 0,87 |
| FD002 | 259 | 27,7 | 16,2 | 0,85 | 0,87 |
| FD003 | 100 | 19,0 | 17,7 | 0,94 | 0,71 |
| FD004 | 248 | 30,1 | 19,1 | 0,89 | 0,90 |

Sắp hỏng = RUL thật ≤ 60. Mô hình học nhãn RUL chặn ở 125, nên cột "chặn 125" là cột so được với cách huấn luyện; cột còn lại phạt cả những Máy test còn rất khỏe (RUL thật tới hơn 300) mà mô hình cố ý không phân biệt.

## 2. Thông lượng và độ trễ đầu-cuối

Độ trễ = lúc ghi Postgres − `event_time` lớn nhất trong cửa sổ, tính trên mọi dự đoán trong khoảng đo. Mỗi lần: khởi động 90–120 giây, đo 300–600 giây.

| Tải đặt | Kafka nhận (Bản tin/giây) | Dự đoán/giây | Độ trễ p50 | Độ trễ p95 | RAM các container | Ghi chú |
|---|---|---|---|---|---|---|
| 500/giây (N = 500, k = 1), yêu cầu của đề | 494 | 88,1 | 12,2 s | 23,3 s | 9,5 GB | 3 lần × 5 phút |
| 5.000/giây (N = 500, k = 10) | 4.588 | 843 | 15,4 s | 30,7 s | 9,9 GB | 3 lần × 10 phút |

- Dự đoán/giây thấp hơn tải/5 vì Máy ảo mới sinh chưa đủ 20 Bản tin và 5–9 Chu kỳ cuối đời không được chấm.
- Ở 5.000/giây Kafka chỉ nhận khoảng 4.600/giây: cả hệ thống chạy sát trần máy (Kafka 217% CPU, hai worker 130–150%, RAM trống còn ~140 MB, swap 1,5 GB), bộ giả lập trễ nhịp và không phát bù. Riêng bộ giả lập với Kafka (chưa có Spark) đạt 5.018/giây trong 10 phút.
- Chạy liên tục 30 phút ở 5.000/giây: độ trễ p50 từng 5 phút 9,7–12,3 s, không tăng dần, không ngoại lệ. Với cả bốn Lỗi chèn 5%: p50 12,9–16,8 s, có hai lần vọt lên 31 và 41 s rồi tự về.

## 3. `spark.sql.shuffle.partitions`: 2 so với 200 (mặc định của Spark)

Cùng tải 500/giây, cùng cụm. Bản 200 chạy trên checkpoint riêng.

| `shuffle.partitions` | Dự đoán/giây | Độ trễ p50 | Độ trễ p95 |
|---|---|---|---|
| 2 (= số core của job luồng) | 88,1 | 12,2 s | 23,3 s |
| 200 | 66,5 | 69,4 s | 112,6 s |

Toán tử có trạng thái tạo một kho trạng thái cho mỗi partition, và hàm cửa sổ pandas chạy một lần cho mỗi partition có dữ liệu. Với 200 partition trên 2 core, mỗi lô 5 giây phải lập lịch hàng trăm tác vụ nhỏ, nên job không theo kịp: độ trễ tăng gần 6 lần và một phần cửa sổ ra trễ khỏi khoảng đo. Ở 5.000/giây, bản 200 partition làm máy hết RAM và không đo được.

## 4. Thời gian huấn luyện trên cụm

`train.py 1.0` (cả nhóm gốc, khoảng 21.000 cửa sổ, hai mô hình GBT 50 cây), gửi lên cụm, job luồng tắt. Thời gian gồm cả khởi động container driver (~2 s).

| Cụm | Lần 1 | Lần 2 | Lần 3 | Trung vị |
|---|---|---|---|---|
| 2 worker, 4 core | 193 s | 202 s | 221 s | 202 s |
| 1 worker, 2 core | 185 s | 167 s | 182 s | 182 s |

Thêm worker không làm huấn luyện nhanh hơn: dữ liệu nhỏ, GBT xây cây nối tiếp, mỗi vòng có chi phí điều phối và shuffle qua mạng giữa các container lớn hơn phần tính toán được chia ra. Cụm có lợi cho đường luồng (tách tài nguyên job luồng và job huấn luyện) hơn là cho tốc độ huấn luyện ở cỡ dữ liệu này.

## 5. Dung lượng

Đo trên dữ liệu đã tích lũy: Kafka 25,0 GB cho 38,98 triệu Bản tin; kho 2,88 GB cho 21,49 triệu dòng.

| | Byte mỗi Bản tin | Mỗi ngày ở 500/giây | Mỗi ngày ở 5.000/giây |
|---|---|---|---|
| Kafka (JSON, có `truth`) | 642 | 27,7 GB | 277 GB |
| Kho (Parquet, đã làm sạch) | 134 | 5,8 GB | 58 GB |

Kafka đang giữ dữ liệu 7 ngày (mặc định): ở 5.000/giây ổ đĩa đầy trong vài ngày. Cần đặt `KAFKA_LOG_RETENTION_HOURS` hoặc `KAFKA_LOG_RETENTION_BYTES` cho demo dài.

## 6. Làm sạch, đối chiếu với đáp án

Cả bốn Lỗi chèn 5%, 5.000/giây, cửa sổ `event_time` 14:05–14:15 (10 phút).

| | Số |
|---|---|
| Bản tin trong Kafka | 2.927.868 (giá trị sai 147.239, trùng 146.251, trễ 146.524) |
| Dòng kho mong đợi (trừ bản trùng) | 2.781.617 |
| Dòng kho thực tế | 2.781.617, không trùng, không dòng nào ngoài Kafka |
| Giá trị sai bị bỏ sót | 0 |
| Số đo thật bị bắt nhầm là sai | 5 (0,0002%) |

Không có Lỗi chèn: kho khớp Kafka tuyệt đối (2.980.000 Bản tin trong 10 phút).

## 7. Huấn luyện lại và Cổng kiểm định

| Lần chạy DAG | Ứng viên | RMSE ứng viên / đang dùng | F1 ứng viên / đang dùng | Kết quả |
|---|---|---|---|---|
| Theo lịch: nhóm gốc + 500 Máy ảo đã hỏng | `v20261015T000000` | 18,69 / 21,33 | 0,842 / 0,822 | Được nhận; job luồng đổi mô hình không khởi động lại |
| Chạy tay, `fraction = 0.02` | `v20261015T134027` | 31,94 / 18,69 | 0,718 / 0,842 | Không nhận; con trỏ giữ nguyên |

Đo trên nhóm kiểm định (71 Máy). Phiên bản đầu học 20% nhóm gốc.

## 8. Lệnh bảo trì

Trong 15 phút ở 5.000/giây: 95,9% Máy hỏng (có ít nhất một dự đoán) đã có Lệnh bảo trì trước khi hỏng; trung vị báo trước 52 Chu kỳ. RMSE trên Máy ảo đã hỏng theo phương pháp sinh, so với RUL thật chặn ở 125: `gauss` 20,3, `blend` 21,3, `parametric` 25,0 (`parametric` khó dự đoán nhất).

## 9. Phục hồi sau khi tắt job luồng

500/giây. `docker compose stop spark` lúc 02:49:37, `start` lúc 02:50:53 (tắt 76 giây). Một lần đo.

| | |
|---|---|
| Dự đoán đầu tiên sau khi bật | 02:51:57 (64 giây: khởi động driver, nạp mô hình, đọc tiếp từ checkpoint) |
| Phút đuổi phần dồn (02:52) | độ trễ p50 58 s, lớn nhất 154 s |
| Về bình thường | từ 02:53, p50 10,8 s |
| Cửa sổ bị mất | 0: 29.161/29.161 cửa sổ liên tiếp cách nhau đúng 5 Chu kỳ (trạng thái từng Máy khôi phục từ checkpoint) |
| Lệnh bảo trì trùng | 0 (153.509 lệnh cho 153.509 Máy, khóa chính) |
| Dự đoán ghi hai lần | 214 (lô đang dở lúc tắt; mức bảo đảm ít nhất một lần, như thiết kế) |
