# Kế hoạch triển khai

Kế hoạch xây hệ thống mô tả trong [`architecture.md`](architecture.md). Kế hoạch xếp theo thứ tự việc, không theo ngày: một lát bắt đầu khi lát trước đã đạt tiêu chí "xong khi".

Trạng thái ngày 14/10/2026: **lát 1 đã xong: đường chấm điểm tối thiểu chạy thông từ Bộ phát lại tới Grafana trên cụm Spark.** Chi tiết ở Mục 3.

## 1. Mục tiêu và điều kiện hoàn thành

Sản phẩm nộp: demo chạy trực tiếp, báo cáo viết, slide, mã nguồn.

Tiêu chí chấm lấy từ `15. Capstone-Project.pdf`: báo cáo 30%, demo live 30%, thuyết trình 20%, hỏi đáp 20%.

Dự án coi là xong khi cả tám điều sau đúng:

1. Từ một máy sạch, một lệnh dựng lên toàn bộ hệ thống.
2. Bộ phát lại giữ được ≥ 500 events/giây trong 10 phút, có số đo ghi lại.
3. Một Máy ảo chạy tới gần hỏng thì Lệnh bảo trì tự xuất hiện trên Grafana trước khi Máy hỏng.
4. DAG huấn luyện lại chạy được hai lần: một lần mô hình mới được nhận, một lần bị từ chối, và job luồng tự dùng mô hình mới mà không khởi động lại.
5. Báo cáo có RMSE trên tập test chính thức của C-MAPSS (riêng từng tập con), precision/recall của mô hình phân lớp, thông lượng và độ trễ đầu-cuối.
6. Demo chạy trọn hai lần liên tiếp không sửa tay, gồm cả bước tắt container `spark` rồi bật lại mà job luồng tự chạy tiếp; có video dự phòng 3–5 phút.
7. Mọi số đo trong báo cáo đo ít nhất 3 lần, lấy trung vị, kèm cỡ dữ liệu và cấu hình (số worker, core, RAM); có một cặp số trước/sau khi đổi `spark.sql.shuffle.partitions`.
8. Báo cáo đủ khung 7 chương của rubric, trích 6 tài liệu chuẩn của môn có chương/trang, có phụ lục khai báo phạm vi dùng công cụ AI và bảng phân công có chữ ký; repo có tag `final`.

## 2. Cách làm

**Lát mỏng đầu-cuối.** Lát 1 nối một đường tối thiểu chạy thông từ Bộ phát lại tới Grafana bằng mã thật. Mỗi lát sau làm dày thêm một phần của đường đó. Không có giai đoạn viết mã thử rồi bỏ: các rủi ro ghép nối ở Mục 13 của tài liệu kiến trúc được kiểm chứng ngay trong lát đầu tiên chạm tới chúng.

**Một luồng việc tuần tự.** Mã do một người làm cùng Claude, lát này xong mới sang lát kia. Không chia nhánh cho phần mã. Phân vai trong nhóm chỉ áp cho báo cáo, slide và giảng chéo (lát 6).

**Mỗi lát một spec.** Trước khi viết mã của một lát, viết spec và các issue của nó trong `.scratch/<tên-lát>/`, theo mẫu `.scratch/huan-luyen-phien-ban-dau/`.

**Spark chạy trên cụm ngay từ lát 1.** Một master và hai worker, mỗi worker 2 core và 2 GB, đều là container trên cùng laptop. Job luồng bị giới hạn 2 core (`spark.cores.max=2`) để job huấn luyện lại luôn có 2 core còn lại; không đặt trần thì job luồng chiếm hết cụm và job của Airflow chờ mãi.

## 3. Hiện trạng

Đã có và đã chạy trên dữ liệu thật:

| Phần | File | Ghi chú |
|---|---|---|
| Image Docker | `docker/Dockerfile` | Python 3.12, Java 17, PySpark 4.1.3, thư viện Kafka cho Spark, driver JDBC Postgres, `confluent-kafka` |
| Toàn bộ hệ thống, một lệnh dựng | `docker-compose.yml`, `docker/postgres/`, `docker/grafana/` | Kafka, Postgres, Grafana, cụm Spark 1 master + 2 worker, `init`, `spark`, `simulator` |
| Đọc C-MAPSS, lược đồ, 16 cảm biến | `src/pdm/schema.py` | |
| Điều kiện vận hành, chuẩn hóa, cửa sổ trượt | `src/pdm/features.py` | Một hàm cho cả DataFrame tĩnh và luồng; chế độ luồng đã kiểm thử và chạy trên cụm |
| RUL và nhãn Sắp hỏng | `src/pdm/labels.py` | |
| Quy tắc Cổng kiểm định | `src/pdm/gate.py` | |
| Chia ba nhóm Máy, bảng thống kê chuẩn hóa | `src/jobs/prepare_data.py` | 496 / 71 / 142 Máy |
| Huấn luyện hai mô hình GBT | `src/jobs/train.py` | Tham số tỷ lệ nhóm gốc đem học |
| Bộ phát lại | `src/simulator/replay.py` | 500 Máy ảo, 500 events/giây; chưa có sự kiện hỏng, Lỗi chèn |
| Job luồng, truy vấn chấm điểm | `src/jobs/stream.py` | Ghi `predictions`; nạp lại mô hình khi con trỏ đổi (chưa thử) |
| Cổng kiểm định | `src/jobs/validate.py` | Ghi kết quả ra file, chưa ghi Postgres |
| Kiểm thử | `tests/test_pdm.py`, `tests/test_replay.py` | |

Đo trên nhóm kiểm định: phiên bản học cả nhóm huấn luyện gốc (`v20261013T1719`) RMSE 18,7, precision 0,85, recall 0,87, F1 0,86; phiên bản học 20% (99 Máy, Mô hình đang dùng hiện tại) RMSE 21,4, precision 0,85, recall 0,84, F1 0,84. Dự đoán hằng số: RMSE 44,9.

Số đo của lát 1 (thông lượng, độ trễ, RAM) và kết quả kiểm chứng các rủi ro ghép nối nằm ở Mục 13 của tài liệu kiến trúc.

Mã sẽ thêm:

```
src/pdm/cleaning.py       lọc miền hợp lệ, khử trùng                     (lát 4)
src/pdm/orders.py         quy tắc sinh Lệnh bảo trì                      (lát 2)
src/simulator/replay.py   sự kiện hỏng, Lỗi chèn, nhật ký                (lát 2, 4)
src/jobs/stream.py        truy vấn lưu kho, sự kiện hỏng, Lệnh bảo trì   (lát 2, 4)
src/jobs/build_training_set.py   ghép kho với nhóm huấn luyện gốc        (lát 3)
src/jobs/evaluate_test.py        đo trên tập test chính thức             (lát 5)
dags/retrain_rul.py       DAG huấn luyện lại                             (lát 3)
```

## 4. Các lát

| Lát | Tên | Kết thúc bằng | Điều kiện hoàn thành đạt được |
|---|---|---|---|
| 1 | Đường chấm điểm tối thiểu | Dự đoán RUL của 500 Máy ảo hiện trên Grafana | 1, một phần 2 |
| 2 | Lệnh bảo trì và kho | Lệnh bảo trì ra trước khi Máy hỏng; Bản tin nằm trong kho | 3 |
| 3 | Huấn luyện lại bằng Airflow | Một lần được nhận, một lần bị từ chối, job luồng tự đổi mô hình | 4 |
| 4 | Lỗi chèn và làm sạch | Kết quả làm sạch khớp nhật ký | — |
| 5 | Đo | Bảng số liệu cho báo cáo | 2, 5, 7 |
| 6 | Hoàn thiện | Demo, báo cáo, slide | 6, 8 |

Airflow đi trước Lỗi chèn vì nó là chỗ ghép nối chưa kiểm chứng lớn nhất và là phần đề yêu cầu trực tiếp, còn một phần của lát 4 nằm trong danh sách cắt giảm (Mục 5). Hệ quả: trong lát 2 và 3, kho chứa Bản tin chưa qua bước làm sạch; vô hại vì lúc đó Bộ phát lại chưa chèn lỗi.

### Lát 1 — Đường chấm điểm tối thiểu (xong 14/10/2026)

| Việc | Xong khi |
|---|---|
| Thêm vào image: thư viện Kafka cho Spark (đúng bản 4.1.3), driver JDBC của Postgres, client Kafka cho Python | Build xong; job không tải gì từ mạng lúc chạy |
| `docker-compose.yml`: `kafka`, `postgres`, `grafana`, `spark-master`, hai `spark-worker`, `init`, `spark` (driver của job luồng), `simulator`. `models/`, `data/`, `src/` gắn cùng đường dẫn vào mọi container Spark; checkpoint nằm trên một volume của Docker | `docker compose up -d` trên máy sạch: mọi dịch vụ sống sau 5 phút, giao diện master thấy 2 worker |
| `train.py` nhận tỷ lệ nhóm huấn luyện gốc đem học; `init` chạy `prepare_data.py` rồi `train.py` với tỷ lệ 20% nếu chưa có `current.json`; `spark` và `simulator` chờ `init` thoát thành công | Xóa `models/` rồi `docker compose up`: hệ thống tự có phiên bản đầu tiên |
| Bảng `predictions` tạo tự động khi dựng Postgres | Bảng có sẵn sau `up` |
| Bộ phát lại: giữ 500 Máy ảo sống, mỗi máy một Bản tin mỗi giây vào `sensor.raw`, nhiễu Gauss riêng từng máy, bắt đầu ở Chu kỳ ngẫu nhiên, máy hết vòng đời thì sinh máy thay thế; in thông lượng mỗi 10 giây | Kafka nhận ≥ 500 Bản tin/giây trong 10 phút |
| Truy vấn chấm điểm: đọc Kafka, phân tích JSON, watermark 10 giây, chuẩn hóa, cửa sổ trượt, bỏ cửa sổ dưới 20 Bản tin; trong `foreachBatch` nạp Mô hình đang dùng, áp hai mô hình, ghi `predictions` | Mỗi Máy ảo có một dự đoán mỗi 5 giây |
| Kiểm thử: hàm đặc trưng cho cùng kết quả ở chế độ tĩnh và chế độ luồng trên cùng dữ liệu | Kiểm thử xanh |
| Một ô Grafana: RUL dự đoán theo thời gian của vài Máy, khai báo sẵn trong repo | Ô có dữ liệu sống ngay sau `up` |
| Đo RAM thật của từng container (`docker stats`) | Có bảng số; biết còn bao nhiêu cho Airflow |

Rủi ro được kiểm chứng ở lát này, và phương án lùi:

- Master và worker khởi động được từ PySpark cài bằng pip trong image của dự án. Nếu không: dùng image `apache/spark` cùng bản cho master và worker, giữ image của dự án cho driver.
- Áp `PipelineModel` trong `foreachBatch` khi chạy trên cụm, với mô hình đọc từ thư mục gắn chung. Nếu executor không đọc được: kiểm tra đường dẫn gắn giống nhau ở mọi container.
- Checkpoint trên volume của Docker, executor ghi trạng thái vào đó.
- Bộ phát lại bằng Python đạt 500 events/giây. Nếu không: gửi theo lô mỗi giây; nếu vẫn thiếu, chạy hai tiến trình phát.
- Tổng RAM vượt sức laptop. Nếu vượt: giảm mỗi worker xuống 1,5 GB; Airflow chỉ bật khi chạy DAG.

Đã làm: `models/` cũ (gồm `v20261013T1719` học cả nhóm gốc) chuyển sang `data/models_before_slice1/`; `init` dựng lại từ đầu với phiên bản đầu tiên học 20%.

### Lát 2 — Lệnh bảo trì và kho

| Việc | Xong khi |
|---|---|
| Bộ phát lại phát sự kiện vào `machine.failed` khi một Máy ảo hết vòng đời | Khoảng 2 sự kiện mỗi giây ở 500 Máy ảo |
| Truy vấn sự kiện hỏng: ghi `failures/` trong kho và bảng `failures` | Bảng tăng đều khi Bộ phát lại chạy |
| Truy vấn lưu kho: ghi Bản tin ra `readings/`, phân vùng theo ngày, mỗi phút một lô | Số Bản tin trong kho khớp số đã phát |
| `orders.py`: quy tắc ở Mục 8 của tài liệu kiến trúc, có kiểm thử cho thời điểm đề xuất và Khoảng an toàn | Kiểm thử xanh |
| Sinh Lệnh bảo trì trong `foreachBatch`, ghi `maintenance_orders` kèm `ON CONFLICT DO NOTHING` | Mỗi Máy ảo có nhiều nhất một lệnh |
| Nạp lại mô hình khi `current.json` đổi | Đổi con trỏ bằng tay: cột phiên bản trong `predictions` đổi theo, job không khởi động lại |
| Ô Grafana thứ hai: Lệnh bảo trì mới nhất | Hai ô bắt buộc có dữ liệu sống |

Tiêu chí nghiệm thu: chạy liên tục 30 phút ở 500 events/giây không lỗi, độ trễ đầu-cuối không tăng dần; một Máy ảo gần hỏng có Lệnh bảo trì trước sự kiện hỏng của nó.

Điểm dừng để quyết định: nếu cửa sổ trượt trên luồng không chạy ổn trên cụm, chuyển sang tính toàn bộ đặc trưng trong `foreachBatch` (giữ 30 giây Bản tin gần nhất của mỗi Máy).

### Lát 3 — Huấn luyện lại bằng Airflow

| Việc | Xong khi |
|---|---|
| Thêm Airflow 3.3.2 vào image dùng chung; dịch vụ `airflow` chế độ `standalone`; database `airflow` trong Postgres | Giao diện Airflow mở được; đo lại RAM |
| `build_training_set.py`: ghép `readings` với `failures`, lấy tối đa 500 Máy hỏng gần nhất, gán nhãn, gộp với cả nhóm huấn luyện gốc | Số Máy = 496 + min(500, số Máy ảo đã hỏng); không có Máy nào của nhóm kiểm định |
| `train.py` huấn luyện được trên tập do bước trên dựng | Sinh một thư mục phiên bản mới |
| `validate.py` ghi bảng `model_validation` thay cho file | Mỗi lần chạy thêm một dòng |
| DAG `retrain_rul`: `build_training_set >> train >> validate >> branch >> promote / keep_current`, mỗi bước `spark-submit` lên cụm | DAG chạy xanh; `current.json` chỉ đổi khi ứng viên được nhận |
| Lần được nhận: chạy DAG trên hệ thống vừa dựng (Mô hình đang dùng học 20% nhóm gốc) | Dòng "được nhận"; cột phiên bản trong `predictions` đổi; lặp lại được từ máy sạch |
| Lần bị từ chối: chạy DAG với tham số huấn luyện trên rất ít Máy | Dòng "không nhận"; `current.json` không đổi |

Rủi ro và phương án lùi:

- Airflow 3 trục trặc quá nửa ngày: dùng Airflow 2.11.
- Airflow và PySpark không cài chung được trong một image: tách image Airflow, gọi `spark-submit` qua `docker exec` vào container `spark`.
- Ứng viên học đủ vẫn không thắng rõ bản học 20%: hạ tỷ lệ của phiên bản đầu tiên.
- Nếu trễ: gộp ba bước đầu thành một lệnh `spark-submit`, DAG còn hai bước.

### Lát 4 — Lỗi chèn và làm sạch

| Việc | Xong khi |
|---|---|
| Bốn loại Lỗi chèn trong Bộ phát lại, mỗi loại bật/tắt và chỉnh tỷ lệ bằng tham số | Chạy từng loại riêng lẻ, đếm trong Kafka khớp tỷ lệ đặt |
| Nhật ký: ánh xạ Máy ảo → vòng đời nguồn, và từng Lỗi chèn | File đọc được bằng Spark |
| `cleaning.py`: lọc miền hợp lệ theo [min, max] của nhóm huấn luyện gốc, khử trùng theo (`machine_id`, `cycle`); có kiểm thử | Kiểm thử xanh |
| Truy vấn lưu kho và truy vấn chấm điểm cùng gọi hàm làm sạch | Số Bản tin trong kho = số đã phát − số bị loại theo nhật ký |

Rủi ro: khử trùng nối với gom cửa sổ (hai phép có trạng thái) không chạy ở chế độ append. Phương án lùi: khử trùng trong `foreachBatch`.

Tiêu chí nghiệm thu: bật cả bốn loại lỗi, chạy 30 phút, hai ô Grafana vẫn đúng và độ trễ không tăng dần.

### Lát 5 — Đo

| Việc | Xong khi |
|---|---|
| `evaluate_test.py`: huấn luyện một phiên bản trên cả nhóm gốc, đo trên 707 Máy test, bỏ ngưỡng 20 Bản tin | RMSE gộp và riêng từng tập con; precision/recall |
| Chạy 10 phút ở 500 events/giây; ghi thông lượng tại Bộ phát lại, tại Kafka, và tốc độ xử lý của Spark | Có bảng số liệu |
| Độ trễ đầu-cuối từ `predictions` (thời điểm ghi − cuối cửa sổ) | Trung vị và phân vị 95 |
| Ba chỉ số của Lệnh bảo trì (Mục 8 của tài liệu kiến trúc); chỉnh N và Khoảng an toàn nếu cần | Có số liệu |
| Cặp trước/sau: `spark.sql.shuffle.partitions` = 200 rồi = số core của job luồng | Hai cột số và một câu giải thích |
| Thời gian `train.py` với 1 worker và với 2 worker | Hai con số, kèm số executor, core, RAM |
| Dung lượng: GB/ngày vào Kafka và vào kho ở 500 events/giây | Có con số |
| Phục hồi: `docker compose stop spark`, chờ, bật lại | Job đọc tiếp từ checkpoint; không Máy nào có hai Lệnh bảo trì |

Mỗi phép đo chạy 3 lần, báo trung vị, ghi kèm cỡ dữ liệu và cấu hình. Chỉ đo một mức tải; mức cao hơn làm nếu còn thời gian.

Tiêu chí nghiệm thu: RMSE trên FD001 nằm trong khoảng hợp lý so với các bài nghiên cứu dùng mô hình cây; nếu lệch xa, kiểm tra bước chuẩn hóa trước tiên.

### Lát 6 — Hoàn thiện

| Việc | Xong khi |
|---|---|
| Kịch bản demo từng bước, kèm lệnh | Người kia trong nhóm làm theo được |
| Demo từ máy sạch | Chạy trọn hai lần liên tiếp không sửa tay |
| Bước phục hồi trong demo (thêm `restart: unless-stopped`) | Tập thành công ít nhất 3 lần; nếu không ổn thì chỉ để trong video |
| Video demo dự phòng | Có file video |
| Báo cáo theo khung 7 chương: bài toán, dữ liệu, kiến trúc, hiện thực, thực nghiệm, an toàn và vận hành, kết luận đối chiếu tiêu chí ở chương 1 | PDF 20–30 trang; chương 2 khai báo dữ liệu là mô phỏng; chương 6 có ít nhất 10 dòng (credential trong file env, lịch DAG, phục hồi từ checkpoint) |
| Trích 6 tài liệu chuẩn của môn, có chương/trang, cạnh các bài nghiên cứu về bảo trì dự đoán | Mỗi khối trong sơ đồ kiến trúc có một câu lý do và một tham chiếu |
| Phụ lục: khai báo phạm vi dùng công cụ AI; bảng phân công đóng góp có chữ ký | Có trong báo cáo |
| Slide 10–12 trang, trang cuối là "3 kết quả chính" | Tập trình bày bấm giờ 2 lần, không quá 12 phút |
| Giảng chéo: mỗi người trình bày lại mọi khối trong sơ đồ cho người kia | Cả hai trả lời được câu hỏi về mọi khối |
| README hướng dẫn dựng lại | Thành viên kia làm theo README trên máy khác được |
| Gắn tag `final` | Tag có trên remote |

Lưu ý cho báo cáo: mọi bài nghiên cứu phải được đọc toàn văn trước khi trích; số liệu thực nghiệm chỉ lấy từ lần chạy thật của nhóm. Khung báo cáo có thể bắt đầu viết từ sau lát 2, khi kiến trúc đã chạy thông.

## 5. Thứ tự cắt giảm khi trễ

Cắt từ trên xuống. Mục cuối không được cắt vì là phần đề yêu cầu trực tiếp.

1. Ba ô Grafana không bắt buộc.
2. Đo riêng từng tập con của C-MAPSS; đo thời gian huấn luyện 1 worker so với 2 worker.
3. Lỗi chèn loại "Bản tin trùng" và bước khử trùng.
4. Chỉ số đánh giá Lệnh bảo trì (giữ việc sinh lệnh).
5. Dùng lại FD001 và FD003 (một Điều kiện vận hành), bỏ FD002 và FD004.
6. **Không cắt:** Kafka ≥ 500 events/giây; làm sạch và đặc trưng cửa sổ trượt trên luồng; cả hai mô hình GBT (đề ghi "mô hình phân lớp", còn mô hình hồi quy cho thời điểm đề xuất bảo trì); huấn luyện lại có Cổng kiểm định bằng Airflow.

## 6. Câu hỏi còn mở với giảng viên

- 500 events/giây đo ở đâu? Trong lúc chờ, lát 5 đo cả ở Bộ phát lại lẫn Kafka.
- Cụm Spark gồm nhiều container trên một laptop có được chấp nhận là "tính toán phân tán" không?
- Tiêu chí chấm: phần nào nặng điểm nhất?
- "Mô hình phân lớp ước lượng RUL" hiểu là phân lớp, hồi quy, hay cả hai? Thiết kế làm cả hai.
- Dùng dữ liệu C-MAPSS (không có cảm biến độ rung) có được không?
