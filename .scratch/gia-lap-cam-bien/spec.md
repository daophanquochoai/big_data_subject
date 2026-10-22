# Spec: Bộ giả lập cảm biến (v2)

Status: draft, chờ duyệt

Thuật ngữ in hoa (Máy, Máy ảo, Bản tin, Chu kỳ, RUL, Điều kiện vận hành, Lỗi chèn) theo Mục 15 của `v1/docs/architecture.md`.

## Problem Statement

v2 dựng lại hệ thống từ đầu, bắt đầu từ nguồn dữ liệu. Bộ phát lại của v1 là một script không có giao diện: muốn đổi số máy, mức nhiễu hay bật Lỗi chèn phải sửa lệnh rồi khởi động lại, và nó chỉ làm được một việc là phát lại vòng đời gốc cộng nhiễu, nên đường xuống cấp của mọi Máy ảo vẫn trùng với 709 Máy của C-MAPSS. Nhóm cần một công cụ để, trong lúc phần Spark phía sau đang chạy, chỉnh tải, chỉnh cách sinh dữ liệu và chèn lỗi ngay lập tức, nhìn thấy dữ liệu sinh ra có hợp lý không, và có đáp án để đối chiếu.

## Solution

Một ứng dụng trong `v2/sensor/`: BE FastAPI chứa bộ máy sinh và gửi Bản tin vào Kafka, FE React để điều khiển và theo dõi. Bộ máy sinh tạo Máy ảo mới từ các vòng đời C-MAPSS trong `v2/data/raw/cmapss` bằng bốn phương pháp (nhiễu Gauss, co giãn thời gian, trộn hai vòng đời, mô hình xuống cấp tham số), phát N × k Bản tin mỗi giây (tối đa khoảng 5.000), chèn bốn loại lỗi theo tỷ lệ chỉnh được, và gắn đáp án vào từng Bản tin trong một trường lồng `truth` mà phần xử lý bỏ qua. Mọi thay đổi áp ngay khi đang chạy; cấu hình lưu được thành kịch bản có tên.

Người dùng là nhóm phát triển, không phải người xem demo: ưu tiên nhiều tham số và số liệu chi tiết hơn giao diện đẹp.

## User Stories

1. Là thành viên nhóm, tôi muốn chạy, dừng và tạo lại đội Máy ảo từ giao diện, để không phải gõ lệnh.
2. Là thành viên nhóm, tôi muốn chỉnh số Máy ảo N và số Chu kỳ mỗi giây của mỗi máy k khi đang chạy, để thử tải N × k events/giây mà không khởi động lại.
3. Là thành viên nhóm, tôi muốn thử tải tới khoảng 5.000 events/giây, để đo cụm Spark ở mức gấp 10 lần yêu cầu của đề.
4. Là thành viên nhóm, tôi muốn chọn tập con FD001–FD004 làm nguồn, để thử riêng trường hợp một hay sáu Điều kiện vận hành.
5. Là thành viên nhóm, tôi muốn Máy ảo được sinh bằng bốn phương pháp với tỷ lệ trộn chỉnh được, để dữ liệu luồng không chỉ là bản sao có nhiễu của dữ liệu huấn luyện.
6. Là thành viên nhóm, tôi muốn xem trước một máy sinh thử cạnh máy gốc trước khi bắn, để biết phương pháp và tham số cho ra dữ liệu hợp lý.
7. Là thành viên nhóm, tôi muốn bật từng loại Lỗi chèn và chỉnh tỷ lệ, để thử phần làm sạch.
8. Là thành viên nhóm, tôi muốn mỗi Bản tin mang đáp án (RUL thật, phương pháp sinh, loại lỗi) trong trường riêng, để phần đánh giá đối chiếu được mà phần xử lý không vô tình dùng.
9. Là thành viên nhóm, tôi muốn một sự kiện khi Máy ảo hỏng kèm nguồn gốc của nó, để gán nhãn khi huấn luyện lại và so phương pháp sinh nào khó dự đoán hơn.
10. Là thành viên nhóm, tôi muốn thấy mỗi giây số Bản tin đã gửi và đã được Kafka xác nhận, số máy sống, đã hỏng, số Lỗi chèn theo loại và số nhịp quá hạn, để biết bộ giả lập có theo kịp không.
11. Là thành viên nhóm, tôi muốn xem danh sách Máy ảo với nguồn gốc, Chu kỳ và RUL thật, để chọn máy cần theo dõi.
12. Là thành viên nhóm, tôi muốn mở biểu đồ sống của một máy, gồm trọn vòng đời đã sinh và các Bản tin vừa phát, để thấy nhiễu và lỗi đặt lên đường xuống cấp ra sao.
13. Là thành viên nhóm, tôi muốn lưu cấu hình thành kịch bản có tên và nạp lại, để chạy lại đúng một tình huống.
14. Là thành viên nhóm, tôi muốn cùng kịch bản và cùng hạt giống cho cùng chuỗi Bản tin, để so hai lần chạy của phần Spark.
15. Là thành viên nhóm, tôi muốn mã Máy ảo không bao giờ lặp lại, kể cả sau khi tạo lại hay khởi động lại, để lịch sử hai máy khác nhau không lẫn vào nhau.
16. Là thành viên nhóm, tôi muốn bộ giả lập không chết khi Kafka gặp sự cố, để chỉ cần chờ Kafka trở lại.
17. Là thành viên nhóm, tôi muốn một lệnh `docker compose up` dựng Kafka và bộ giả lập, mở `http://localhost:8000` là dùng được.

## Implementation Decisions

### Hợp đồng dữ liệu trên Kafka

- Topic `sensor.raw`, 3 partition, khóa là `machine_id`. Bản tin JSON:

  ```json
  {
    "machine_id": "vm-000123",
    "cycle": 87,
    "event_time": "2026-10-15T09:30:12.400Z",
    "setting1": 20.0004, "setting2": 0.7002, "setting3": 100.0,
    "s1": 491.19, "...": "...", "s21": 14.8301,
    "truth": {"rul": 153, "method": "warp", "fault": null}
  }
  ```

  - Phần ngoài `truth` giữ đúng lược đồ Bản tin của v1 (Mục 5 của `v1/docs/architecture.md`).
  - `event_time` là thời điểm của nhịp phát, dạng ISO-8601 UTC, độ chính xác mili giây. Với k Chu kỳ mỗi giây, hai Bản tin liên tiếp của một máy cách nhau khoảng 1/k giây: phần Spark của v2 phải gom cửa sổ theo `cycle`, không theo giây.
  - `truth.rul` = độ dài vòng đời của Máy ảo − `cycle`; bằng 0 ở Chu kỳ cuối. `truth.method` ∈ `gauss | warp | blend | parametric`. `truth.fault` ∈ `null | "bad_value" | "late" | "duplicate"`.
  - Job xử lý của Spark phải phân tích Bản tin bằng lược đồ cố định không có `truth`; chỉ job đánh giá dùng lược đồ có `truth`.
- Topic `machine.failed`, 1 partition, khóa là `machine_id`, phát khi Máy ảo phát xong Chu kỳ cuối:

  ```json
  {"machine_id": "vm-000123", "failed_at": "2026-10-15T09:32:45.000Z", "last_cycle": 240,
   "method": "blend", "base": ["FD002-017", "FD002-104"], "params": {"weight": 0.37}}
  ```

  `base` là mã các Máy nguồn (cách đặt mã như v1: tên tập con + số máy đệm ba chữ số); với `parametric` là máy cho chuỗi thiết lập. `params` là tham số đã bốc cho máy đó: `{"factor": f}` với `warp`, `{"weight": w}` với `blend`, rỗng với `gauss` và `parametric`.
- BE tạo hai topic khi khởi động nếu chưa có (`sensor.raw` 3 partition, `machine.failed` 1 partition), không dựa vào tự tạo của broker.

### Bộ máy sinh (`engine/`)

Python thuần cộng numpy, không đụng mạng hay đồng hồ: nhận nguồn ngẫu nhiên và thời điểm từ bên ngoài, trả về Bản tin; kiểm thử được độc lập.

- **Nguồn.** Đọc `train_FD00x.txt` của các tập con được chọn (đọc cả bốn một lần lúc khởi động, lọc theo cấu hình). Tính bảng thống kê: trung bình μ và độ lệch chuẩn σ của từng cảm biến trong từng Điều kiện vận hành, trên toàn bộ máy nguồn. Điều kiện vận hành suy ra bằng cách làm tròn ba cột thiết lập như v1.
- **Không biến đổi trên số đo thô.** FD002 và FD004 đổi Điều kiện vận hành gần như mỗi Chu kỳ, nên mọi phương pháp làm theo ba bước:
  1. chuẩn hóa 16 cảm biến mang thông tin: z = (x − μ_c) / σ_c theo điều kiện c của Chu kỳ đó;
  2. biến đổi chuỗi z theo phương pháp;
  3. gán cho mỗi Chu kỳ mới một dòng thiết lập (ba cột thiết lập lấy nguyên từ một Chu kỳ của máy nguồn, như mô tả từng phương pháp), rồi đổi ngược x = z · σ_c + μ_c.

  Năm cảm biến hằng số (1, 5, 16, 18, 19) không chuẩn hóa: lấy nguyên giá trị thô ở Chu kỳ nguồn tương ứng (với `parametric`: của máy cho chuỗi thiết lập). Dữ liệu hiện có không có cảm biến mang thông tin nào với σ_c = 0; bộ đọc dữ liệu dừng với thông báo nếu gặp.
- **Bốn phương pháp.** Mỗi Máy ảo được sinh sẵn trọn vòng đời (mảng Chu kỳ × 24 cột) lúc ra đời:

  | Phương pháp | Vòng đời mới |
  |---|---|
  | `gauss` | Một máy nguồn ngẫu nhiên, giữ nguyên |
  | `warp` | Một máy nguồn độ dài L, hệ số f ~ U(f_min, f_max). Độ dài mới L' = max(2, round(L·f)). Chu kỳ mới i (0..L'−1) ứng với vị trí i·(L−1)/(L'−1) trên máy nguồn: z nội suy tuyến tính, thiết lập lấy theo Chu kỳ nguồn gần nhất |
  | `blend` | Hai máy nguồn khác nhau cùng tập con, w ~ U(0, 1). L' = round(w·L₁ + (1−w)·L₂). Mỗi máy được co về độ dài L' như `warp`, z = w·z₁ + (1−w)·z₂; thiết lập lấy theo máy có trọng số lớn hơn |
  | `parametric` | Khớp mỗi cảm biến của mỗi máy nguồn với z(τ) = a + b·e^(c·τ), τ = t/L ∈ [0, 1]: duyệt c trên lưới 0,5…10 (bước 0,25), với mỗi c giải a, b bằng bình phương tối thiểu, giữ c có sai số nhỏ nhất, ghi kèm độ lệch chuẩn phần dư r. Mỗi đường lưu dưới dạng (z0 = a + b, z1 = a + b·e^c, c) thay vì (a, b, c): b và c phụ thuộc phi tuyến nên bốc thẳng (a, b, c) từ phân bố chuẩn ra đường mũ bùng nổ (đã gặp: z tới ±25.000). Mỗi tập con: khớp phân bố chuẩn nhiều chiều trên vectơ (L, z0, z1, c, r của 16 cảm biến) của các máy nguồn, thêm 1e-6 vào đường chéo. Máy mới: bốc một vectơ, chặn L ≥ 30, r ≥ 0 và c trong lưới, suy ra b = (z1 − z0)/(e^c − 1), a = z0 − b, sinh z = a + b·e^(c·τ) + N(0, r). Thiết lập: lấy chuỗi thiết lập của một máy nguồn ngẫu nhiên cùng tập con, co về độ dài L như `warp` |

  Khớp `parametric` chạy một lần lúc khởi động, chỉ dùng numpy.
- **Nhiễu.** Mọi phương pháp, ở bước phát: cộng N(0, (noise · σ_c)²) vào 16 cảm biến mang thông tin. Ba cột thiết lập và năm cảm biến hằng số không cộng nhiễu.
- **Đội máy.** Giữ đúng N Máy ảo đang sống.
  - Máy mới chọn phương pháp theo tỷ lệ trộn, chọn tập con theo tỷ lệ số máy nguồn của các tập con đang bật.
  - Đội ban đầu (lúc tạo lại) và máy thêm vào khi tăng N bắt đầu ở Chu kỳ ngẫu nhiên đều trong [1, L']; máy thay thế bắt đầu ở Chu kỳ 1.
  - Bộ đếm của mỗi máy bắt đầu ở một pha ngẫu nhiên, để ở k ≤ 1 các máy không phát dồn vào cùng một nhịp.
  - Nhịp 100 ms. Mỗi nhịp mỗi máy cộng k × 0,1 vào bộ đếm và phát phần nguyên Bản tin (k ≤ 10 nên tối đa 1 Bản tin mỗi máy mỗi nhịp). Bản tin của một nhịp mang cùng `event_time` là thời điểm nhịp.
  - Máy phát xong Chu kỳ cuối: phát `machine.failed` trong cùng nhịp, thay bằng máy mới.
  - Mã Máy ảo `vm-` + số tăng dần đệm sáu chữ số. Số kế tiếp lưu trong volume (cùng chỗ với kịch bản), ghi trước khi máy mới phát Bản tin đầu tiên, nên không lặp lại qua tạo lại hay khởi động lại.
- **Lỗi chèn.** Mỗi Bản tin bốc đúng một kết cục: bình thường, `bad_value`, `drop`, `late` hoặc `duplicate`, theo xác suất cấu hình (tổng ≤ 1).
  - `bad_value`: một cảm biến ngẫu nhiên trong 21 cảm biến thành −9999 hoặc gấp 10 lần (mỗi bên 50%); `truth.fault = "bad_value"`.
  - `drop`: không gửi; chỉ tăng bộ đếm.
  - `late`: giữ lại d ~ U(1, late_max_s) giây rồi gửi, `event_time` giữ nguyên; `truth.fault = "late"`.
  - `duplicate`: gửi Bản tin gốc (`fault: null`) rồi gửi thêm một bản sao trong cùng nhịp với `fault: "duplicate"`.
- **Tái lập.** Một nguồn ngẫu nhiên numpy (`Generator`) khởi tạo từ hạt giống của cấu hình ở mỗi lần tạo lại. Cùng cấu hình, cùng hạt giống, không chỉnh gì giữa chừng thì cho cùng chuỗi Bản tin, trừ phần số trong mã máy (được đánh tiếp) và `event_time`.

### BE (FastAPI)

- Bộ máy sinh và việc gửi Kafka chạy trong một luồng riêng; API chạy trên asyncio. Cấu hình là một đối tượng bất biến; `PATCH` dựng đối tượng mới và thay nguyên, luồng phát đọc đối tượng hiện hành ở đầu mỗi nhịp.
- Gửi Kafka bằng `confluent-kafka`, mã hóa bằng `orjson`, không chờ xác nhận mỗi nhịp (`poll(0)`); số đã xác nhận đếm qua callback. Hàng đợi gửi đầy (`BufferError`): bỏ phần còn lại của nhịp đó, đếm vào "bỏ do nghẽn", không để tiến trình chết. Lỗi gửi đếm vào "lỗi Kafka".
- Cấu hình (giá trị mặc định; giới hạn kiểm bằng pydantic, sai trả 422 kèm lý do):

  | Trường | Mặc định | Giới hạn |
  |---|---|---|
  | `machines` | 500 | 1–10.000 |
  | `speed` (k) | 1,0 | 0,1–10 |
  | `noise` | 0,1 | 0–1 |
  | `subsets` | cả bốn | khác rỗng |
  | `method_weights` | mỗi phương pháp 1 | ≥ 0, tổng > 0; lát 1 chỉ nhận `gauss`, `warp` |
  | `warp_range` | [0,7; 1,5] | 0,3–3, min ≤ max |
  | `faults` | `bad_value`, `drop`, `late`, `duplicate` đều 0 | mỗi cái 0–1, tổng ≤ 1 |
  | `late_max_s` | 5 | 1–60 |
  | `seed` | 42 | số nguyên ≥ 0 |

  Đổi `machines`: thêm hoặc bớt máy ngay, kể cả khi đang dừng (bớt máy thì bỏ máy mới nhất, không phát `machine.failed`). Đổi `speed`, `noise`, `faults`, `late_max_s`: áp từ nhịp kế. Đổi `subsets`, `method_weights`, `warp_range`: áp cho máy sinh sau đó. Đổi `seed`: chỉ có tác dụng ở lần tạo lại kế tiếp.
- Đường dẫn:

  | Đường dẫn | Việc |
  |---|---|
  | `GET /api/config`, `PATCH /api/config` | Đọc, sửa cấu hình |
  | `POST /api/start`, `POST /api/stop`, `POST /api/reset` | Chạy; dừng (giữ đội máy và vị trí); tạo lại đội máy từ hạt giống (dừng nếu đang chạy) |
  | `GET /api/status` | Đang chạy hay không, trạng thái kết nối Kafka |
  | `GET /api/machines?method=&sort=&page=` | Danh sách: mã, phương pháp, `base`, `params`, Chu kỳ hiện tại, độ dài vòng đời, RUL thật; 50 dòng mỗi trang |
  | `GET /api/machines/{id}` | Trọn vòng đời đã sinh (chưa nhiễu) của máy đó; 404 nếu máy không còn sống |
  | `POST /api/preview` | Body: phương pháp, tập con, tham số, hạt giống. Trả vòng đời của máy nguồn (với `blend`: cả hai) và máy sinh; không đụng đội máy đang chạy |
  | `GET /api/scenarios`, `GET/PUT/DELETE /api/scenarios/{name}`, `POST /api/scenarios/{name}/load` | Kịch bản: lưu cấu hình hiện hành thành file JSON có tên; nạp thì thay cấu hình rồi tạo lại đội máy |
  | `WS /ws/stats` | Mỗi giây: events/giây đã gửi và đã xác nhận (cả hai topic), máy sống, máy đã hỏng (cộng dồn từ lần tạo lại), số Lỗi chèn theo loại, lỗi Kafka trong giây vừa qua, bỏ do nghẽn, nhịp quá 100 ms, độ dài hàng đợi gửi |
  | `WS /ws/machines/{id}` | Đẩy từng Bản tin của máy đó ngay khi phát (gồm cả lỗi); mỗi người xem một bộ đệm riêng. Máy hỏng hoặc bị bớt thì gửi `{"gone": true}` rồi đóng |

- Tên kịch bản: chữ, số, `-`, `_`, tối đa 64 ký tự (chặn đường dẫn lạ). Kịch bản lưu trong thư mục `/state/scenarios` trên volume.
- Cấu hình bên ngoài mã: `KAFKA_BOOTSTRAP` (mặc định `kafka:9092`), `DATA_DIR` (mặc định `/data/raw/cmapss`), `STATE_DIR` (mặc định `/state`).
- FastAPI phục vụ bản build tĩnh của FE ở `/`.

### FE (Vite + React + TypeScript)

- Recharts cho biểu đồ, CSS thường; không Redux, không bộ thư viện giao diện lớn. Gọi API bằng `fetch`, hai hook cho WebSocket, tự nối lại sau mỗi 2 giây và hiện "Mất kết nối" khi rớt.
- Một trang. Thanh trên: trạng thái chạy, trạng thái Kafka, nút Chạy / Dừng / Tạo lại, chọn và lưu kịch bản.
- Cột trái, bảng điều khiển: Tốc độ và quy mô (N, k), Sinh dữ liệu (tập con, tỷ lệ trộn bốn phương pháp, mức nhiễu, khoảng co giãn), Lỗi chèn (tỷ lệ từng loại, 0 là tắt, thanh trượt tới 20%; độ trễ tối đa), Hạt giống. Sửa tới đâu gửi `PATCH` tới đó sau 300 ms ngừng gõ; ô sai đổi đỏ và hiện lý do BE trả về.
- Tab Theo dõi: hàng ô số liệu từ `/ws/stats` (ô events/giây có đường 60 giây gần nhất của gửi và xác nhận); bảng Máy ảo phân trang, lọc theo phương pháp, sắp theo cột. Bấm một dòng mở ngăn bên phải: đường mảnh là trọn vòng đời đã sinh, chấm là Bản tin vừa phát nhận qua `/ws/machines/{id}`, vạch dọc là Chu kỳ hiện tại; chọn cảm biến để vẽ (mặc định s2, s4, s11, s15).
- Tab Xem trước: chọn phương pháp, tập con, tham số, hạt giống, bấm "Sinh thử"; biểu đồ máy sinh cạnh máy nguồn trên các cảm biến đã chọn; "Sinh thử khác" tăng hạt giống lên 1.
- Khi phát triển: `npm run dev` với proxy `/api` và `/ws` tới `localhost:8000`.

### Cấu trúc và triển khai

```
v2/
  docker-compose.yml        kafka + sensor; phần Spark thêm vào sau
  data/raw/cmapss/          gắn chỉ đọc vào /data/raw/cmapss
  sensor/
    Dockerfile              nhiều tầng: node build FE → python:3.12-slim chứa BE và FE tĩnh
    backend/
      requirements.txt      fastapi, uvicorn, numpy, confluent-kafka, orjson (phiên bản cố định)
      app/main.py           tạo app, phục vụ FE tĩnh
      app/api.py            REST và WebSocket
      app/runner.py         luồng phát: nhịp, gửi, bộ đếm
      app/kafka_sink.py     producer, tạo topic; giao diện chung với sink giả dùng trong kiểm thử
      engine/data.py        đọc C-MAPSS, bảng thống kê, chuẩn hóa và đổi ngược
      engine/generators.py  gauss, warp, blend, parametric
      engine/fleet.py       đội máy, cộng dồn k, thay máy
      engine/faults.py      Lỗi chèn
      tests/
    frontend/
```

- `kafka`: `apache/kafka:4.2.2`, một broker KRaft, cấu hình listener như v1, dữ liệu trên volume có tên.
- `sensor`: build từ `v2/sensor`, cổng 8000, chờ `kafka` khỏe. Volume có tên gắn vào `/state` (kịch bản, số mã kế tiếp). Không gắn mã nguồn từ Windows.

## Testing Decisions

Kiểm hành vi thấy được từ bên ngoài: cấu hình và hạt giống vào, Bản tin ra. Không kiểm cách bộ máy giữ trạng thái bên trong.

1. **Bộ sinh** (`engine/`, pytest, dữ liệu C-MAPSS thật và vài vòng đời dựng tay):
   - `warp`: độ dài nằm trong [L·f_min, L·f_max] (làm tròn); `blend`: độ dài nằm giữa L₁ và L₂.
   - Mọi phương pháp: ba cột thiết lập của mỗi Chu kỳ trùng một dòng thiết lập có thật của máy nguồn; RUL thật giảm đúng 1 mỗi Chu kỳ và bằng 0 ở Chu kỳ cuối; với s4 và s11, hiệu giữa trung bình z ở 20% cuối và 20% đầu vòng đời cùng dấu với máy nguồn (với `parametric`: cùng dấu với đa số máy nguồn của tập con).
   - Chuẩn hóa rồi đổi ngược một vòng đời không biến đổi cho lại đúng số đo gốc (sai số dưới 1e-9).
   - `warp`, `blend`, `parametric`: trên 200 máy, max |z| không quá 1,5 lần của dữ liệu thật; phân vị 1 và 99 không lệch quá 1 so với dữ liệu thật.
2. **Đội máy**: với k = 2,5, sau 100 nhịp (10 giây) mỗi máy phát đúng 25 Bản tin; với k = 1, mỗi nhịp phát 0,1 · N Bản tin tính trung bình trên 100 nhịp; số máy sống luôn bằng N; máy thay thế có mã chưa từng dùng và bắt đầu ở Chu kỳ 1; mỗi máy hết vòng đời sinh đúng một sự kiện hỏng với `last_cycle` bằng độ dài vòng đời.
3. **Lỗi chèn**: hạt giống cố định, 100.000 Bản tin, mỗi tỷ lệ đặt 0,05: tỷ lệ thực tế lệch không quá ±10% tỷ lệ đặt; Bản tin `late` đi ra sau ít nhất 1 giây với `event_time` gốc; mỗi `duplicate` đi kèm một bản gốc giống hệt ngoài `truth.fault`.
4. **Tái lập**: hai lần tạo lại với cùng cấu hình và hạt giống cho cùng chuỗi Bản tin (bỏ qua mã máy và `event_time`).
5. **API** (TestClient, sink giả, không cần Kafka): `PATCH` sai trả 422 với từng giới hạn ở bảng cấu hình; chạy/dừng/tạo lại đổi trạng thái; `preview` không làm đổi đội máy; lưu, liệt kê, nạp, xóa kịch bản; tên kịch bản lạ bị từ chối.
6. **FE**: `tsc --noEmit` và `vite build` không lỗi.
7. **Nghiệm thu bằng tay** (`docker compose up` trong `v2/`):
   - mở `http://localhost:8000`, đặt N = 500, k = 10 (5.000 events/giây), chạy 10 phút: số đã xác nhận xấp xỉ số đã gửi, không có nhịp quá hạn, không bỏ do nghẽn;
   - đọc thử vài Bản tin trong `sensor.raw` và `machine.failed`: đúng lược đồ ở trên;
   - tắt Kafka 30 giây rồi bật lại: bộ giả lập không chết, lỗi Kafka hiện trên giao diện rồi về 0.

## Các lát

Mỗi lát chạy được đầu-cuối và có kiểm thử của nó. Lát sau chỉ bắt đầu khi lát trước đạt "xong khi".

| Lát | Nội dung | Xong khi |
|---|---|---|
| 1 Lõi | `engine/data.py`; `gauss`, `warp`; đội máy N × k; Bản tin có `truth`; `machine.failed`; tạo topic; config, chạy/dừng/tạo lại, status, danh sách máy, `/ws/stats`; FE: thanh trên, bảng điều khiển phần Tốc độ và Sinh dữ liệu (hai phương pháp), ô số liệu, bảng máy; compose | Kiểm thử 1 (phần `gauss`, `warp`), 2, 4, 5 (phần lát 1), 6 xanh; nghiệm thu 7 đạt |
| 2 Sinh dữ liệu | `blend`, `parametric`; tab Xem trước; `GET /api/machines/{id}` | Kiểm thử 1 đủ bốn phương pháp xanh; xem trước được cả bốn phương pháp cạnh máy nguồn |
| 3 Lỗi chèn | `engine/faults.py`; phần Lỗi chèn của bảng điều khiển và ô số liệu | Kiểm thử 3 xanh; với mỗi loại lỗi đặt 5%, đếm `truth.fault` trong Kafka (và số đếm `drop` ở BE) khớp tỷ lệ |
| 4 Kịch bản và biểu đồ máy | Kịch bản; `/ws/machines/{id}`; ngăn biểu đồ sống | Kiểm thử 5 đủ phần kịch bản xanh; nạp một kịch bản hai lần cho cùng chuỗi Bản tin |

## Out of Scope

- Phần Spark, Postgres, Grafana, Airflow của v2.
- Danh sách máy loại trừ khỏi nguồn (cần khi phần Spark v2 huấn luyện trên cùng các máy train; thêm lúc đó).
- Bỏ cả đoạn Bản tin liên tục (chỉ có bỏ từng Bản tin).
- Nhiều tiến trình phát; tải trên khoảng 5.000 events/giây.
- Xác thực người dùng: công cụ chỉ chạy trên máy phát triển, cổng 8000 chỉ mở ở localhost.
- Đọc tập test chính thức của C-MAPSS làm nguồn (không có vòng đời trọn tới lúc hỏng).

## Further Notes

- Bài học từ v1 đã đưa vào thiết kế: không chờ broker xác nhận mỗi nhịp; không ghi file mỗi nhịp lên thư mục gắn từ Windows (trạng thái nằm trên volume); mã Máy ảo không lặp qua các lần khởi động.
- Dữ liệu sinh bằng `warp`, `blend`, `parametric` là dữ liệu tổng hợp; báo cáo phải nói rõ và chỉ đo RMSE "trên dữ liệu thật" bằng tập test chính thức.
