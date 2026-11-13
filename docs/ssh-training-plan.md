# Tách Training Control và thêm cụm máy SSH

> Tài liệu lịch sử: phần Máy train/SSH đã được gỡ theo yêu cầu tiếp theo. Ứng dụng hiện chỉ chạy Docker local qua Airflow tại <http://localhost:8001/>; các đường dẫn thêm máy bên dưới không còn là chức năng hiện hành. Lịch sử run vẫn giữ nguyên. Xem [hướng dẫn hiện tại](../training/README.md).

## Thiết kế

Mục tiêu: ứng dụng training độc lập ở cổng 8001, sensor giữ cổng 8000. Giữ lịch sử/cấu hình đã có. Training Control có tab Huấn luyện và Máy train; sensor chỉ liên kết sang ứng dụng này. Airflow hiện hữu vẫn điều phối cả chế độ local và SSH.

Máy SSH là Linux có Docker, user có quyền chạy Docker. Thêm tên, host/port, username, password hoặc private key và địa chỉ mạng training. Trước khi gửi thông tin xác thực, đọc và hiển thị fingerprint host key; người dùng xác nhận khóa máy. Khóa/mật khẩu lưu mã hóa bằng Fernet; khóa mã hóa nằm trong volume riêng, không trả secret về browser. Khi host key đổi, kết nối bị từ chối.

Mỗi máy chạy container từ cùng image PyTorch 2.6/CUDA 12.4 được ghim digest. Có thao tác tải image riêng khi thiếu. Dataset NPZ và train.py được chép qua SFTP rồi Docker cp; kiểm tra SHA256 trước start. Docker daemon trên mỗi máy giữ workload độc lập với phiên SSH. Tên container, label cấu hình và manifest ổn định để retry tìm lại. Dùng network host trên Linux, torchrun static rendezvous qua địa chỉ máy đầu tiên và port cấu hình; các máy phải kết nối trực tiếp được qua LAN/VPN, bao gồm lưu lượng Gloo/NCCL.

Chọn máy và số process trên mỗi máy. Tổng rank = số máy × process/máy; tối đa 32 rank. Rank toàn cục được hiển thị theo từng máy. Lưu snapshot node/credential version cho run, chặn xóa máy đang có workload. Hủy hoặc lỗi một node thì dừng toàn cụm; chỉ giải phóng lượt chạy khi xác nhận mọi workload đã dừng. Chưa có cơ chế co giãn hay tự cài Docker/driver.

## Kế hoạch thực hiện (inline)

- [x] Di chuyển control API/UI sang v2/training, thêm Docker service và giữ dữ liệu cũ.
- [x] Node registry: validate, mã hóa secret, host key trust, kiểm tra SSH/Docker/GPU và tải image.
- [x] Bổ sung config/run snapshots cho cụm SSH, giữ tương thích run local cũ.
- [x] Runner SSH multi-node: prepare/attach/monitor/cancel/artifact, dùng workload DDP hiện hữu.
- [x] UI độc lập: tab máy, form SSH, fingerprint, trạng thái tài nguyên, chọn máy và topology theo node/rank.
- [x] Test bí mật/host key/idempotency/failure/cancel; kiểm chứng hai SSH endpoint cục bộ và DDP TCP thật. Phân biệt rõ container kiểm thử và máy vật lý.
- [x] Build, khởi chạy, kiểm tra UI desktop/mobile, cập nhật tài liệu và giới hạn thực tế.

## Nguồn kỹ thuật

- [PyTorch torchrun](https://docs.pytorch.org/docs/stable/elastic/run.html): node/rank, rendezvous và kết nối giữa worker.
- [Paramiko SSHClient](https://docs.paramiko.org/en/stable/api/client.html): RejectPolicy, timeouts và xác thực host key.

Không có thông tin truy cập máy vật lý từ người dùng; không được đánh dấu kiểm chứng nhiều máy vật lý khi chỉ thử bằng container.

## Kết quả

- Giữ nguyên bốn run cũ khi chuyển SQLite sang volume `v2_training-state`.
- Run 5: thêm và xác minh hai endpoint từ giao diện, chọn cụm SSH, train Gloo hai rank thành công 6 epoch; checkpoint đã nạp lại, đủ 33 đặc trưng, 4.407 train / 2.817 validation.
- Run 6: yêu cầu hủy khi cụm đang chạy, xác nhận cả hai workload dừng rồi ghi cancelled.
- Run 7: dừng một endpoint SSH, khôi phục kết nối; runner tìm lại workload, ghi failed và xác nhận hai container đều dừng.
- SSH từ chối host key sai trước xác thực; argument chứa ký tự shell được giữ nguyên. 39 test training và 76 test sensor đạt, gồm trường hợp Docker đã nhận create nhưng mất phản hồi. Run 8 local CUDA một rank thành công 3 epoch sau khi tách.
- Các endpoint test dùng chung Docker daemon của máy phát triển, không chứng minh nhiều máy GPU vật lý. Đã gỡ node test khỏi registry và dừng SSH server test; lịch sử snapshot của run vẫn giữ để xem topology. Lịch vận hành được trả về local CPU hai rank, 02:00 hằng ngày.

Ứng dụng: <http://localhost:8001/>. Lượt SSH thành công: <http://localhost:8001/?run=5>. Thêm máy thật: <http://localhost:8001/?page=machines>.
