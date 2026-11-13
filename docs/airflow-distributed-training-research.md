# Airflow lên lịch huấn luyện mô hình phân tán

Ngày nghiên cứu: 2026-11-13. Phạm vi: nghiên cứu kiến trúc, chưa triển khai hoặc kiểm chứng trên cụm GPU.

Tài liệu tiếp theo: [triển khai ba phương án: một máy nhiều GPU, Kubernetes và Slurm](airflow-distributed-training-deployment.md).

## Bối cảnh và kết luận

Dự án đích là PDM trong `project_claude`: Airflow 3.3.2, Spark 4.1.3, C-MAPSS và hai model Spark MLlib GBT dự báo RUL/sắp hỏng. Phương án một máy được triển khai bằng một ứng viên PyTorch DDP dùng cùng 33 đặc trưng và phân nhóm train/validation; không thể nạp checkpoint PyTorch bằng Spark PipelineModel. Máy phát triển có một GTX 1650 Max-Q 4 GB. Xem [hướng dẫn chạy thực tế](../training/README.md); phần còn lại của tài liệu giải thích kiến trúc tổng quát cho cả ba backend.

**Đề xuất kiến trúc có điều kiện:** Airflow quản lý thời điểm chạy và chuỗi công việc; hạ tầng tính toán quản lý tài nguyên; runtime huấn luyện quản lý các tiến trình cùng train một mô hình. Đây là tổng hợp thiết kế từ khả năng của [Airflow Kubernetes operators](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html), [torchrun](https://docs.pytorch.org/docs/stable/elastic/run.html) và [Kubeflow Trainer](https://www.kubeflow.org/docs/components/trainer/overview/), không phải cấu hình đã được thử nghiệm trong repo.

```text
Airflow schedule
  → chốt phiên bản dữ liệu / cấu hình
  → submit hoặc tìm lại training job
  → đợi kết quả của toàn job
  → đánh giá mô hình
  → công bố artifact khi đạt điều kiện

Training backend
  → cấp phát đủ GPU / node
  → khởi động launcher và các rank
  → đọc dữ liệu, đồng bộ gradient, checkpoint
  → trả trạng thái và URI artifact
```

## Các khái niệm cần phân biệt

| Thành phần | Trách nhiệm |
|---|---|
| Airflow task | Một bước workflow; ví dụ submit và theo dõi một lần train |
| Pod hoặc training node | Môi trường chứa launcher và các tiến trình train |
| Rank | Một tiến trình tham gia process group của distributed training |
| DDP | Đồng bộ gradient giữa các bản sao mô hình |
| torchrun | Sinh tiến trình trên từng node, thiết lập rendezvous và biến môi trường |

Ví dụ thiết kế: 2 training node, mỗi node 4 GPU, dùng một tiến trình/GPU sẽ có 8 rank nhưng có thể chỉ có một Airflow task quản lý toàn job. `torchrun` có thể chạy một node hoặc nhiều node; mỗi tiến trình GPU dùng một GPU. Các node cần cùng rendezvous ID/endpoint cho cùng job; các job khác nhau cần được tách rendezvous để tránh xung đột. [Nguồn: torchrun](https://docs.pytorch.org/docs/stable/elastic/run.html).

DDP không tự chia input; ứng dụng cần chia dữ liệu, chẳng hạn bằng `DistributedSampler`. Tạo nhiều Airflow task chạy script train thông thường không tự biến chúng thành một lần train phân tán. Cách dùng task mapping phù hợp hơn cho nhiều trial hoặc mô hình độc lập; đây là suy luận thiết kế từ cơ chế đồng bộ process group của DDP. [Nguồn: PyTorch DDP](https://docs.pytorch.org/docs/2.15/generated/torch.nn.parallel.DistributedDataParallel.html).

DDP thông thường vẫn giữ các bản sao tham số và optimizer state trên từng rank. Thêm GPU theo DDP không tự giải quyết trường hợp model không vừa bộ nhớ một GPU. Khi nút thắt là bộ nhớ mô hình, cần đánh giá sharding như FSDP hoặc cơ chế tương ứng; `FULL_SHARD` chia tham số, gradient và optimizer state. Chọn cách nào còn phụ thuộc model và workload. [Nguồn: PyTorch FSDP](https://docs.pytorch.org/docs/stable/fsdp.html).

## Chọn backend theo hệ thống đang có

Các lựa chọn dưới đây là đề xuất thiết kế; chưa đủ dữ liệu để chốt công nghệ hoặc khẳng định hiệu năng.

| Điều kiện | Hướng bắt đầu |
|---|---|
| Một máy nhiều GPU | Một training pod/process environment chạy `torchrun`; tránh thêm hệ quản lý multi-node nếu chưa cần |
| Đã có Kubernetes, một node đủ | `KubernetesPodOperator` hoặc `KubernetesJobOperator`, container yêu cầu đủ GPU |
| Đã có Kubernetes, train nhiều node thường xuyên | Đánh giá Kubeflow Trainer/JobSet và hệ quản lý quota, admission phù hợp |
| Đã có Slurm hoặc dịch vụ training managed | Airflow submit/theo dõi backend hiện có; chưa có cơ sở để dựng thêm Kubernetes |

`KubernetesPodOperator` tạo một Pod và không yêu cầu Airflow dùng KubernetesExecutor. `KubernetesJobOperator` quản lý Kubernetes Job; `parallelism` là số pod tối đa, không tự cấp DDP rank, rendezvous hay đồng bộ gradient. Với provider đang được tài liệu `stable` hiển thị là **11.0.0**, `wait_until_job_complete=False` là mặc định; muốn task theo dõi thành công/thất bại của Job phải bật chờ. Chế độ `deferrable=True` của operator này yêu cầu `wait_until_job_complete=True`. Cần đối chiếu lại phiên bản được cài trước khi viết DAG. [Nguồn: Pod operator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html), [Job operator API](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/_api/airflow/providers/cncf/kubernetes/operators/job/index.html).

Kubeflow Trainer hiện cung cấp `TrainJob` và các runtime dùng lại cho distributed training; không nên lấy ví dụ `PyTorchJob` của Training Operator v1 rồi mặc định đó là API của Trainer hiện hành. Airflow cần submit/theo dõi đúng loại tài nguyên được cụm cài đặt; `KubernetesJobOperator` không phải operator tổng quát cho mọi CRD. [Nguồn: Trainer overview](https://www.kubeflow.org/docs/components/trainer/overview/), [Runtime guide](https://trainer.kubeflow.org/en/latest/operator-guides/runtime.html), [Job operator API](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/_api/airflow/providers/cncf/kubernetes/operators/job/index.html).

## GPU và hiện tượng đứng chờ khi đến lịch

Với đường cấp GPU qua device plugin, node cần driver và plugin phù hợp. Pod yêu cầu GPU bằng tài nguyên như `nvidia.com/gpu`; Kubernetes cho phép chỉ khai báo GPU limit để dùng làm request, hoặc khai báo request/limit bằng nhau. Cần thêm lựa chọn node khi workload yêu cầu loại GPU cụ thể. [Nguồn: Kubernetes GPU scheduling](https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/).

Trong train đồng bộ, một phần pod chạy còn phần khác chưa được cấp GPU có thể khiến rank đã chạy giữ GPU nhưng không tiến triển. Kueue xét quota cho toàn workload; tuy nhiên quota tổng không đồng nghĩa đủ chỗ trên các node. Tài liệu Kueue phân biệt admission theo quota, topology-aware placement và cơ chế readiness. [Nguồn: Kueue all-or-nothing](https://kueue.sigs.k8s.io/docs/concepts/all_or_nothing/).

Nếu dùng Kueue, đánh giá `waitForPodsReady`: pod không sẵn sàng đủ trong thời hạn sẽ làm workload bị thu hồi admission và đưa lại vào hàng đợi. Đây là cơ chế timeout/eviction, không phải bảo đảm mọi pod bắt đầu cùng một thời điểm. Cấu hình phải phù hợp thời gian tải image, cấp node và khởi động training. [Nguồn: Kueue readiness](https://kueue.sigs.k8s.io/docs/tasks/manage/setup_wait_for_pods_ready/).

## Khi Airflow đến lịch: cửa sổ dữ liệu và giới hạn chạy

Giờ kích hoạt và khoảng dữ liệu train cần được định nghĩa riêng. Tài liệu Airflow stable đang hiển thị 3.4.0: cron thuần mặc định dùng trigger timetable, có thể cho `data_interval_start == data_interval_end`. Nếu cần khoảng dữ liệu liên tục, chọn rõ `CronDataIntervalTimetable`; không mặc định mọi `@daily` luôn đại diện ngày hôm trước. Đối chiếu phiên bản/cấu hình thực tế trước khi chuyển DAG. [Nguồn: Airflow timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html).

Đề xuất cho retrain định kỳ: chốt một manifest dữ liệu bất biến cho run, gồm khoảng thời gian, phiên bản dữ liệu, image/code và cấu hình. Ví dụ thiết kế: chạy lúc 02:00 nhưng dữ liệu kết thúc ở 00:00, sau khi nguồn xác nhận đã ghi đủ. Retry giữ nguyên manifest; chạy lại cùng dữ liệu không đồng nghĩa kết quả số học luôn giống tuyệt đối. Đây là lựa chọn thiết kế, không phải Airflow tự bảo đảm dữ liệu đã sẵn sàng.

| Điều khiển | Đề xuất và giới hạn |
|---|---|
| `catchup=False` | Khi không muốn tự tạo các run lịch bị bỏ qua; vẫn cần xử lý run đã tồn tại và trigger thủ công |
| `max_active_runs=1` | Khi muốn tuần tự các run của một DAG; không phải khóa toàn hệ thống cho cùng một model |
| Airflow pool / `pool_slots` | Hạn chế số task/job từ các DAG dùng chung pool; không trực tiếp đặt chỗ GPU |
| Admission ở backend | Quyết định job có đủ GPU/node để chạy; xử lý cả workload ngoài Airflow |

Hai điều khiển đầu dựa trên [Airflow timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html) và [giới hạn DAG run](https://airflow.apache.org/docs/apache-airflow/stable/faq.html). Pool là cơ chế đếm slot; nếu dùng pool để đại diện ngân sách GPU, phải quy định cách quy đổi `pool_slots` và xét các job ngoài Airflow. [Nguồn: Airflow pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html).

Job train dài nên được theo dõi bằng operator/sensor hỗ trợ deferral và có triggerer. Deferral giải phóng Airflow worker, còn job GPU tiếp tục chạy. Mặc định task deferred không chiếm pool slot: nếu pool đang giới hạn các job còn chạy bên ngoài, bật tùy chọn tính cả deferred task. [Nguồn: Airflow deferring](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html), [Airflow pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html).

Nếu train lâu hơn chu kỳ lịch, phải chọn nghiệp vụ: xếp hàng mọi phiên bản dữ liệu, chỉ train phiên bản mới nhất, hoặc hủy/thay thế run cũ. Các giới hạn đồng thời không tự thực hiện chính sách thay thế đó. Đặt riêng hạn chờ tài nguyên, hạn rendezvous/khởi động và hạn train; kiểm chứng cách operator hủy external job khi timeout.

## Retry, checkpoint và tránh tạo hai lần train

Airflow có thể chạy lại task nên thao tác phải lặp an toàn; tài liệu khuyên dữ liệu lớn ở remote storage và chỉ truyền thông tin nhỏ/URI qua XCom. [Nguồn: Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html).

Đề xuất giao thức cho task quản lý training job:

1. Tạo khóa ổn định từ workflow run, task và trial; không đưa `try_number` vào khóa nhận diện của cùng một job.
2. Trước khi tạo, tra job theo khóa: còn chạy thì theo dõi tiếp; đã thành công thì kiểm tra artifact; thất bại thì áp dụng chính sách restart được định nghĩa trước.
3. Khi API submit timeout, tra lại trước khi gửi lệnh tạo mới; timeout không chứng minh job chưa được tạo.
4. Tách thư mục output theo run; chỉ công bố manifest/artifact hoàn chỉnh. Không cho mọi lần chạy ghi đè chung một `latest`.
5. Xác định rõ khi Airflow task bị hủy thì training tiếp tục hay bị hủy; cần dọn toàn job để không giữ GPU mồ côi.

Các bước trên là suy luận thiết kế từ yêu cầu retry an toàn, không phải bảo đảm exactly-once có sẵn của Airflow. Khả năng reattach/durable của operator phụ thuộc phiên bản và loại tài nguyên; phải kiểm tra hành vi đang dùng. [Nguồn nền: Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html), [Pod durable execution](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#durable-execution).

`torchrun` có thể restart nhóm tiến trình khi có lỗi; script vẫn cần tự lưu và tải checkpoint. Tiến độ sau checkpoint gần nhất có thể mất. Airflow retry toàn task không tự phục hồi model/optimizer. [Nguồn: PyTorch elastic train script](https://docs.pytorch.org/docs/main/distributed/elastic/train_script.html).

Đề xuất chỉ định rõ tầng chịu retry: runtime xử lý lỗi tiến trình ngắn hạn; training controller xử lý pod/node theo khả năng backend; Airflow xử lý việc submit/theo dõi hoặc retry job đã thất bại. Cần đặt tổng giới hạn số lần và thời gian để tránh các tầng nhân số lần chạy. Checkpoint nên được kiểm tra cùng phiên bản dữ liệu, code và cấu hình trước khi resume; đây là yêu cầu thiết kế cần xác nhận trên framework thực tế.

## Những phép thử cần có trước khi đưa vào lịch thật

Đây là danh sách kiểm chứng được đề xuất, chưa chạy:

- Một job nhỏ đúng đủ rank, dữ liệu không bị mỗi rank đọc lại toàn bộ ngoài chủ đích.
- Đến lịch khi thiếu GPU: job ở hàng đợi có giới hạn chờ và giải phóng tài nguyên khi hủy.
- Dừng một rank/node: toàn job phục hồi hoặc thất bại rõ ràng, không đứng vô hạn.
- Dừng Airflow worker sau submit: lần retry tìm lại đúng job, không sinh job thứ hai.
- Lỗi sau khi ghi checkpoint và sau khi upload artifact: retry không công bố kết quả dở dang.
- Hai lịch/trial trùng thời gian: tách rendezvous, output và khóa nhận diện.
- Task deferred vẫn giữ đúng ngân sách pool; kiểm tra cả một DAG khác dùng chung GPU.
- Chạy lịch sau một khoảng tạm dừng và chạy thủ công: manifest lấy đúng khoảng dữ liệu theo chính sách đã chọn.
- So sánh thời gian toàn job với một GPU/một node trên dữ liệu đại diện; nhiều GPU chỉ hữu ích nếu phần tính toán bù được chi phí giao tiếp và I/O.

Thông tin cần có để chuyển từ nghiên cứu sang thiết kế cụ thể: mục tiêu tăng tốc hay xử lý model quá lớn; framework/model; phiên bản Airflow/provider; hạ tầng và số/loại GPU; thời lượng train so với chu kỳ lịch; dữ liệu/checkpoint đang lưu ở đâu; có lỗi thực tế và log rank hay chưa.
