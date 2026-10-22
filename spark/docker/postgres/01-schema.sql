-- Database pdm (tạo sẵn bởi POSTGRES_DB). Mỗi dòng là một lần chấm điểm một Máy; ghi ít nhất một lần nên dòng trùng được phép.
CREATE TABLE predictions (
    machine_id       text             NOT NULL,
    window_end       timestamptz      NOT NULL,  -- event_time lớn nhất trong cửa sổ
    window_end_cycle integer          NOT NULL,  -- tuổi Máy ở cuối cửa sổ
    rul_pred         double precision NOT NULL,
    failing_prob     double precision NOT NULL,
    model_version    text             NOT NULL,
    written_at       timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX ON predictions (window_end);
CREATE INDEX ON predictions (machine_id, window_end);

-- Mỗi Máy tối đa một Lệnh bảo trì (ghi kèm ON CONFLICT DO NOTHING).
CREATE TABLE maintenance_orders (
    machine_id      text PRIMARY KEY,
    created_at      timestamptz      NOT NULL DEFAULT now(),
    cycle           integer          NOT NULL,
    rul_pred        double precision NOT NULL,
    failing_prob    double precision NOT NULL,
    suggested_cycle integer          NOT NULL,  -- tính bằng Chu kỳ vì số Chu kỳ mỗi giây đổi được
    model_version   text             NOT NULL
);

CREATE TABLE failures (
    machine_id text PRIMARY KEY,
    failed_at  timestamptz NOT NULL,
    last_cycle integer     NOT NULL,
    method     text,       -- phương pháp sinh của bộ giả lập, để so sai số theo phương pháp
    base       text[]
);

CREATE TABLE model_validation (
    validated_at  timestamptz      NOT NULL DEFAULT now(),
    candidate     text             NOT NULL,
    candidate_rmse double precision NOT NULL,
    candidate_f1  double precision NOT NULL,
    current       text             NOT NULL,
    current_rmse  double precision NOT NULL,
    current_f1    double precision NOT NULL,
    accepted      boolean          NOT NULL
);

-- Dữ liệu nội bộ của Airflow (lát 3).
CREATE DATABASE airflow;
