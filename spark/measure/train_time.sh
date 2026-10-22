#!/usr/bin/env bash
# Đo thời gian train.py (học cả nhóm gốc) trên cụm: lấy hết core đang có, chạy N lần.
# Dùng (trong v2/, Git Bash): bash spark/measure/train_time.sh <nhãn> [số_lần=3]
# Job luồng phải tắt trước (docker compose stop spark) để job huấn luyện lấy được mọi core.
# Các phiên bản đo ghi vào models/vtime-*; xóa tay sau khi đo. Không đổi con trỏ Mô hình đang dùng.
set -euo pipefail
export MSYS_NO_PATHCONV=1
LABEL=$1; RUNS=${2:-3}
workers=$(curl -s localhost:8080/json/ | py -3.12 -c "import json,sys; d=json.load(sys.stdin); print(sum(w['state']=='ALIVE' for w in d['workers']), sum(w['cores'] for w in d['workers'] if w['state']=='ALIVE'))")
echo "nhãn,lần,worker,core,giây"
for run in $(seq 1 "$RUNS"); do
  t0=$(date +%s)
  # spark_session() lấy master từ SPARK_MASTER và đè lên --master: phải đặt, không thì job chạy cục bộ.
  docker compose run --rm --no-deps -e SPARK_MASTER=spark://spark-master:7077 init spark-submit --master spark://spark-master:7077 --driver-memory 2g \
    --conf spark.executor.cores=1 src/jobs/train.py 1.0 --version "vtime-$LABEL-$run" >/dev/null 2>&1
  echo "$LABEL,$run,${workers// /,},$(( $(date +%s) - t0 ))"
done
