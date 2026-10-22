#!/usr/bin/env bash
# Đo thông lượng và độ trễ của đường nóng: chạy N lần, mỗi lần khởi động 2 phút rồi đo 10 phút.
# Dùng (trong v2/, Git Bash): bash spark/measure/throughput.sh [số_lần=3] [máy=500] [k=10]
# In mỗi lần một dòng: Bản tin/giây Kafka nhận (theo offset), dự đoán/giây, độ trễ p50/p95, RAM tổng các container.
set -euo pipefail
export MSYS_NO_PATHCONV=1
RUNS=${1:-3}; N=${2:-500}; K=${3:-10}; WARMUP=${WARMUP:-120}; WINDOW=${WINDOW:-600}

kafka_total() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sensor.raw \
    | awk -F: '{s += $3} END {print s}'
}
sql() { docker compose exec -T postgres psql -U pdm -d pdm -tAc "$1"; }

curl -s -X PATCH localhost:8000/api/config -H 'Content-Type: application/json' \
  -d "{\"machines\": $N, \"speed\": $K, \"faults\": {\"bad_value\": 0, \"drop\": 0, \"late\": 0, \"duplicate\": 0}}" >/dev/null
echo "lần,bắt_đầu,kafka/s,dự_đoán/s,trễ_p50_s,trễ_p95_s,ram_GB"
for run in $(seq 1 "$RUNS"); do
  curl -s -X POST localhost:8000/api/start >/dev/null
  sleep "$WARMUP"
  t0=$(date -u +%s); k0=$(kafka_total); start=$(date -u +%Y-%m-%dT%H:%M:%S)
  sleep "$WINDOW"
  k1=$(kafka_total); t1=$(date -u +%s); end=$(date -u +%Y-%m-%dT%H:%M:%S)
  rate=$(( (k1 - k0) / (t1 - t0) ))
  row=$(sql "select round(count(*)::numeric / $WINDOW, 1) || ',' ||
                    round(percentile_cont(0.5) within group (order by extract(epoch from written_at - window_end))::numeric, 1) || ',' ||
                    round(percentile_cont(0.95) within group (order by extract(epoch from written_at - window_end))::numeric, 1)
             from predictions where window_end between '$start' and '$end'")
  ram=$(docker stats --no-stream --format '{{.MemUsage}}' | awk '{v=$1; if (v ~ /GiB/) {sub(/GiB/,"",v); s+=v} else if (v ~ /MiB/) {sub(/MiB/,"",v); s+=v/1024}} END {printf "%.1f", s}')
  echo "$run,$start,$rate,$row,$ram"
  curl -s -X POST localhost:8000/api/stop >/dev/null
  sleep 60
done
