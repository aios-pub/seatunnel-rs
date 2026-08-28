#!/usr/bin/env bash
# Collect benchmark metrics: engine Prometheus gauges (via seatunnel-web)
# plus Kafka end offsets as an independent delivery-rate verification.
#
# usage: collect_metrics.sh <out-file> <interval-secs> <count> [web-base]
set -u

OUT=$1; INTERVAL=$2; COUNT=$3; WEB=${4:-http://127.0.0.1:8080}

KAFKA_CONTAINER=seatunnel-rs-kafka-1
TOPICS="ai_learn_ok_class_topic resource_binlog question_html_update_binlog \
sub_catelog_binlog sub_knowledge_binlog user_binlog product_binlog \
teacher_ksystem_recommand_binlog kefu_binlog canal_sync_route_unmatched"

: > "$OUT"
for i in $(seq 1 "$COUNT"); do
  {
    echo "===== sample $i $(date +%s) ====="
    curl -s --max-time 4 "$WEB/metrics" | grep -E \
      'seatunnel_task_(records_per_second|processed_records|idle_seconds)|seatunnel_task_sink_(sent|delivered|failed|in_flight|delivery_latency)' \
      || echo "(web metrics unavailable)"
    for t in $TOPICS; do
      offsets=$(docker exec "$KAFKA_CONTAINER" kafka-get-offsets \
        --bootstrap-server localhost:9092 --topic "$t" 2>/dev/null \
        | awk -F: '{s+=$3} END {print s+0}')
      echo "kafka_end_offsets{topic=\"$t\"} $offsets"
    done
  } >> "$OUT" 2>>"$OUT.stderr"
  [ "$i" -lt "$COUNT" ] && sleep "$INTERVAL"
done
echo "collected $COUNT samples -> $OUT"
