#!/usr/bin/env bash
# Create the stress-test schema: 2 databases x TABLES tables each, seeded
# with SEED_ROWS rows of ts_ms=0 (snapshot-phase data, excluded from the
# latency statistics by the probe).
set -euo pipefail

MYSQL_CONTAINER="${MYSQL_CONTAINER:-seatunnel-rs-mysql-1}"
DBS="${DBS:-perf_a perf_b}"
TABLES="${TABLES:-250}"
SEED_ROWS="${SEED_ROWS:-100}"

for db in $DBS; do
  echo "creating database $db with $TABLES tables..."
  docker exec -i "$MYSQL_CONTAINER" mysql -uroot -proot -e "DROP DATABASE IF EXISTS \`$db\`; CREATE DATABASE \`$db\`;"
  for ((i = 0; i < TABLES; i++)); do
    printf 'CREATE TABLE `%s`.`t%03d` (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, ts_ms BIGINT NOT NULL DEFAULT 0, seq BIGINT NOT NULL DEFAULT 0, payload VARCHAR(96) NOT NULL DEFAULT %s, PRIMARY KEY (id)) ENGINE=InnoDB;\n' "$db" "$i" "''"
  done | docker exec -i "$MYSQL_CONTAINER" mysql -uroot -proot "$db"
  # Seed rows for the snapshot phase (ts_ms = 0 marks them as snapshot data).
  for ((i = 0; i < TABLES; i++)); do
    printf 'INSERT INTO `t%03d` (ts_ms, seq, payload) VALUES ' "$i"
    for ((r = 0; r < SEED_ROWS; r++)); do
      if ((r > 0)); then printf ','; fi
      printf "(0, %d, 'seed-%03d-%03d')" "$((i * SEED_ROWS + r))" "$i" "$r"
    done
    printf ';\n'
  done | docker exec -i "$MYSQL_CONTAINER" mysql -uroot -proot "$db"
done

docker exec "$MYSQL_CONTAINER" mysql -uroot -proot -e "SELECT table_schema, COUNT(*) AS tables_cnt FROM information_schema.tables WHERE table_schema IN ('perf_a','perf_b') GROUP BY table_schema;"
echo "mysql stress schema ready."
