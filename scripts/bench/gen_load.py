#!/usr/bin/env python3
"""MySQL write-load generator for the canal-sync benchmark.

Drives the local docker MySQL through a single persistent
`docker exec -i ... mysql` pipe (stdlib only, no client drivers) with a
mixed INSERT/UPDATE/DELETE workload mirroring real canal-sync traffic:

  - 70% INSERT  multi-row batches with EXPLICIT ids (base 10_000_000)
               so UPDATE/DELETE never need to read ids back;
  - 25% UPDATE  multi-row `WHERE id IN (...)` — one row event per matched
               row, exercising the canal update pairing (oldData);
  -  5% DELETE  multi-row deletes of ids inserted earlier this run.

Every write lands in the binlog (ROW format) and must flow through the
engine (canal encode -> topic routing -> kafka fan-out) to the bench
topics. Rates are throttled per half-second tick; achieved rates are
logged so the report can compare source rate vs delivery rate.

Usage:
  python3 gen_load.py --schema bench_schema.json \
      --levels 500:60,2000:60,5000:60,10000:60 [--log-out samples.csv]
"""

import argparse
import csv
import json
import random
import subprocess
import sys
import time
from pathlib import Path

MYSQL = ["docker", "exec", "-i", "seatunnel-rs-mysql-1", "mysql", "-uroot", "-proot"]
# Explicit insert ids start at now-in-ms × 1000: far below BIGINT UNSIGNED
# overflow, and the 1000-ids-per-millisecond headroom keeps re-runs (and
# runs faster than one row per microsecond) from ever colliding with rows
# left by an earlier run.
ID_BASE = int(time.time() * 1000) * 1000
INSERT_ROWS = 250
UPDATE_IDS = 100
DELETE_IDS = 50

# Hot tables get most of the traffic (mirrors production skew); every
# pipeline still receives a share so all 5 run concurrently.
HOT = {
    "neworiental_v3.entity_user": 3,
    "neworiental_v3.entity_question": 3,
    "neworiental_v3.link_respackage_resource": 2,
    "neworiental_v3.entity_school_ksystem": 2,
    "neworiental_v3.entity_public_school": 1,
    "neworiental_user.entity_user": 3,
    "neworiental_product.entity_sku": 2,
    "ailearn_okminicourse.ailearn_minicourse_v2": 2,
    "neworiental_data_recommand.entity_question_lib_0": 2,
    "ailearn_kefu.repository_content": 1,
}


def value_for(kind: str, rng: random.Random, table: str, row_id: int, col: str) -> str:
    if kind == "str":
        s = f"{table[:10]}-{col}-{row_id}-{rng.randrange(1 << 30):x}"
        return f"'{s[:70]}'"
    if kind == "int":
        return str(rng.randrange(1, 2**40))
    if kind == "dt":
        return "'2026-08-28 12:00:00'"
    if kind == "dec":
        return f"{rng.randrange(1, 100000) / 100:.2f}"
    if kind == "text":
        payload = f"payload-{table}-{row_id}-" + "x" * rng.randrange(120, 260)
        return f"'{payload}'"
    return "NULL"


def qualify(table: str) -> str:
    """`db.table` key -> backtick-quoted `db`.`table` (a single pair of
    backticks around the dotted name would be ONE identifier, not a
    qualified one)."""
    db, _, tbl = table.partition(".")
    return f"`{db}`.`{tbl}`"


class LoadGen:
    def __init__(self, schema: dict, out: csv.writer):
        self.tables = list(schema.keys())
        weights = [HOT.get(t, 1) for t in self.tables]
        self.picker = random.Random(42)
        self.table_pool = self.tables
        self.table_weights = weights
        self.layout = schema
        self.out = out
        self.rng = random.Random(7)
        self.next_id = ID_BASE
        # Per-table ranges of ids inserted this run: list of (lo, hi).
        self.ranges: dict[str, list[tuple[int, int]]] = {t: [] for t in self.tables}
        self.inserted = 0
        self.updated = 0
        self.deleted = 0

    def pick_table(self) -> str:
        return self.picker.choices(self.table_pool, self.table_weights, k=1)[0]

    def sql_insert(self, table: str, n: int) -> int:
        cols = self.layout[table]
        names = ", ".join(f"`{c['name']}`" for c in cols)
        rows = []
        lo = self.next_id
        for _ in range(n):
            row_id = self.next_id
            self.next_id += 1
            vals = ", ".join(
                value_for(c["kind"], self.rng, table, row_id, c["name"]) for c in cols
            )
            rows.append(f"({row_id}, {vals})")
        hi = self.next_id - 1
        if hi >= lo:
            self.ranges[table].append((lo, hi))
        # Keep only the last 40 spans per table (bounded memory).
        del self.ranges[table][:-40]
        stmt = (
            f"INSERT INTO {qualify(table)} (`id`, {names}) VALUES\n  "
            + ",\n  ".join(rows)
            + ";"
        )
        self.inserted += n
        return stmt

    def sql_update(self, table: str) -> int | None:
        ids = self._sample_ids(table, UPDATE_IDS)
        if not ids:
            return None
        cols = [c for c in self.layout[table] if c["kind"] in ("str", "int")][:4]
        if not cols:  # defensive: never emit `SET  WHERE`
            cols = self.layout[table][:2]
        sets = ", ".join(
            f"`{c['name']}` = {value_for(c['kind'], self.rng, table, 0, c['name'])}"
            for c in cols
        )
        self.updated += len(ids)
        return (
            f"UPDATE {qualify(table)} SET {sets} WHERE `id` IN ({', '.join(map(str, ids))});"
        )

    def sql_delete(self, table: str) -> int | None:
        ids = self._sample_ids(table, DELETE_IDS)
        if not ids:
            return None
        self.deleted += len(ids)
        return f"DELETE FROM {qualify(table)} WHERE `id` IN ({', '.join(map(str, ids))});"

    def _sample_ids(self, table: str, n: int) -> list[int]:
        spans = self.ranges[table]
        if not spans:
            return []
        ids = []
        for _ in range(n):
            lo, hi = self.rng.choice(spans)
            ids.append(self.rng.randrange(lo, hi + 1))
        return ids


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="bench_schema.json")
    ap.add_argument("--levels", default="500:60,2000:60,5000:60,10000:60",
                    help="rows_per_sec:seconds phases")
    ap.add_argument("--log-out", default="load_samples.csv")
    args = ap.parse_args()

    schema = json.loads(Path(args.schema).read_text())
    log_path = Path(args.log_out)
    new_log = not log_path.exists()
    log_f = open(log_path, "a", newline="")
    out = csv.writer(log_f)
    if new_log:
        out.writerow(["ts", "phase_target", "src_rows_rate", "inserted", "updated", "deleted"])

    err_log = open(log_path.with_suffix(".mysql.err"), "a")
    proc = subprocess.Popen(MYSQL, stdin=subprocess.PIPE,
                            stdout=subprocess.DEVNULL, stderr=err_log,
                            text=True, bufsize=1 << 20)

    gen = LoadGen(schema, out)
    start = time.time()
    phase_rows = 0
    phase_start = start
    phase_target = 0

    def flush_stmt(stmt: str) -> None:
        proc.stdin.write(stmt + "\n")
        proc.stdin.flush()

    def close_phase() -> None:
        nonlocal phase_rows, phase_start
        elapsed = time.time() - phase_start
        if elapsed > 0 and phase_rows > 0:
            rate = phase_rows / elapsed
            print(f"phase target={phase_target}/s: achieved {rate:.0f} rows/s "
                  f"(ins={gen.inserted} upd={gen.updated} del={gen.deleted})",
                  flush=True)
            out.writerow([int(time.time()), phase_target, f"{rate:.1f}",
                          gen.inserted, gen.updated, gen.deleted])
            log_f.flush()
        phase_rows = 0
        phase_start = time.time()

    for spec in args.levels.split(","):
        rate_s, secs = spec.split(":")
        phase_target, phase_secs = int(rate_s), float(secs)
        print(f"=== phase: {phase_target} rows/s for {phase_secs:.0f}s", flush=True)
        phase_start = time.time()
        tick_start = phase_start
        tick_rows = 0
        while True:
            now = time.time()
            if now - phase_start >= phase_secs:
                break
            # Budget for a 0.5s tick.
            budget = phase_target * 0.5
            while tick_rows < budget:
                stmts: list[str] = []
                remaining = budget - tick_rows
                r = gen.rng.random()
                if r < 0.70 or sum(len(s) for s in gen.ranges.values()) == 0:
                    # Adaptive batch size keeps low-rate phases from
                    # overshooting (a fixed 250-row batch floors the
                    # achievable rate at 500 rows/s).
                    n = max(10, min(INSERT_ROWS, int(remaining)))
                    stmts.append(gen.sql_insert(gen.pick_table(), n))
                    tick_rows += n
                elif r < 0.95:
                    s = gen.sql_update(gen.pick_table())
                    if s:
                        stmts.append(s)
                        tick_rows += UPDATE_IDS
                else:
                    s = gen.sql_delete(gen.pick_table())
                    if s:
                        stmts.append(s)
                        tick_rows += DELETE_IDS
                for s in stmts:
                    flush_stmt(s)
            # Throttle to the tick boundary.
            target_tick_end = tick_start + 0.5
            sleep_for = target_tick_end - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            tick_start = target_tick_end
            phase_rows += tick_rows
            tick_rows = 0
        close_phase()

    proc.stdin.close()
    rc = proc.wait(timeout=30)
    err_log.close()
    total = time.time() - start
    print(f"done in {total:.0f}s: inserted={gen.inserted} updated={gen.updated} "
          f"deleted={gen.deleted} (mysql rc={rc})", flush=True)
    return 0 if rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
