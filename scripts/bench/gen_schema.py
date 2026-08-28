#!/usr/bin/env python3
"""Generate the benchmark schema for the canal-sync consolidated job.

Emits `init_bench_schema.sql` (CREATE DATABASE/TABLE, mirroring the
table list of jobs/configs/canal-sync-consolidated.yaml 1:1) plus
`bench_schema.json` (column layout per table, consumed by gen_load.py
to build INSERT/UPDATE/DELETE statements without introspecting MySQL).

Column layout is deterministic (hash of db.table): 16..32 columns of
mixed types so canal-client payloads land around 0.5-2 KB like the real
production tables. No credentials involved — everything targets the
local docker MySQL.

Usage: python3 gen_schema.py <out-dir>
"""

import hashlib
import json
import sys
from pathlib import Path

# (database, [tables]) — exactly the subscription sets of the five
# pipelines in canal-sync-consolidated.yaml (P5 adds entity_org_admin
# and link_district_user_role on top of P1's set).
SCHEMA = [
    (
        "neworiental_v3",
        [
            "entity_subject", "entity_public_school", "entity_private_school",
            "link_school_microcourse", "link_platform_k_microcourse",
            "link_schoolk_mirror_mcourse", "entity_school_ksystem",
            "link_qiniu_convert", "yunying_question_info_topic",
            "yunying_question_info_chapter", "link_teaching_chapter_knowledge",
            "entity_res_clip", "entity_res_courseware", "entity_exercise_paper",
            "counter_res", "entity_exercise", "link_respackage_resource",
            "base_word", "entity_course_package", "link_course_package_resource",
            "link_custom_list_resource", "link_custom_question_resource",
            "link_region_list_resource", "base_word_translation",
            "entity_question", "link_resource_chapter", "link_question_chapter",
            "link_question_topic", "link_exercise_topic", "link_resource_topic",
            "link_question_exam_sites", "link_exercise_special",
            "link_resource_series", "entity_question_check",
            "link_school_resourcelib_question", "link_school_resourcelib_material",
            "link_school_resourcelib_microcourse", "link_region_resourcelib_question",
            "link_region_resourcelib_resource", "entity_question_version",
            "entity_question_weight", "entity_teaching_chapter", "entity_topic",
            "entity_teaching_directory", "entity_special_exam_sites",
            "entity_region_list", "entity_region_list_group", "entity_ksystem_dict",
            "entity_custom_ksystem", "entity_org_admin",
        ],
    ),
    (
        "neworiental_user",
        [
            "entity_user", "entity_profile_student", "entity_profile_teacher",
            "entity_profile_parent", "l_extend_user", "l_class_student",
            "l_class_teacher", "link_student_school", "link_district_user_role",
        ],
    ),
    (
        "neworiental_product",
        [
            "entity_sku", "entity_sku_extend", "entity_sku_info_json",
            "entity_sku_pic", "link_item_category",
        ],
    ),
    (
        "ailearn_okminicourse",
        [
            "ailearn_minicourse_v2", "ailearn_minicourse_content",
            "ailearn_minicourse_latest_version", "link_minicourse_attach_v2",
            "link_entity_tag", "ailearn_tag", "ailearn_resources_resource",
            "link_resources_attach", "ailearn_minicourse_goods",
            "link_coupon_goods", "ailearn_goods_coupon", "link_minicourse_goods",
            "link_minicourse_serve_timetable", "link_goods_school_collect",
            "ailearn_minicourse_img", "link_minicourse_resource",
            "ailearn_account", "link_account_attach",
        ],
    ),
    (
        "ailearn-english",
        ["english_dictionary_words"],
    ),
    (
        "neworiental_data_recommand",
        [f"entity_question_lib_{i}" for i in range(10)]
        + [f"entity_resource_lib_{i}" for i in range(10)]
        + ["entity_question_used", "entity_resource_used"],
    ),
    (
        "ailearn_kefu",
        ["repository_content"],
    ),
]


def column_layout(db: str, table: str):
    """Deterministic column list: 16..32 columns keyed by hash(db.table)."""
    digest = hashlib.sha256(f"{db}.{table}".encode()).digest()
    count = 16 + digest[0] % 17  # 16..32
    columns = []
    for i in range(count):
        # Spread kinds over 10 buckets of the digest byte so ~40% string,
        # ~30% int, ~20% datetime, ~10% decimal (comparing the RAW byte
        # against small values would almost always fall through to dec).
        kind = digest[(1 + i) % 32] % 10
        if kind < 4:  # ~40% strings
            width = (64, 128, 255)[digest[2 + i % 3] % 3]
            columns.append((f"c{i:02d}_s", f"VARCHAR({width})", "str"))
        elif kind < 7:  # ~30% integers
            columns.append((f"c{i:02d}_i", "BIGINT", "int"))
        elif kind < 9:  # ~20% temporal
            columns.append((f"c{i:02d}_t", "DATETIME", "dt"))
        else:  # ~10% decimal
            columns.append((f"c{i:02d}_d", "DECIMAL(10,2)", "dec"))
    # One TEXT column on ~half the tables for larger payloads.
    if digest[31] % 2 == 0:
        columns.append(("c_payload", "TEXT", "text"))
    return columns


def main() -> int:
    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    out_dir.mkdir(parents=True, exist_ok=True)

    sql = [
        "-- Generated by gen_schema.py — benchmark schema for",
        "-- canal-sync-consolidated (do not edit by hand).",
        "SET FOREIGN_KEY_CHECKS = 0;",
    ]
    manifest = {}
    for db, tables in SCHEMA:
        sql.append(f"CREATE DATABASE IF NOT EXISTS `{db}`;")
        sql.append(f"USE `{db}`;")
        for table in tables:
            cols = column_layout(db, table)
            manifest[f"{db}.{table}"] = [
                {"name": name, "kind": kind} for name, _sql_type, kind in cols
            ]
            body = ",\n  ".join(
                ["`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"]
                + [f"`{name}` {_sql_type} NULL" for name, _sql_type, _k in cols]
            )
            sql.append(f"DROP TABLE IF EXISTS `{table}`;")
            sql.append(f"CREATE TABLE `{table}` (\n  {body}\n) ENGINE=InnoDB;")

    (out_dir / "init_bench_schema.sql").write_text("\n".join(sql) + "\n")
    (out_dir / "bench_schema.json").write_text(json.dumps(manifest, indent=1))
    total = sum(len(t) for _d, t in SCHEMA)
    print(f"generated {total} tables across {len(SCHEMA)} databases -> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
