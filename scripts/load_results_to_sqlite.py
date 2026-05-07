from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable


QUALITY_FILES = {
    "semantic_accuracy": "semantic_accuracy_results.jsonl",
    "entity_resolution": "entity_resolution_results.jsonl",
    "source_credibility": "source_credibility_results.jsonl",
}

REMEDIATION_FILES = {
    "semantic_accuracy": "semantic_accuracy_remediations.jsonl",
    "entity_resolution": "entity_resolution_remediations.jsonl",
    "source_credibility": "source_credibility_remediations.jsonl",
}


def read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def upsert_record(conn: sqlite3.Connection, event_id: str, source_record: Dict[str, Any]) -> None:
    now = "datetime('now')"
    conn.execute(
        f"""
        INSERT INTO records(event_id, source_record_json, first_seen_at, last_seen_at)
        VALUES(?, ?, {now}, {now})
        ON CONFLICT(event_id) DO UPDATE SET
          source_record_json = excluded.source_record_json,
          last_seen_at = {now}
        """,
        (event_id, json.dumps(source_record, ensure_ascii=False)),
    )


def normalize_run_id(row: Dict[str, Any], check_name: str) -> str:
    run_id = row.get("run_id")
    if isinstance(run_id, str) and run_id.strip():
        return run_id
    # Backfill for legacy rows created before run_id was added.
    return f"legacy_{check_name}"


def load_quality(conn: sqlite3.Connection, output_dir: Path) -> None:
    for check_name, file_name in QUALITY_FILES.items():
        path = output_dir / file_name
        for row in read_jsonl(path):
            llm_check = row.get("llm_check") or {}
            source_record = row.get("source_record") or {}
            event_id = llm_check.get("event_id")
            if not isinstance(event_id, str):
                continue
            upsert_record(conn, event_id, source_record)
            run_id = normalize_run_id(row, check_name)

            conn.execute(
                """
                INSERT INTO llm_quality_results(
                  run_id, created_at, check_name, prompt_version, prompt_file, model,
                  event_id, result, confidence, reason, llm_check_json, llm_metrics_json, source_record_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row.get("created_at"),
                    check_name,
                    row.get("prompt_version"),
                    row.get("prompt_file"),
                    row.get("model"),
                    event_id,
                    llm_check.get("result"),
                    llm_check.get("confidence"),
                    llm_check.get("reason"),
                    json.dumps(llm_check, ensure_ascii=False),
                    json.dumps(row.get("llm_metrics"), ensure_ascii=False),
                    json.dumps(source_record, ensure_ascii=False),
                ),
            )


def load_remediations(conn: sqlite3.Connection, remediations_dir: Path) -> None:
    for check_name, file_name in REMEDIATION_FILES.items():
        path = remediations_dir / file_name
        for row in read_jsonl(path):
            failed_check = row.get("failed_check") or {}
            source_record = row.get("source_record") or {}
            event_id = failed_check.get("event_id")
            if isinstance(event_id, str):
                upsert_record(conn, event_id, source_record)
            run_id = normalize_run_id(row, check_name)

            conn.execute(
                """
                INSERT INTO llm_remediation_results(
                  run_id, created_at, check_name, model, event_id,
                  failed_check_json, remediation_json, llm_metrics_json, source_record_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row.get("created_at"),
                    check_name,
                    row.get("model"),
                    event_id,
                    json.dumps(failed_check, ensure_ascii=False),
                    json.dumps(row.get("remediation"), ensure_ascii=False),
                    json.dumps(row.get("llm_metrics"), ensure_ascii=False),
                    json.dumps(source_record, ensure_ascii=False),
                ),
            )


def load_audit(conn: sqlite3.Connection, llm_log_file: Path) -> None:
    for row in read_jsonl(llm_log_file):
        run_id = row.get("run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            run_id = f"legacy_{row.get('stage') or 'unknown'}"
        conn.execute(
            """
            INSERT INTO llm_call_audit(
              stage, run_id, check_name, model, provider, prompt_file, prompt_version,
              latency_ms, cost_usd, usage_json, decision_json, request_json, response_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("stage"),
                run_id,
                row.get("check_name"),
                row.get("model"),
                row.get("provider"),
                row.get("prompt_file"),
                row.get("prompt_version"),
                row.get("latency_ms"),
                row.get("cost_usd"),
                json.dumps(row.get("usage"), ensure_ascii=False),
                json.dumps(row.get("decision"), ensure_ascii=False),
                json.dumps(row.get("request"), ensure_ascii=False),
                json.dumps(row.get("response"), ensure_ascii=False),
            ),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Load quality artifacts into SQLite DB.")
    parser.add_argument("--db-path", type=Path, default=Path("quality_checks.db"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--remediations-dir", type=Path, default=Path("remediations"))
    parser.add_argument("--llm-log-file", type=Path, default=Path("logs/llm_calls.jsonl"))
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    try:
        load_quality(conn, args.output_dir)
        load_remediations(conn, args.remediations_dir)
        load_audit(conn, args.llm_log_file)
        conn.commit()
    finally:
        conn.close()

    print(f"Loaded artifacts into DB: {args.db_path}")


if __name__ == "__main__":
    main()
