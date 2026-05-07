from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def fetch_rows(conn: sqlite3.Connection, query: str):
    cur = conn.execute(query)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export human review labels and evaluation summaries to evals/."
    )
    parser.add_argument("--db-path", type=Path, default=Path("quality_checks.db"))
    parser.add_argument("--evals-dir", type=Path, default=Path("evals"))
    args = parser.parse_args()

    if not args.db_path.exists():
        raise FileNotFoundError(f"DB not found: {args.db_path}")

    labelled_dir = args.evals_dir / "labelled_sets"
    results_dir = args.evals_dir / "results"
    findings_dir = args.evals_dir / "findings"
    labelled_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    findings_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db_path)
    try:
        labels = fetch_rows(
            conn,
            """
            SELECT h.quality_result_id, h.event_id, h.check_name, h.human_label, h.reviewer,
                   h.notes, h.labeled_at, q.run_id, q.prompt_version, q.model, q.reason,
                   q.confidence, q.result as llm_result
            FROM human_review_labels h
            JOIN llm_quality_results q ON q.id = h.quality_result_id
            ORDER BY h.labeled_at DESC
            """,
        )

        by_check = fetch_rows(
            conn,
            """
            SELECT check_name, human_label, COUNT(*) as count
            FROM human_review_labels
            GROUP BY check_name, human_label
            ORDER BY check_name, count DESC
            """,
        )

        totals = fetch_rows(
            conn,
            """
            SELECT
              COUNT(*) as total_reviews,
              SUM(CASE WHEN human_label='override_pass' THEN 1 ELSE 0 END) as overrides,
              SUM(CASE WHEN human_label='approve_fail' THEN 1 ELSE 0 END) as approve_fail
            FROM human_review_labels
            """,
        )
    finally:
        conn.close()

    labels_file = labelled_dir / "human_review_labels_v1.jsonl"
    with labels_file.open("w", encoding="utf-8") as f:
        for row in labels:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "dataset": "human_review_labels",
        "total_reviews": totals[0]["total_reviews"] if totals else 0,
        "overrides": totals[0]["overrides"] if totals else 0,
        "approve_fail": totals[0]["approve_fail"] if totals else 0,
        "breakdown_by_check_and_label": by_check,
    }
    summary_file = results_dir / "human_review_evaluation_summary_v1.json"
    summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    findings_md = findings_dir / "human_review_findings.md"
    findings_md.write_text(
        "\n".join(
            [
                "# Human Review Findings",
                "",
                "This document summarizes exported human review outcomes from SQLite.",
                "",
                f"- Total reviewed items: **{summary['total_reviews']}**",
                f"- Overrides (`override_pass`): **{summary['overrides']}**",
                f"- Approved fails (`approve_fail`): **{summary['approve_fail']}**",
                "",
                "## Breakdown by Check and Human Label",
                "",
            ]
            + [
                f"- `{row['check_name']}` / `{row['human_label']}`: **{row['count']}**"
                for row in by_check
            ]
            + [
                "",
                "## Notes",
                "- Use these artifacts for governance and prompt tuning decisions.",
                "- Compare this with drift reports to understand version impact.",
            ]
        ),
        encoding="utf-8",
    )

    print(f"Exported labels: {labels_file}")
    print(f"Exported evaluation summary: {summary_file}")
    print(f"Exported findings markdown: {findings_md}")


if __name__ == "__main__":
    main()
