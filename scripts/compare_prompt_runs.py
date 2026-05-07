from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Tuple, List


CHECK_FILES = {
    "semantic_accuracy": "semantic_accuracy_results.jsonl",
    "entity_resolution": "entity_resolution_results.jsonl",
    "source_credibility": "source_credibility_results.jsonl",
}


def load_runs(path: Path) -> Dict[Tuple[str, str], Dict[str, Dict[str, Any]]]:
    """
    Returns mapping: (prompt_version, run_id) -> { event_id -> row }
    """
    runs: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            run_id = row.get("run_id")
            prompt_version = row.get("prompt_version")
            llm_check = row.get("llm_check") or {}
            event_id = llm_check.get("event_id")
            if isinstance(event_id, str) and isinstance(run_id, str) and isinstance(prompt_version, str):
                key = (prompt_version, run_id)
                runs.setdefault(key, {})[event_id] = row
    return runs


def latest_run_for_version(
    runs: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]], version: str
) -> Tuple[str | None, Dict[str, Dict[str, Any]]]:
    candidates = [(run_id, rows) for (v, run_id), rows in runs.items() if v == version]
    if not candidates:
        return None, {}
    # run_id is sortable (YYYYMMDDTHHMMSSZ)
    run_id, rows = sorted(candidates, key=lambda x: x[0])[-1]
    return run_id, rows


def compare_check(
    base_rows: Dict[str, Dict[str, Any]], candidate_rows: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    base = base_rows
    candidate = candidate_rows
    common_ids = sorted(set(base).intersection(candidate))

    decision_changed = 0
    confidence_delta_total = 0.0
    compared = 0
    examples = []

    for event_id in common_ids:
        b = base[event_id].get("llm_check") or {}
        c = candidate[event_id].get("llm_check") or {}
        b_res = b.get("result")
        c_res = c.get("result")
        b_conf = b.get("confidence")
        c_conf = c.get("confidence")

        if b_res != c_res:
            decision_changed += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "event_id": event_id,
                        "base_result": b_res,
                        "candidate_result": c_res,
                        "base_confidence": b_conf,
                        "candidate_confidence": c_conf,
                    }
                )

        if isinstance(b_conf, (int, float)) and isinstance(c_conf, (int, float)):
            confidence_delta_total += float(c_conf) - float(b_conf)
            compared += 1

    return {
        "base_rows": len(base),
        "candidate_rows": len(candidate),
        "common_rows": len(common_ids),
        "decision_changed_count": decision_changed,
        "decision_changed_rate": (decision_changed / len(common_ids)) if common_ids else 0.0,
        "avg_confidence_delta_candidate_minus_base": (
            confidence_delta_total / compared if compared else None
        ),
        "examples": examples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare prompt versions (v1 vs v2) within a single output folder."
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("output"), help="Folder containing output JSONL files"
    )
    parser.add_argument(
        "--base-version", type=str, default="v1", help="Baseline prompt version (e.g. v1)"
    )
    parser.add_argument(
        "--candidate-version", type=str, default="v2", help="Candidate prompt version (e.g. v2)"
    )
    parser.add_argument(
        "--report-name",
        type=str,
        default=None,
        help="Optional report filename (defaults to drift_report_<v1>_vs_<v2>.json)",
    )
    args = parser.parse_args()

    out_folder = Path("Prompt result version comparision")
    out_folder.mkdir(parents=True, exist_ok=True)
    report: Dict[str, Any] = {
        "base_version": args.base_version,
        "candidate_version": args.candidate_version,
        "checks": {},
    }
    for check_name, file_name in CHECK_FILES.items():
        file_path = args.output_dir / file_name
        if not file_path.exists():
            report["checks"][check_name] = {"error": f"Missing file: {file_path}"}
            continue
        runs = load_runs(file_path)
        base_run_id, base_rows = latest_run_for_version(runs, args.base_version)
        cand_run_id, cand_rows = latest_run_for_version(runs, args.candidate_version)
        if not base_run_id or not cand_run_id:
            report["checks"][check_name] = {
                "error": f"Missing runs for {args.base_version} or {args.candidate_version}",
                "base_run_id": base_run_id,
                "candidate_run_id": cand_run_id,
            }
            continue

        report["checks"][check_name] = {
            "base_run_id": base_run_id,
            "candidate_run_id": cand_run_id,
            **compare_check(base_rows, cand_rows),
        }

    report_name = args.report_name or f"drift_report_{args.base_version}_vs_{args.candidate_version}.json"
    report_file = out_folder / report_name
    report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Drift report written to: {report_file}")


if __name__ == "__main__":
    main()
