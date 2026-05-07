from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Tuple
from datetime import datetime, timezone

from run_quality_checks import DEFAULT_MODEL_ROUTING, run_checks
from run_remediation import run_remediation


def triage_record(record: Dict[str, Any]) -> Tuple[str, str]:
    data = (record.get("data") or [{}])[0]
    attrs = data.get("attributes") or {}
    event_id = data.get("id")
    category = attrs.get("category")
    article_sentence = attrs.get("article_sentence")
    if not event_id or not category or not article_sentence:
        return "fail", "Missing required event fields"
    return "escalate", "Required fields present"


def run_triage(
    input_file: Path,
    triage_log: Path,
    llm_input_file: Path,
    triage_fail_sink: Path,
    run_id: str,
) -> Dict[str, int]:
    triage_log.parent.mkdir(parents=True, exist_ok=True)
    llm_input_file.parent.mkdir(parents=True, exist_ok=True)
    triage_fail_sink.parent.mkdir(parents=True, exist_ok=True)
    counts = {"pass": 0, "fail": 0, "escalate": 0}

    with (
        input_file.open("r", encoding="utf-8") as src,
        triage_log.open("a", encoding="utf-8") as log_f,
        llm_input_file.open("w", encoding="utf-8") as llm_f,
        triage_fail_sink.open("a", encoding="utf-8") as fail_f,
    ):
        for raw in src:
            line = raw.strip()
            if not line:
                continue
            record = json.loads(line)
            decision, reason = triage_record(record)
            counts[decision] += 1
            event_id = ((record.get("data") or [{}])[0]).get("id")
            log_f.write(
                json.dumps(
                    {
                        "run_id": run_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "event_id": event_id,
                        "decision": decision,
                        "reason": reason,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            if decision == "escalate":
                llm_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            elif decision == "fail":
                # Scalable solution: a quarantine / dead-letter stream for invalid inputs.
                fail_f.write(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "event_id": event_id,
                            "reason": reason,
                            "source_record": record,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Run end-to-end quality + remediation pipeline.")
    parser.add_argument("--input-file", type=Path, default=Path("input/sample_100.jsonl"))
    parser.add_argument("--prompts-dir", type=Path, default=Path("prompts"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--remediations-dir", type=Path, default=Path("remediations"))
    parser.add_argument("--logs-dir", type=Path, default=Path("logs"))
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--max-body-chars", type=int, default=0)
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    escalated_input = args.logs_dir / "llm_escalated_input.jsonl"
    triage_log = args.logs_dir / "triage_decisions.jsonl"
    triage_fail_sink = args.logs_dir / "triage_failed_records.jsonl"
    llm_log_file = args.logs_dir / "llm_calls.jsonl"

    counts = run_triage(
        args.input_file, triage_log, escalated_input, triage_fail_sink, run_id=run_id
    )
    print(
        f"Triage completed: pass={counts['pass']} fail={counts['fail']} escalate={counts['escalate']}"
    )

    run_checks(
        input_file=escalated_input,
        prompts_dir=args.prompts_dir,
        output_dir=args.output_dir,
        model_routing=DEFAULT_MODEL_ROUTING,
        batch_size=args.batch_size,
        max_body_chars=args.max_body_chars,
        llm_log_file=llm_log_file,
        run_id=run_id,
        max_records=None,
        sleep_seconds=0.0,
    )

    run_remediation(
        input_dir=args.output_dir,
        output_dir=args.remediations_dir,
        model="gpt-4o-mini",
        llm_log_file=llm_log_file,
        max_rows_per_file=None,
    )
    print("Pipeline done.")


if __name__ == "__main__":
    main()
