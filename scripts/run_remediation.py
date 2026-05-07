from __future__ import annotations

import argparse
import json
import os
import socket
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


INPUT_FILES = {
    "semantic_accuracy": "semantic_accuracy_results.jsonl",
    "entity_resolution": "entity_resolution_results.jsonl",
    "source_credibility": "source_credibility_results.jsonl",
}

REMEDIATION_PROMPT_VERSION = "v1"
MODEL_PRICING_PER_1K_TOKENS = {
    "gpt-4o-mini": {"input": 0.00015, "output": 0.00060},
    "gpt-4o": {"input": 0.00500, "output": 0.01500},
    "gpt-4.1": {"input": 0.01000, "output": 0.03000},
}


def estimate_openai_cost_usd(model: str, usage: Dict[str, Any]) -> float | None:
    price = MODEL_PRICING_PER_1K_TOKENS.get(model)
    if not price:
        return None
    in_tokens = usage.get("prompt_tokens")
    out_tokens = usage.get("completion_tokens")
    if not isinstance(in_tokens, int) or not isinstance(out_tokens, int):
        return None
    return (in_tokens / 1000.0) * price["input"] + (out_tokens / 1000.0) * price["output"]


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def call_openai_json(
    api_key: str,
    model: str,
    system_prompt: str,
    payload: Dict[str, Any],
    timeout_seconds: int = 300,
    max_retries: int = 3,
) -> Dict[str, Any]:
    url = "https://api.openai.com/v1/chat/completions"
    request_payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    "Return only JSON.\n"
                    "INPUT:\n"
                    f"{json.dumps(payload, ensure_ascii=False)}"
                ),
            },
        ],
    }

    req = urllib.request.Request(
        url=url,
        data=json.dumps(request_payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    last_error: Exception | None = None
    started = time.perf_counter()
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", errors="replace")
            # Retry on transient server throttling/errors only.
            if err.code in {408, 429, 500, 502, 503, 504} and attempt < max_retries:
                time.sleep(min(2**attempt, 8))
                last_error = RuntimeError(
                    f"Remediation API transient error ({err.code}): {detail}"
                )
                continue
            raise RuntimeError(f"Remediation API error ({err.code}): {detail}") from err
        except (urllib.error.URLError, TimeoutError, socket.timeout) as err:
            last_error = err
            if attempt < max_retries:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"Remediation request timed out: {err}") from err
    else:
        raise RuntimeError(f"Remediation request failed after retries: {last_error}")

    content = body["choices"][0]["message"]["content"]
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as err:
        raise RuntimeError(f"Model did not return valid JSON: {content}") from err
    latency_ms = int((time.perf_counter() - started) * 1000)
    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    return {
        "parsed": parsed,
        "latency_ms": latency_ms,
        "usage": usage,
        "request_payload": request_payload,
        "response_payload": body,
    }


def remediation_prompt() -> str:
    return (
        "You are a data remediation assistant for structured news-event extraction.\n"
        "Given failed records and their failed quality checks, propose minimal safe remediation for each record.\n"
        "Rules:\n"
        "1) Use only provided input; do not use external or web data.\n"
        "2) Do not invent facts.\n"
        "3) Prefer no_change if evidence is ambiguous.\n"
        "4) Keep source immutable; suggest a patch only.\n"
        "Return only JSON with this schema:\n"
        "{\n"
        '  "results": [\n'
        "    {\n"
        '      "event_id": "string",\n'
        '      "decision": "correct_label|correct_entity_link|mark_low_credibility|no_change",\n'
        '      "proposed_patch": {"path":"string","old_value":"any","new_value":"any"} or null,\n'
        '      "reasoning": "string",\n'
        '      "confidence": 0.0,\n'
        '      "evidence_spans": [{"text":"string","source_field":"article_sentence|source_title|source_body"}],\n'
        '      "review_recommended": true\n'
        "    }\n"
        "  ]\n"
        "}"
    )


def is_failed(row: Dict[str, Any]) -> bool:
    llm_check = row.get("llm_check") or {}
    return llm_check.get("result") == "fail"


def run_remediation(
    input_dir: Path,
    output_dir: Path,
    model: str,
    llm_log_file: Path | None = None,
    max_rows_per_file: int | None = None,
) -> None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY is not set.")

    output_dir.mkdir(parents=True, exist_ok=True)
    prompt = remediation_prompt()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for check_name, file_name in INPUT_FILES.items():
        input_path = input_dir / file_name
        if not input_path.exists():
            raise FileNotFoundError(f"Missing input file: {input_path}")

        output_path = output_dir / f"{check_name}_remediations.jsonl"
        failed_rows: List[Dict[str, Any]] = []
        with input_path.open("r", encoding="utf-8") as in_f:
            for raw in in_f:
                line = raw.strip()
                if not line:
                    continue
                row = json.loads(line)
                if not is_failed(row):
                    continue
                if max_rows_per_file is not None and len(failed_rows) >= max_rows_per_file:
                    continue
                failed_rows.append(row)

        remediations_by_event_id: Dict[str, Dict[str, Any]] = {}
        remediation_metrics_by_event_id: Dict[str, Dict[str, Any]] = {}
        if failed_rows:
            # Target: one request per file. If timeout occurs, degrade to chunked fallback.
            try:
                payload = {
                    "check_name": check_name,
                    "failed_records": [
                        {
                            "event_id": (row.get("llm_check") or {}).get("event_id"),
                            "source_record": row.get("source_record"),
                            "failed_check": row.get("llm_check"),
                        }
                        for row in failed_rows
                    ],
                }
                response = call_openai_json(
                    api_key=api_key,
                    model=model,
                    system_prompt=prompt,
                    payload=payload,
                )
                results = response["parsed"].get("results")
                if isinstance(results, list):
                    for item in results:
                        event_id = item.get("event_id")
                        if isinstance(event_id, str):
                            remediations_by_event_id[event_id] = item
                call_cost = estimate_openai_cost_usd(model, response.get("usage", {}))
                per_row_cost = (
                    (call_cost / len(failed_rows))
                    if isinstance(call_cost, (int, float)) and len(failed_rows) > 0
                    else None
                )
                per_row_latency = (
                    (float(response.get("latency_ms")) / len(failed_rows))
                    if isinstance(response.get("latency_ms"), (int, float))
                    and len(failed_rows) > 0
                    else None
                )
                for row in failed_rows:
                    event_id = (row.get("llm_check") or {}).get("event_id")
                    if isinstance(event_id, str):
                        remediation_metrics_by_event_id[event_id] = {
                            "estimated_latency_ms_per_row": per_row_latency,
                            "estimated_cost_usd_per_row": per_row_cost,
                            "call_latency_ms": response.get("latency_ms"),
                            "call_usage": response.get("usage", {}),
                        }
                if llm_log_file is not None:
                    append_jsonl(
                        llm_log_file,
                        {
                            "stage": "remediation",
                            "check_name": check_name,
                            "model": model,
                            "prompt_version": REMEDIATION_PROMPT_VERSION,
                            "latency_ms": response.get("latency_ms"),
                            "usage": response.get("usage", {}),
                            "cost_usd": estimate_openai_cost_usd(
                                model, response.get("usage", {})
                            ),
                            "decision": {
                                "total_results": len(results) if isinstance(results, list) else 0
                            },
                            "request": response.get("request_payload"),
                            "response": response.get("response_payload"),
                        },
                    )
            except RuntimeError as err:
                # Fallback for large payload timeouts: smaller chunks for this file only.
                print(f"{check_name}: full-file request failed, falling back to chunked mode.")
                chunk_size = 10
                for i in range(0, len(failed_rows), chunk_size):
                    chunk = failed_rows[i : i + chunk_size]
                    payload = {
                        "check_name": check_name,
                        "failed_records": [
                            {
                                "event_id": (row.get("llm_check") or {}).get("event_id"),
                                "source_record": row.get("source_record"),
                                "failed_check": row.get("llm_check"),
                            }
                            for row in chunk
                        ],
                    }
                    response = call_openai_json(
                        api_key=api_key,
                        model=model,
                        system_prompt=prompt,
                        payload=payload,
                    )
                    results = response["parsed"].get("results")
                    if isinstance(results, list):
                        for item in results:
                            event_id = item.get("event_id")
                            if isinstance(event_id, str):
                                remediations_by_event_id[event_id] = item
                    call_cost = estimate_openai_cost_usd(model, response.get("usage", {}))
                    per_row_cost = (
                        (call_cost / len(chunk))
                        if isinstance(call_cost, (int, float)) and len(chunk) > 0
                        else None
                    )
                    per_row_latency = (
                        (float(response.get("latency_ms")) / len(chunk))
                        if isinstance(response.get("latency_ms"), (int, float))
                        and len(chunk) > 0
                        else None
                    )
                    for row in chunk:
                        event_id = (row.get("llm_check") or {}).get("event_id")
                        if isinstance(event_id, str):
                            remediation_metrics_by_event_id[event_id] = {
                                "estimated_latency_ms_per_row": per_row_latency,
                                "estimated_cost_usd_per_row": per_row_cost,
                                "call_latency_ms": response.get("latency_ms"),
                                "call_usage": response.get("usage", {}),
                            }
                    if llm_log_file is not None:
                        append_jsonl(
                            llm_log_file,
                            {
                                "stage": "remediation",
                                "check_name": check_name,
                                "model": model,
                                "prompt_version": REMEDIATION_PROMPT_VERSION,
                                "latency_ms": response.get("latency_ms"),
                                "usage": response.get("usage", {}),
                                "cost_usd": estimate_openai_cost_usd(
                                    model, response.get("usage", {})
                                ),
                                "decision": {
                                    "total_results": len(results)
                                    if isinstance(results, list)
                                    else 0
                                },
                                "request": response.get("request_payload"),
                                "response": response.get("response_payload"),
                                "note": "chunked_fallback",
                            },
                        )

        processed = 0
        with output_path.open("w", encoding="utf-8") as out_f:
            for row in failed_rows:
                llm_check = row.get("llm_check") or {}
                event_id = llm_check.get("event_id")
                remediation = remediations_by_event_id.get(event_id)
                if not isinstance(remediation, dict):
                    remediation = {
                        "event_id": event_id,
                        "decision": "no_change",
                        "proposed_patch": None,
                        "reasoning": "No valid remediation returned for this row.",
                        "confidence": 0.0,
                        "evidence_spans": [],
                        "review_recommended": True,
                    }
                out_line = {
                    "run_id": run_id,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "check_name": check_name,
                    "model": model,
                    "source_record": row.get("source_record"),
                    "failed_check": llm_check,
                    "remediation": remediation,
                    "llm_metrics": remediation_metrics_by_event_id.get(event_id),
                }
                out_f.write(json.dumps(out_line, ensure_ascii=False) + "\n")
                processed += 1

        print(f"{check_name}: {processed}/{len(failed_rows)} failed rows remediated")

    print(f"Done. Remediation files written to: {output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run remediation on failed rows from quality-check outputs."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("output"),
        help="Directory containing quality-check output JSONL files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("remediations"),
        help="Directory to write remediation JSONL files",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o-mini",
        help="OpenAI model to generate remediation recommendations",
    )
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=None,
        help="Optional cap of failed rows remediated per file",
    )
    parser.add_argument(
        "--llm-log-file",
        type=Path,
        default=Path("logs/llm_calls.jsonl"),
        help="Append-only audit log for each LLM call",
    )
    args = parser.parse_args()

    if args.max_rows_per_file is not None and args.max_rows_per_file <= 0:
        raise ValueError("--max-rows-per-file must be positive if provided")

    run_remediation(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        model=args.model,
        llm_log_file=args.llm_log_file,
        max_rows_per_file=args.max_rows_per_file,
    )


if __name__ == "__main__":
    main()
