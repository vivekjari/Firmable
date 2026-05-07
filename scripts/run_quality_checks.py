from __future__ import annotations

import argparse
import json
import os
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List


PROMPT_BASE_NAMES = {
    "semantic_accuracy": "semantic_accuracy",
    "entity_resolution": "entity_resolution",
    "source_credibility": "source_credibility",
}

DEFAULT_MODEL_ROUTING = {
    "semantic_accuracy": "openai:gpt-4o-mini",
    "entity_resolution": "openai:gpt-4o-mini",
    "source_credibility": "openai:gpt-4o-mini",
}

MODEL_PRICING_PER_1K_TOKENS = {
    "gpt-4o-mini": {"input": 0.00015, "output": 0.00060},
    "gpt-4o": {"input": 0.00500, "output": 0.01500},
    "gpt-4.1": {"input": 0.01000, "output": 0.03000},
}


def parse_prompt_version(filename: str) -> str:
    stem = Path(filename).stem
    parts = stem.split("_")
    tail = parts[-1] if parts else "v1"
    return tail if tail.startswith("v") else "v1"


def utc_run_id_now() -> str:
    # Sortable run id for "latest run" selection.
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def find_latest_prompt_file(prompts_dir: Path, base_name: str) -> str:
    """
    Pick latest prompt file by numeric vN suffix:
    e.g. semantic_accuracy_v1.txt, semantic_accuracy_v2.txt -> picks v2.
    """
    candidates = list(prompts_dir.glob(f"{base_name}_v*.txt"))
    if not candidates:
        raise FileNotFoundError(f"No prompt files found for base '{base_name}' in {prompts_dir}")

    def version_num(p: Path) -> int:
        v = parse_prompt_version(p.name)
        try:
            return int(v.lstrip("v"))
        except ValueError:
            return 0

    best = max(candidates, key=lambda p: (version_num(p), p.name))
    return best.name


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


def summarize_decisions(results: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"pass": 0, "fail": 0, "other": 0}
    for item in results:
        result_obj = item.get("result")
        if isinstance(result_obj, dict):
            decision = result_obj.get("result")
            if decision == "pass":
                summary["pass"] += 1
            elif decision == "fail":
                summary["fail"] += 1
            else:
                summary["other"] += 1
        else:
            summary["other"] += 1
    return summary


def load_prompt(prompts_dir: Path, filename: str) -> str:
    path = prompts_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    return path.read_text(encoding="utf-8")


def extract_company_entities(included: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    entities: List[Dict[str, Any]] = []
    for item in included:
        if item.get("type") != "company":
            continue
        attributes = item.get("attributes") or {}
        entities.append(
            {
                "entity_id": item.get("id"),
                "entity_type": "company",
                "entity_name": attributes.get("company_name"),
                "entity_domain": attributes.get("domain"),
            }
        )
    return entities


def extract_record_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    data_items = record.get("data") or []
    included = record.get("included") or []
    event = data_items[0] if data_items else {}
    attrs = event.get("attributes") or {}

    source = next((x for x in included if x.get("type") == "news_article"), {})
    source_attrs = source.get("attributes") or {}

    return {
        "event_id": event.get("id"),
        "labeled_event_type": attrs.get("category"),
        "event_summary": attrs.get("summary"),
        "article_sentence": attrs.get("article_sentence"),
        "source_title": source_attrs.get("title"),
        "source_body": source_attrs.get("body"),
        "event_date": attrs.get("effective_date"),
        "source_url": source_attrs.get("url"),
        "published_at": source_attrs.get("published_at"),
        "linked_entities": extract_company_entities(included),
    }


def _truncate_text(value: Any, max_chars: int) -> Any:
    if not isinstance(value, str):
        return value
    if max_chars <= 0 or len(value) <= max_chars:
        return value
    return value[:max_chars]


def build_check_input(
    check_name: str, payload: Dict[str, Any], max_body_chars: int
) -> Dict[str, Any]:
    if check_name == "semantic_accuracy":
        return {
            "event_id": payload.get("event_id"),
            "labeled_event_type": payload.get("labeled_event_type"),
            "event_summary": payload.get("event_summary"),
            "article_sentence": payload.get("article_sentence"),
            "source_title": payload.get("source_title"),
            "source_body": _truncate_text(payload.get("source_body"), max_body_chars),
            "event_date": payload.get("event_date"),
        }
    if check_name == "entity_resolution":
        return {
            "event_id": payload.get("event_id"),
            "labeled_event_type": payload.get("labeled_event_type"),
            "event_summary": payload.get("event_summary"),
            "article_sentence": payload.get("article_sentence"),
            "source_title": payload.get("source_title"),
            "source_body": _truncate_text(payload.get("source_body"), max_body_chars),
            "linked_entities": payload.get("linked_entities", []),
        }
    if check_name == "source_credibility":
        return {
            "event_id": payload.get("event_id"),
            "labeled_event_type": payload.get("labeled_event_type"),
            "event_summary": payload.get("event_summary"),
            "article_sentence": payload.get("article_sentence"),
            "source_title": payload.get("source_title"),
            "source_body": _truncate_text(payload.get("source_body"), max_body_chars),
            "source_url": payload.get("source_url"),
            "published_at": payload.get("published_at"),
            "dedupe_context": {
                "possible_duplicate_count": 0,
                "similar_event_summaries": [],
            },
        }
    raise ValueError(f"Unknown check: {check_name}")


def call_openai_json(
    api_key: str,
    model: str,
    prompt_text: str,
    check_inputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    url = "https://api.openai.com/v1/chat/completions"
    user_message = (
        "Evaluate each record according to the prompt.\n"
        "Return only JSON with this exact schema:\n"
        '{"results":[{"event_id":"string","result":{...}}]}\n\n'
        f"INPUT:\n{json.dumps(check_inputs, ensure_ascii=False)}"
    )

    request_payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": prompt_text},
            {"role": "user", "content": user_message},
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

    started = time.perf_counter()
    body: Dict[str, Any] | None = None
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", errors="replace")
            if err.code in {408, 429, 500, 502, 503, 504} and attempt < 3:
                time.sleep(min(2**attempt, 8))
                last_error = RuntimeError(f"LLM transient API error ({err.code}): {detail}")
                continue
            raise RuntimeError(f"LLM API error ({err.code}): {detail}") from err
        except (urllib.error.URLError, socket.timeout, ConnectionResetError, TimeoutError) as err:
            last_error = err
            if attempt < 3:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"LLM request failed after retries: {err}") from err
    if body is None:
        raise RuntimeError(f"LLM request failed after retries: {last_error}")

    content = body["choices"][0]["message"]["content"]
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as err:
        raise RuntimeError(f"Model did not return valid JSON: {content}") from err
    results = parsed.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"Model JSON missing 'results' array: {parsed}")
    latency_ms = int((time.perf_counter() - started) * 1000)
    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    return {
        "results": results,
        "latency_ms": latency_ms,
        "usage": usage,
        "request_payload": request_payload,
        "response_payload": body,
    }


def call_anthropic_json(
    api_key: str,
    model: str,
    prompt_text: str,
    check_inputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    url = "https://api.anthropic.com/v1/messages"
    user_message = (
        "Evaluate each record according to the prompt.\n"
        "Return only JSON with this exact schema:\n"
        '{"results":[{"event_id":"string","result":{...}}]}\n\n'
        f"INPUT:\n{json.dumps(check_inputs, ensure_ascii=False)}"
    )
    request_payload = {
        "model": model,
        "max_tokens": 1200,
        "temperature": 0,
        "system": prompt_text,
        "messages": [{"role": "user", "content": user_message}],
    }

    req = urllib.request.Request(
        url=url,
        data=json.dumps(request_payload).encode("utf-8"),
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )

    started = time.perf_counter()
    body: Dict[str, Any] | None = None
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", errors="replace")
            if err.code in {408, 429, 500, 502, 503, 504} and attempt < 3:
                time.sleep(min(2**attempt, 8))
                last_error = RuntimeError(f"LLM transient API error ({err.code}): {detail}")
                continue
            raise RuntimeError(f"LLM API error ({err.code}): {detail}") from err
        except (urllib.error.URLError, socket.timeout, ConnectionResetError, TimeoutError) as err:
            last_error = err
            if attempt < 3:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"LLM request failed after retries: {err}") from err
    if body is None:
        raise RuntimeError(f"LLM request failed after retries: {last_error}")

    parts = body.get("content") or []
    text = "".join(part.get("text", "") for part in parts if part.get("type") == "text")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as err:
        raise RuntimeError(f"Model did not return valid JSON: {text}") from err
    results = parsed.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"Model JSON missing 'results' array: {parsed}")
    latency_ms = int((time.perf_counter() - started) * 1000)
    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    return {
        "results": results,
        "latency_ms": latency_ms,
        "usage": usage,
        "request_payload": request_payload,
        "response_payload": body,
    }


def parse_model_spec(spec: str) -> tuple[str, str]:
    if ":" in spec:
        provider, model = spec.split(":", 1)
        provider = provider.strip().lower()
        model = model.strip()
    else:
        provider, model = "openai", spec.strip()
    if provider not in {"openai", "anthropic"}:
        raise ValueError(
            f"Unsupported provider '{provider}'. Use 'openai:model' or 'anthropic:model'."
        )
    if not model:
        raise ValueError(f"Invalid model spec: {spec}")
    return provider, model


def call_llm_json(
    model_spec: str,
    prompt_text: str,
    check_inputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    provider, model = parse_model_spec(model_spec)
    if provider == "openai":
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise EnvironmentError("OPENAI_API_KEY is not set.")
        return call_openai_json(
            api_key=openai_api_key,
            model=model,
            prompt_text=prompt_text,
            check_inputs=check_inputs,
        )
    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY is not set.")
    return call_anthropic_json(
        api_key=anthropic_api_key,
        model=model,
        prompt_text=prompt_text,
        check_inputs=check_inputs,
    )


def chunked(items: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def read_processed_ids(path: Path) -> set[str]:
    processed: set[str] = set()
    if not path.exists():
        return processed
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            event_id = obj.get("event_id")
            if isinstance(event_id, str):
                processed.add(event_id)
    return processed


def run_checks(
    input_file: Path,
    prompts_dir: Path,
    output_dir: Path,
    model_routing: Dict[str, str],
    batch_size: int = 10,
    max_body_chars: int = 1500,
    llm_log_file: Path | None = None,
    run_id: str | None = None,
    max_records: int | None = None,
    sleep_seconds: float = 0.0,
) -> None:
    if run_id is None:
        run_id = utc_run_id_now()

    prompt_files = {
        check_name: find_latest_prompt_file(prompts_dir, base_name)
        for check_name, base_name in PROMPT_BASE_NAMES.items()
    }

    prompt_meta = {
        check_name: {
            "text": load_prompt(prompts_dir, filename),
            "version": parse_prompt_version(filename),
            "file": filename,
        }
        for check_name, filename in prompt_files.items()
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        check_name: output_dir / f"{check_name}_results.jsonl"
        for check_name in PROMPT_BASE_NAMES
    }
    out_files = {
        check_name: output_paths[check_name].open("a", encoding="utf-8")
        for check_name in PROMPT_BASE_NAMES
    }

    rows: List[Dict[str, Any]] = []
    processed = 0
    with input_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if max_records is not None and processed >= max_records:
                break

            record = json.loads(line)
            payload = extract_record_payload(record)
            rows.append({"record": record, "payload": payload})
            processed += 1

    total_calls = 0
    try:
        for check_name, meta in prompt_meta.items():
            model_spec = model_routing[check_name]
            provider, model_name = parse_model_spec(model_spec)
            prompt_text = meta["text"]
            check_rows = [r for r in rows if r["payload"].get("event_id")]
            check_inputs = [
                build_check_input(check_name, r["payload"], max_body_chars) for r in check_rows
            ]
            batches = chunked(check_inputs, batch_size)
            enriched_count = 0
            for batch_idx, batch in enumerate(batches, start=1):
                call = call_llm_json(
                    model_spec=model_spec,
                    prompt_text=prompt_text,
                    check_inputs=batch,
                )
                results = call["results"]
                total_calls += 1
                batch_rows = check_rows[(batch_idx - 1) * batch_size : batch_idx * batch_size]
                result_by_event_id = {
                    item.get("event_id"): item.get("result")
                    for item in results
                    if item.get("event_id")
                }
                call_usage = call.get("usage", {})
                call_cost = (
                    estimate_openai_cost_usd(model_name, call_usage)
                    if provider == "openai"
                    else None
                )
                per_row_cost = (
                    (call_cost / len(batch_rows))
                    if isinstance(call_cost, (int, float)) and len(batch_rows) > 0
                    else None
                )
                call_latency = call.get("latency_ms")
                per_row_latency = (
                    (float(call_latency) / len(batch_rows))
                    if isinstance(call_latency, (int, float)) and len(batch_rows) > 0
                    else None
                )

                for row in batch_rows:
                    event_id = row["payload"].get("event_id")
                    result_payload = result_by_event_id.get(event_id)
                    if not isinstance(result_payload, dict):
                        result_payload = {
                            "check_name": check_name,
                            "check_version": "v1",
                            "event_id": event_id,
                            "result": "fail",
                            "confidence": 0.0,
                            "reason": "No valid model result returned for this row.",
                        }

                    enriched_row = {
                        "run_id": run_id,
                        "prompt_version": meta["version"],
                        "prompt_file": meta["file"],
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "source_record": row["record"],
                        "llm_check": dict(result_payload),
                        "model": model_spec,
                        "llm_metrics": {
                            "estimated_latency_ms_per_row": per_row_latency,
                            "estimated_cost_usd_per_row": per_row_cost,
                            "call_latency_ms": call_latency,
                            "call_usage": call_usage,
                        },
                    }
                    enriched_row["llm_check"]["event_id"] = event_id
                    enriched_row["llm_check"].setdefault("check_name", check_name)
                    enriched_row["llm_check"].setdefault("check_version", meta["version"])
                    out_files[check_name].write(
                        json.dumps(enriched_row, ensure_ascii=False) + "\n"
                    )
                    enriched_count += 1
                print(f"{check_name}: {enriched_count}/{len(check_rows)} rows enriched")
                if llm_log_file is not None:
                    append_jsonl(
                        llm_log_file,
                        {
                            "stage": "quality_check",
                            "run_id": run_id,
                            "check_name": check_name,
                            "model": model_spec,
                            "provider": provider,
                            "prompt_file": meta["file"],
                            "prompt_version": meta["version"],
                            "latency_ms": call.get("latency_ms"),
                            "usage": call.get("usage", {}),
                            "cost_usd": estimate_openai_cost_usd(
                                model_name, call.get("usage", {})
                            )
                            if provider == "openai"
                            else None,
                            "decision": summarize_decisions(results),
                            "request": call.get("request_payload"),
                            "response": call.get("response_payload"),
                        },
                    )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
    finally:
        for f in out_files.values():
            f.close()

    print(f"Done. Processed {processed} records from input.")
    print(f"Total API calls made this run: {total_calls}")
    print(f"Outputs written to: {output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run LLM quality checks on input JSONL records."
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=Path("input/sample_100.jsonl"),
        help="Input JSONL file to evaluate",
    )
    parser.add_argument(
        "--prompts-dir",
        type=Path,
        default=Path("prompts"),
        help="Directory containing versioned prompt files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where check outputs will be written",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Optional global model spec (e.g. openai:gpt-4o-mini)",
    )
    parser.add_argument(
        "--semantic-model",
        type=str,
        default=DEFAULT_MODEL_ROUTING["semantic_accuracy"],
        help="Model spec for semantic accuracy check",
    )
    parser.add_argument(
        "--entity-model",
        type=str,
        default=DEFAULT_MODEL_ROUTING["entity_resolution"],
        help="Model spec for entity resolution check",
    )
    parser.add_argument(
        "--source-model",
        type=str,
        default=DEFAULT_MODEL_ROUTING["source_credibility"],
        help="Model spec for source credibility check",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional cap for debugging runs",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Records per API call per check (higher = fewer calls, larger prompts)",
    )
    parser.add_argument(
        "--max-body-chars",
        type=int,
        default=1500,
        help="Truncate source_body to this many chars to reduce token cost",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between API calls",
    )
    parser.add_argument(
        "--llm-log-file",
        type=Path,
        default=Path("logs/llm_calls.jsonl"),
        help="Append-only audit log for each LLM call",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional run identifier (defaults to UTC timestamp)",
    )
    args = parser.parse_args()

    if not args.input_file.exists():
        raise FileNotFoundError(f"Input file not found: {args.input_file}")
    if args.max_records is not None and args.max_records <= 0:
        raise ValueError("--max-records must be positive if provided")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    if args.max_body_chars < 0:
        raise ValueError("--max-body-chars cannot be negative")
    if args.sleep_seconds < 0:
        raise ValueError("--sleep-seconds cannot be negative")

    model_routing = {
        "semantic_accuracy": args.semantic_model,
        "entity_resolution": args.entity_model,
        "source_credibility": args.source_model,
    }
    if args.model:
        model_routing = {check_name: args.model for check_name in PROMPT_FILES}

    for spec in model_routing.values():
        parse_model_spec(spec)

    run_checks(
        input_file=args.input_file,
        prompts_dir=args.prompts_dir,
        output_dir=args.output_dir,
        model_routing=model_routing,
        batch_size=args.batch_size,
        max_body_chars=args.max_body_chars,
        llm_log_file=args.llm_log_file,
        run_id=args.run_id,
        max_records=args.max_records,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()
