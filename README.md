# News Events Quality Pipeline

End-to-end data quality workflow for extracted news-event records:

1. Sample records from large JSONL datasets.
2. Run deterministic triage + LLM quality checks.
3. Generate remediation suggestions for failed checks.
4. Persist artifacts to SQLite.
5. Review in a unified Streamlit dashboard (overview, human review, version comparison).

## Repository Structure

- `skills/`: reusable workflow skills (`SKILL.md` files).
- `prompts/`: versioned LLM prompts (`*_v1.txt`, `*_v2.txt`, ...).
- `scripts/`: pipeline runners, loaders, dashboard app.
- `sql/`: DDL schema for SQLite storage.
- `evals/`: labelled sets + eval result artifacts.

## Setup

1. Create environment (optional) and install dependencies:

```bash
pip install streamlit pandas altair
```

2. Set API key:

```bash
export OPENAI_API_KEY="your_key_here"
```

## Step-by-Step Run

```bash
# 1) Sample 100 random records
python3 scripts/sample_data.py --input-dir Datasets-2025-08-08 --output-file data/sample_100.jsonl --sample-size 100

# 2) Copy sample to input
cp data/sample_100.jsonl input/sample_100.jsonl

# 3) Run quality checks (auto-picks latest prompt versions)
python3 scripts/run_quality_checks.py --input-file input/sample_100.jsonl --output-dir output --batch-size 20 --max-body-chars 0

# 4) Run remediation on failed rows
python3 scripts/run_remediation.py --input-dir output --output-dir remediations --model gpt-4o-mini

# 5) Initialize DB schema
python3 scripts/init_quality_db.py --db-path quality_checks.db

# 6) Load artifacts into SQLite
python3 scripts/load_results_to_sqlite.py --db-path quality_checks.db --output-dir output --remediations-dir remediations --llm-log-file logs/llm_calls.jsonl

# 7) Compare prompt versions
python3 scripts/compare_prompt_runs.py --output-dir output --base-version v1 --candidate-version v2

# 8) Launch unified dashboard
streamlit run scripts/dashboard.py
```

## Reproducibility Notes

- Every quality run writes `run_id`, `prompt_version`, `model`, and timestamp into outputs.
- LLM call audit is appended to `logs/llm_calls.jsonl`.
- Version drift can be compared by prompt version and event ID.

## Storage Schema

DDL is in:

- `sql/quality_schema.sql`

Main entities:

- `records`
- `triage_results`
- `llm_quality_results`
- `llm_remediation_results`
- `llm_call_audit`
- `human_review_labels`
