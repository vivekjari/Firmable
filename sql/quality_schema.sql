PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS records (
  event_id TEXT PRIMARY KEY,
  source_record_json TEXT NOT NULL,
  first_seen_at TEXT,
  last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS triage_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  created_at TEXT,
  event_id TEXT,
  decision TEXT NOT NULL,
  reason TEXT,
  FOREIGN KEY (event_id) REFERENCES records(event_id)
);

CREATE TABLE IF NOT EXISTS llm_quality_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  created_at TEXT,
  check_name TEXT NOT NULL,
  prompt_version TEXT,
  prompt_file TEXT,
  model TEXT,
  event_id TEXT NOT NULL,
  result TEXT,
  confidence REAL,
  reason TEXT,
  llm_check_json TEXT,
  llm_metrics_json TEXT,
  source_record_json TEXT,
  FOREIGN KEY (event_id) REFERENCES records(event_id)
);

CREATE INDEX IF NOT EXISTS idx_quality_run_check ON llm_quality_results(run_id, check_name);
CREATE INDEX IF NOT EXISTS idx_quality_event ON llm_quality_results(event_id);
CREATE INDEX IF NOT EXISTS idx_quality_result ON llm_quality_results(result);

CREATE TABLE IF NOT EXISTS llm_remediation_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT,
  created_at TEXT,
  check_name TEXT NOT NULL,
  model TEXT,
  event_id TEXT,
  failed_check_json TEXT,
  remediation_json TEXT,
  llm_metrics_json TEXT,
  source_record_json TEXT,
  FOREIGN KEY (event_id) REFERENCES records(event_id)
);

CREATE INDEX IF NOT EXISTS idx_remediation_run_check ON llm_remediation_results(run_id, check_name);
CREATE INDEX IF NOT EXISTS idx_remediation_event ON llm_remediation_results(event_id);

CREATE TABLE IF NOT EXISTS llm_call_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  stage TEXT,
  run_id TEXT,
  check_name TEXT,
  model TEXT,
  provider TEXT,
  prompt_file TEXT,
  prompt_version TEXT,
  latency_ms REAL,
  cost_usd REAL,
  usage_json TEXT,
  decision_json TEXT,
  request_json TEXT,
  response_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_stage_check ON llm_call_audit(stage, check_name);
CREATE INDEX IF NOT EXISTS idx_audit_run ON llm_call_audit(run_id);

CREATE VIEW IF NOT EXISTS human_review_backlog AS
SELECT q.event_id,
       q.run_id,
       q.check_name,
       q.result,
       q.confidence,
       q.reason,
       q.created_at
FROM llm_quality_results q
WHERE q.result = 'fail';
