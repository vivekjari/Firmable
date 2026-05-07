# Skill: news-events-remediation

## Purpose
Propose remediation recommendations for records flagged by quality checks, as a **separate post-processing step**.

Version: `v1`
Scope: `recommendation-only`
Status: `active`

## Separation Of Concerns
This skill is independent from quality-check execution:
- Run `news-events-quality-check` first to generate flagged outputs.
- Run this skill afterward on those outputs to produce recommendations.
- Do not modify quality-check output files in place.
- Do not re-grade whether the LLM check was correct; use failures as input signals to generate a candidate fix for the raw record.

## When To Trigger
Use this skill when:
- One or more records are flagged by quality checks.
- You need recommended fixes plus reasoning/evidence for reviewer approval.

Do not use this skill when:
- You are still generating quality-check scores.
- You need deterministic structural validation only.

## Required Inputs
- Original source record (or source reference ID).
- Associated failed check result(s) from quality output files.
- Optional policy constraints (confidence threshold, allowed actions, reviewer rules).

## Optional Inputs
- Controlled taxonomy for allowed labels.
- Known entity reference catalog for relinking.
- Duplicate context or source-priority rules.

## Data Boundaries
- Use only source record data + quality-check outputs supplied by the pipeline.
- Do not use web/external sources unless explicitly requested.
- Do not hallucinate corrections not supported by provided evidence.

## Expected Remediation Decisions
Typical decision set:
- `correct_label`
- `correct_entity_link`
- `mark_low_credibility`
- `no_change`

## Raw-Data Fix Requirement
For each flagged record, propose a concrete correction against the **raw record fields** (not against the quality-check output), for example:
- `attributes.category`
- `attributes.summary`
- `relationships.*.data.id`
- `meta.review_status` or equivalent pipeline field

The recommendation must include:
- current value (from raw record),
- proposed new value,
- why the proposed value better matches evidence.

## Output Expectations
Produce recommendations in a separate artifact/output stream defined by the host project.
Each recommendation should include:
- Record identifier (for traceability),
- Proposed action,
- Proposed raw-record field-level patch (or equivalent),
- Reasoning,
- Confidence,
- Evidence spans (where possible),
- Reviewer recommendation flag.

Important:
- Keep original source and quality output immutable.
- Recommendation schema should match host-project contract; this skill should not redefine it.
- Focus on "how to fix the datapoint" rather than "whether the prior LLM verdict was right."

## Interpretation Guidance
- High-confidence actionable suggestion: queue for reviewer approval.
- Medium-confidence suggestion: manual validation required.
- Low-confidence/ambiguous: prefer `no_change` + escalation.

## Acceptance Criteria
- Every flagged record processed by this skill gets exactly one recommendation outcome.
- Recommendations are explainable and evidence-linked.
- No direct mutation of source data or quality-check outputs.
