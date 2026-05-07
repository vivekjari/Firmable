# Skill: news-events-quality-check

## Purpose
Run LLM-based quality checks on structured event records using only provided input data, while preserving the caller's existing input/output contracts.

Version: `v1`
Scope: `quality-evaluation-only`
Status: `active`

## Core Principle
This skill defines **execution behavior**, not data-model changes.

- Do not invent or alter output schemas.
- Do not rename fields or reshape files unless explicitly requested by the caller.
- Reuse the existing pipeline contract already defined in the repository/project.

## When To Trigger
Use this skill when you need one or more of:
- Semantic label validation.
- Entity-link plausibility validation.
- Source credibility/relevance validation.
- Repeatable quality checks for records in batch.

Do not use this skill when:
- The task is deterministic schema validation only.
- The task is to propose fixes (use `news-events-remediation` separately).

## Required Inputs
- Input dataset.
- Prompt definitions for the target checks.
- Existing output destination/format expected by the current pipeline.
- Model routing and runtime parameters (as applicable in the host project).

## Data Boundaries (Generalized Guardrail)
- Use only data provided in input records and related local context files.
- Do not use web lookups, external enrichment, or out-of-band sources unless explicitly requested.
- Keep evaluation evidence grounded in provided fields (for example: sentence, title, body, linked entities).

## Standard Check Set
1. Semantic accuracy
   - Does record text support labeled event type?
2. Entity resolution validation
   - Are linked entities plausible and role-consistent?
3. Source credibility & relevance
   - Is this a credible event signal vs marketing/noise/duplicate-like content?

## Prompt/Eval Dependencies
This skill should reference the prompt specs configured by the host project. Typical mapping:
- `semantic_accuracy` -> semantic prompt file
- `entity_resolution` -> entity-resolution prompt file
- `source_credibility` -> source-quality prompt file

## Output Expectations
- Keep outputs **separate by check** if that is how the host pipeline is designed.
- Preserve 1:1 traceability to source rows (for example via `event_id` or equivalent record key).
- Preserve existing output structure exactly as defined by the caller's pipeline.
- Include pass/fail, reason, and confidence in the structure already used by the project.

## Interpretation Guidance
- High-confidence fail: prioritize for remediation queue.
- Low-confidence fail: queue for human review.
- Pass with low confidence: optional spot-check depending on risk tolerance.

## Acceptance Criteria
- All input rows intended for evaluation are processed or explicitly marked with a failure reason.
- No external data leakage into judgments.
- No schema drift in output artifacts.
