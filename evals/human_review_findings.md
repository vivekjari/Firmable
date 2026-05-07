# Human Review Findings (Per Check)

This evaluation uses hand-labelled examples from the human review queue for each check.

## Ground Truth Definition

- **Ground-truth FAIL**: `approve_fail`, `needs_changes`, `reject_record`
- **Ground-truth PASS**: `override_pass`
- **Model prediction** in this reviewed set: all rows were model-flagged `fail` (queue-based review set).

## Results by Check

| Check | Reviewed (N) | TP | FP | FN | TN | Precision | Recall | Accuracy |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| semantic_accuracy | 32 | 23 | 9 | 0 | 0 | 0.719 | 1.000 | 0.719 |
| entity_resolution | 24 | 10 | 14 | 0 | 0 | 0.417 | 1.000 | 0.417 |
| source_credibility | 27 | 0 | 27 | 0 | 0 | 0.000 | n/a* | 0.000 |

## Interpretation

- `semantic_accuracy` shows the strongest precision among reviewed failures.
- `entity_resolution` has high false-positive pressure (many human overrides).
- `source_credibility` currently over-flags in this reviewed sample.

## Important Method Note

This is a **review-queue-only** evaluation (mostly/only model-failed cases), not a random full-population sample.
So:
- Precision is meaningful for fail-queue quality.
- Recall/accuracy are conditional to this queue and should be interpreted with caution.
- For full defendable judge metrics, include both predicted pass and fail examples in the hand-labelled eval set.

## Next Eval Improvement

Build balanced hand-labelled sets per check with both model-pass and model-fail examples, then recompute precision/recall/accuracy on that combined set.

---
*`source_credibility` recall is undefined in this slice because there were no ground-truth fail positives in the reviewed sample.*
