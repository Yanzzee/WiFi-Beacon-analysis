# Potentially affected station reporting validation

Date: 2026-10-16. Scope: ADR-0016 synthetic semantics, capture-run integration,
cached full-project reporting and visual review. No full capture decode or
published-run rewrite was performed.

## Cached full-project evidence

Source: immutable `2025-full` run `20261014T160532618192Z-3152b7cd`.
The cached check used the existing radio/bin Parquet, infrastructure exports
and physical class-session context. One session row group was read at a time.

- Radio/bin input rows: 2,847,339 across 373 physical sessions.
- Radio/session rows checked: 691.
- New entity/session rows, including overlapping scope/band views: 2,507.
- Existing radio duration, valid/missing/no-beacon CU duration, station
  availability, positive-plus-zero pair duration and paired station-time
  components match all 691 existing band-comparison radio/session rows.
- Disjoint below-high/high-only/very-high station-time components sum exactly
  to paired station-time. Project summaries use radio rows only.

| Measurement | Observed value |
|---|---:|
| Narrow cached-source read | 1.11 s |
| Accumulation | 21.54 s |
| Final tables, graphics and metadata | 0.54 s |
| Seven-file package | 2,191,847 bytes (about 2.09 MiB) |

These are cached checks, not isolated incremental full-capture runtime.
Production shares already-loaded session parts and adds no decode/read pass.
Preview outputs are separate from published runs under
`output/previews/affected_stations_20261016/`. Plot rendering refinement does
not change logical table values; the timings are representative, not an SLA.

## Test and presentation evidence

Regression: 366 passed, 16 external-tool/short-capture tests deselected in
99.76 seconds. Eleven new focused tests cover exact threshold/terminal-time
weighting, stale assistance, missing/zero distinctions, complete-only concurrent
peaks, unequal station-time pooling, sampled 2.4 GHz, mixed infrastructure,
empty sessions, deterministic order, overflow-safe products, create-only writes
and failure cleanup. Capture-run tests verify manifest entries and summary
integration. Compilation and whitespace checks pass.

Logical tables are frozen in `tests/golden/affected_stations_v1.json`; existing
corrected/report goldens are unchanged. PNG bytes are not regression targets.
The two full-project graphics were visually inspected. Exposure bars share a
zero-based scale across thresholds; peak CDFs retain exact discrete counts.

## Remaining checkpoints

Owner review of representative outputs and complete-run runtime/storage remain.
Potentially affected associations are not unique clients or proven impairment.
Strict complete-bin peaks can miss a true peak; inspect complete-bin coverage
separately from exposure pair coverage. Reporting organization and a lightweight
run overview are the recommended next presentation checkpoint.
