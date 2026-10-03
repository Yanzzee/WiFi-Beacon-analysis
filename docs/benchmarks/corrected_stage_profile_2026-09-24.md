# Corrected output narrow CPU/allocation profile — 2026-09-24

## Purpose and scope

This profile follows the initial
[five-artifact boundary baseline](corrected_five_artifact_2026-09-24.md). It
locates work inside the two expensive corrected-output stages without changing
production calculations or output. The diagnostic harness is
[`profile_corrected_outputs.py`](../../profile_corrected_outputs.py).

Status: the recommended single-sort/single-pass ADR-0004 change was approved,
implemented, and measured later on 2026-09-24. The original evidence and the
post-change comparison are both retained below.

The profile separates four phases that the public writers currently perform:

1. selected-observation validation and logical-table construction, including
   its final Arrow schema check;
2. selected-observation date partitioning, Arrow conversion, and Parquet write;
3. ADR-0004 radio/class and classroom/class summary calculation; and
4. summary Arrow conversion and Parquet write.

Capture extraction, vendor normalization, identity resolution, timing/beacon
selection, and the other run-lifecycle artifacts remain outside this narrow
profile. The write-only phases reproduce the existing production writer logic
after its logical build; they are not alternate production implementations.

## Workloads and method

Two separate runs prevent measurement tools from contaminating each other:

| Run | Input | Purpose |
|---|---:|---|
| CPU | 2,930 bins, 12 identities, 33,944 selected rows | `cProfile` call tree on the established 50-minute, 1.024-second workload |
| Allocation | 293 bins, 12 identities, 3,392 selected rows | `tracemalloc` Python peak/retained allocations plus sampled process RSS on a five-minute workload |

The CPU profiler adds substantial absolute overhead. Its times are useful for
attribution, not as replacement wall-time baselines. The uninstrumented
five-artifact benchmark remains the source for ordinary elapsed time. The raw
reports are
[`corrected_profile_cpu_2026-09-24.json`](corrected_profile_cpu_2026-09-24.json)
and
[`corrected_profile_small_2026-09-24.json`](corrected_profile_small_2026-09-24.json).

All four phases retained the expected row counts and produced readable selected
observations and both summary Parquet files.

## CPU evidence

| Phase | Profiled wall s | Share of four profiled phases | Main evidence |
|---|---:|---:|---|
| Selected validation/logical build | 4.895 | 13.9% | 2,036,640 missing-value checks; full row materialization and Arrow schema check |
| Selected partition/Arrow/Parquet | 1.886 | 5.3% | another full partition row materialization and Arrow conversion; Parquet write itself was about 0.014 s |
| ADR-0004 summary calculation | 28.489 | 80.8% | 25.360 s cumulative in `_radio_row`; 8,491 `DataFrame.sort_values` calls |
| Summary Arrow/Parquet | 0.010 | <0.1% | only three radio rows and one classroom row were serialized |

The summary result is specific and actionable. `_radio_row` currently sorts
each small per-bin candidate DataFrame by the complete deterministic observation
order. For this workload that produces approximately one sort for every
radio/bin containing observations. The 8,491 sorts spend 17.899 seconds of
cumulative profiled time in `DataFrame.sort_values`; categorical construction,
lexicographic indexing, and factorization dominate below it. This is summary
calculation cost, not Parquet I/O.

The selected-observation path has a different shape. It validates each value,
constructs Python record dictionaries, constructs a DataFrame, converts that
DataFrame back to records for the Arrow contract check, and the writer converts
the date partition to records again. `_is_missing` was called 2,036,640 times
during logical construction and 1,052,264 times during the write-only phase.
The evidence shows repeated row-wise conversion/checking rather than expensive
Parquet I/O.

## Allocation evidence

Allocation tracing materially slows execution, so only its memory values—not
its elapsed times—are used here.

| Phase | Peak traced Python MiB | Incremental sampled RSS MiB | Python MiB retained at phase end |
|---|---:|---:|---:|
| Selected validation/logical build | 14.71 | 26.32 | 1.05 |
| Selected partition/Arrow/Parquet | 9.54 | 10.38 | 0.06 |
| ADR-0004 summary calculation | 7.12 | 8.00 | 0.33 |
| Summary Arrow/Parquet | 0.04 | 0.16 | 0.02 |

The difference between peak and retained Python memory shows that most of the
working allocation is transient. The selected-observation phases are the
larger allocation concern even though the summary calculation is the CPU
concern. End-minus-start Arrow allocator deltas were zero because temporary
tables were released before each phase returned; that value does not prove
that Arrow made no transient allocation.

## Resultant recommendations

1. **Implemented: simplify ADR-0004 radio summarization.**
   Preserve every accepted metric and tie-break rule, but order each
   radio/session observation set once and derive its per-bin states in one
   pass. Avoid a new DataFrame sort and chained filtering for every bin. This
   was the narrowest change against the dominant CPU cost.
2. **Completed: re-run the oracle tests, five-artifact boundary benchmark, and
   this profile.** The accepted radio and classroom summary results remain
   unchanged and the repeated per-bin sort pattern is gone.
3. **Treat selected-observation memory as the second target.** If memory still
   needs improvement, design one typed conversion path so validation does not
   repeatedly materialize DataFrame → records → normalized records → Arrow.
   Preserve the exact durable schema, one-row grain, deterministic ordering,
   date partitions, and atomic publication.
4. **Do not add chunking, spill storage, concurrency, a different file format,
   or changed bin semantics from this evidence.** Those are broader policies;
   the current profile first supports two local behavior-preserving internal
   improvements.

## Implemented result

The production summary calculation now performs one stable sort per
radio/session, then consumes its ordered observations alongside the ordered
class bins. It establishes the preferred BSSID when it encounters the first
valid CU observation and retains the same fallback and deterministic ordering
rules. No metric, schema, partition, or artifact changed.

| Measurement | Before | After | Change |
|---|---:|---:|---:|
| Median complete five-artifact wall time | 17.010 s | 4.761 s | 72.0% lower; 3.57× faster |
| Median complete-boundary CPU time | 17.484 s | 4.719 s | 73.0% lower |
| Median ADR-0004 calculation/write stage | 13.603 s | 1.105 s | 91.9% lower; 12.3× faster |
| Profiled ADR-0004 calculation | 28.489 s | 3.090 s | 89.2% lower |
| Profiled `_radio_row` cumulative time | 25.360 s | 0.181 s | 99.3% lower |
| Published run bytes | 396,579 | 396,579 | unchanged |

The three-run after-result is
[`corrected_five_artifact_optimized_2026-09-24.json`](corrected_five_artifact_optimized_2026-09-24.json).
The after CPU and allocation profiles are
[`corrected_profile_cpu_optimized_2026-09-24.json`](corrected_profile_cpu_optimized_2026-09-24.json)
and
[`corrected_profile_small_optimized_2026-09-24.json`](corrected_profile_small_optimized_2026-09-24.json).

The small allocation run did not materially change summary memory: traced
Python peak remained about 7.12 MiB and incremental RSS changed from 8.00 to
about 7.53 MiB. This confirms that the implemented change removed CPU work and
was not a memory-policy change. The selected-observation stage then became the
largest part of the corrected boundary.

## Selected-observation Arrow reuse result

The owner subsequently approved the narrow selected-observation consolidation.
Validation still constructs the same logical rows and exact Arrow schema, but
the writer now retains that validated Arrow table instead of converting the
logical DataFrame back to records. Because logical rows are already ordered by
`class_date`, each Hive date partition is a zero-copy contiguous Arrow slice.
The public DataFrame API, validation rules, row order, schema, compression,
partition paths, and atomic publication behavior are unchanged.

| Measurement | Before reuse | After reuse | Change |
|---|---:|---:|---:|
| Median selected-observation stage | 3.629 s | 2.139 s | 41.1% lower |
| Profiled write-only phase | 1.855 s | 0.080 s | 95.7% lower |
| Write-only peak traced Python allocation, five-minute workload | 9.55 MiB | 0.01 MiB | effectively eliminated |
| Write-only incremental RSS, five-minute workload | 11.13 MiB | 2.91 MiB | 73.9% lower |
| Median complete-boundary incremental RSS | 120.0 MiB | 101.1 MiB | 15.7% lower |
| Published selected-observation bytes | 371,334 | 371,334 | unchanged |
| Published complete-run bytes | 396,579 | 396,579 | unchanged |

The repeated after-reuse boundary run had a 5.351-second median total wall
time. It is not compared directly with the earlier 4.761-second total because
the unchanged ADR-0004 stage ran at a different machine-performance level in
the later run (3.180 rather than 1.105 seconds). The isolated selected-writer
and allocation measurements show the intended improvement; there is no output
or summary-semantic change.

The after-reuse evidence is
[`corrected_five_artifact_arrow_reuse_2026-09-24.json`](corrected_five_artifact_arrow_reuse_2026-09-24.json),
[`corrected_profile_cpu_arrow_reuse_2026-09-24.json`](corrected_profile_cpu_arrow_reuse_2026-09-24.json),
and
[`corrected_profile_small_arrow_reuse_2026-09-24.json`](corrected_profile_small_arrow_reuse_2026-09-24.json).

The initial validation/logical-build phase is now the remaining selected-
observation cost. Replacing its explicit per-field validation with a vectorized
model would be a broader internal change and is not authorized by this result.
The corrected boundary is ready to return to capture-to-run orchestration
unless representative workloads later establish a need for more optimization.

## Reproduction

```powershell
.\.venv\Scripts\python.exe profile_corrected_outputs.py `
  --workload profile_small_5min --allocations `
  --output docs\benchmarks\corrected_profile_small_2026-09-24.json

.\.venv\Scripts\python.exe profile_corrected_outputs.py `
  --workload one_class_1024ms `
  --output docs\benchmarks\corrected_profile_cpu_2026-09-24.json
```
