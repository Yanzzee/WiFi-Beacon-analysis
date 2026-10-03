# Corrected five-artifact boundary baseline — 2026-09-24

## Scope

This is the first observed resource baseline for the complete corrected-output
calculation and publication boundary. It starts with an expected class-bin
grid, stable project identity map, and already selected per-BSSID/bin
observations in memory. It includes:

- corrected-run lifecycle creation and small synthetic input-descriptor hashes;
- project identity-map Parquet writing;
- partitioned selected-observation Parquet writing;
- ADR-0004 radio/class and classroom/class calculation and Parquet writing;
- manifest writing; and
- atomic whole-run publication.

It excludes capture extraction, representative raw-capture hashing, vendor
normalization, AP/radio resolution, per-BSSID timing/selection, plots, CSV
exports, and interface latency. Synthetic input construction is timed
separately and is outside the reported boundary time and incremental-memory
measurement.

The harness is
[`benchmark_corrected_outputs.py`](../../benchmark_corrected_outputs.py). Each
reported run uses a fresh Python worker process. It records wall and CPU time,
baseline/peak/incremental RSS, process I/O, peak output-tree size, published
artifact sizes, row counts, stage durations, and schema/metric/artifact
correctness. `psutil` is optional; memory and process-I/O values are null when
it is unavailable.

## Environment

| Item | Observed value |
|---|---|
| Operating system | Windows 11, build 10.0.26200, 64-bit |
| Python | 3.13.14 |
| Logical CPUs | 24 |
| Visible memory | 68,401,475,584 bytes (approximately 63.7 GiB) |
| Corrected metric | `beacon.metric/corrected-cu/v1` |
| Result schema | `beacon.schema/analysis-results/v1` |

## Synthetic workloads

Every workload represents one classroom with three distinct 2.4/5/6 GHz
radios and four BSSIDs per radio. Classes are 50 minutes. Deterministic patterns
produce measured zero and other valid CU values, missing CU, no-beacon bins,
and preferred-BSSID fallback. No observation is synthesized by the summary
calculation.

| Workload | Repetitions | Bin width | Classes | Expected bins | Selected rows |
|---|---:|---:|---:|---:|---:|
| `one_class_1024ms` | 3 | 1.024 s | 1 | 2,930 | 33,944 |
| `ten_classes_1024ms` | 1 exploratory | 1.024 s | 10 | 29,300 | 339,440 |
| `one_class_102ms_projection` | 1 exploratory | 0.1024 s | 1 | 29,297 | 339,440 |

The one-class 1.024-second result is the initial repeated baseline. The other
two results are single-run scaling observations and must not be treated as
stable medians.

## Results

For the repeated workload, wall/CPU/input/RSS/output values below are medians.
Incremental RSS is peak process RSS minus RSS after the input DataFrames were
built and immediately before the measured boundary began.

| Workload | Input DataFrames MiB | Wall s | CPU s | Incremental peak RSS MiB | Absolute peak RSS MiB | Published MiB | µs/selected row |
|---|---:|---:|---:|---:|---:|---:|---:|
| `one_class_1024ms` | 34.92 | 17.01 | 17.48 | 142.90 | 252.66 | 0.38 | 501.13 |
| `ten_classes_1024ms` | 333.90 | 179.03 | 199.63 | 1,639.88 | 1,912.51 | 3.50 | 527.43 |
| `one_class_102ms_projection` | 349.43 | 179.16 | 192.61 | 1,643.38 | 1,920.52 | 5.69 | 527.80 |

The three repeated `one_class_1024ms` runs observed:

- wall time from 16.98 to 17.17 seconds;
- CPU time from 17.25 to 17.56 seconds;
- incremental peak RSS from 139.9 to 161.5 MiB;
- identical 396,579-byte published runs; and
- identical 397,171-byte process-write measurements.

A subsequent one-run stage diagnostic for the same small workload measured:

| Stage | Wall time | Share of 17.27 s boundary |
|---|---:|---:|
| Project identity-map write | 0.008 s | <0.1% |
| Selected-observation validation/write | 3.640 s | 21.1% |
| ADR-0004 summary calculation/write | 13.603 s | 78.8% |
| Remaining lifecycle/manifest/publication | 0.019 s | 0.1% |

All runs published exactly the four accepted tables plus the manifest, used
the corrected schema/metric IDs, and recorded the three expected writer stages.

## Interpretation

- The approximately equal-row ten-class and 102.4-millisecond workloads have
  nearly identical time and memory. Selected-row volume dominates this
  boundary more than whether those rows are spread across classes or bins.
- Scaling from 33,944 to 339,440 selected rows was broadly linear in time, but
  incremental peak RSS grew from about 143 MiB to about 1.60 GiB. The current
  all-in-memory implementation is therefore expensive well before durable
  output size becomes large.
- The small-workload stage diagnostic attributes most wall time to ADR-0004
  summary calculation/write. It does not yet identify the specific allocation
  or operation responsible; an allocation/CPU profile is required before any
  optimization is selected.
- The published results are compact relative to working memory. The ten-class
  run published 3.50 MiB, and the equal-row 102.4-millisecond projection
  published 5.69 MiB.
- These observations do not establish release budgets, supported maximums, a
  need for chunk/spill storage, or acceptance of 102.4-millisecond production
  bins. They also do not predict capture-to-run performance.

The follow-up
[narrow CPU/allocation profile](corrected_stage_profile_2026-09-24.md) is now
complete. It identifies repeated per-bin sorting as the dominant ADR-0004 CPU
cost and repeated row materialization as the selected-observation allocation
and secondary CPU cost. The approved single-sort/single-pass ADR-0004 change
was subsequently implemented: median complete-boundary wall time fell from
17.01 to 4.76 seconds, the ADR-0004 stage fell from 13.60 to 1.10 seconds, and
published bytes remained identical. Broader changes such as chunking, spill
storage, concurrency, or a different table-processing model still require
separate high-level confirmation.

The later approved selected-observation Arrow reuse is also recorded in that
profile. It reduced the median selected-observation stage from 3.63 to 2.14
seconds and median complete-boundary incremental RSS from about 120 to 101 MiB
without changing the schema, partition layout, or published byte counts.

## Reproduction

Run from the repository root. The first command is the repeated baseline; the
larger workloads are intentionally single-run exploratory measurements:

```powershell
.\.venv\Scripts\python.exe benchmark_corrected_outputs.py `
  --workload one_class_1024ms --repeats 3

.\.venv\Scripts\python.exe benchmark_corrected_outputs.py `
  --workload ten_classes_1024ms --repeats 1

.\.venv\Scripts\python.exe benchmark_corrected_outputs.py `
  --workload one_class_102ms_projection --repeats 1
```

JSON is written to standard output. Use `--output <path>` for an explicit
result file.
