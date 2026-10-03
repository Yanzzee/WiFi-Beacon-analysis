# Bounded capture-stage worker benchmark — 2026-09-29

This benchmark measures the corrected pipeline after two performance changes:

- TShark emits only beacons whose reviewed Aruba or Extreme AP-name field
  matches a configured project hostname; and
- each worker owns extraction, normalization, adapter dispatch, project-scope
  validation, and hidden class-session staging before releasing its
  capture-sized DataFrame.

The machine has an Intel Core i9-12900, 24 logical CPUs, 64 GiB RAM, NVMe
storage, and TShark 4.6.6. The input is eight real Aruba ERM captures from
2025-11-10 totaling 4,304,389,268 bytes. The benchmark uses the current
five-AP acceptance mapping and 14 class sessions for that date.

## Results

| Workers | End-to-end wall | Speedup vs. 1 | Peak process-tree RSS | Sampled CPU | Peak hidden workspace |
|---:|---:|---:|---:|---:|---:|
| 1 | 604.24 s | 1.000x | 1.096 GiB | 604.78 s | 20.98 MiB |
| 2 | 379.43 s | 1.593x | 1.244 GiB | 604.30 s | 20.98 MiB |
| 4 | 274.73 s | 2.199x | 1.642 GiB | 605.73 s | 20.98 MiB |
| 8 | 245.07 s | 2.466x | 2.240 GiB | 633.59 s | 20.98 MiB |

Moving from one to two workers reduced wall time by 37.2%. Moving from two to
four reduced it by another 27.6%. Moving from four to eight reduced it by only
10.8%, while sampled peak RSS increased by about 36% from the four-worker
level. This indicates increasing CPU/storage contention and a substantial
serial remainder after capture staging.

All four logical artifact fingerprints were identical after excluding the
run ID:

- 34 project-identity rows;
- 376,633 selected observations;
- 55 radio/class rows; and
- 14 classroom/class rows.

The generated JSON evidence is
[`capture_workers_bounded_2026-09-29.json`](capture_workers_bounded_2026-09-29.json).
The harness is `benchmark_capture_workers.py` and now accepts an explicit
`--workers` list.

## Scope limitation

This is a concurrency and determinism measurement, not a representative
production-memory claim. The five-AP test scope was intentionally restricted
and historically discarded about 93% of supported rows. A normal project is
expected to discard closer to 40%, so workers may retain and stage far more
rows concurrently. TShark must also decode the complete capture even when its
display filter emits fewer rows.

The source filter therefore remains useful but is not treated as the primary
scaling solution. Exact missing-name and outside-project counts are no longer
calculated in production filtered runs. Exact in-scope rows still enter the
same BSSID/session TSF duplicate handling and deterministic one-per-bin
selection.

## Recommendation

Keep one worker as the conservative default until a representative AP mapping
is measured. Four workers is the best current opt-in recommendation: it gives
most of the observed benefit with materially less memory than eight. Eight
remains available for testing, but its marginal gain is small on this machine.
Do not test or recommend sixteen yet; first repeat 1/2/4/8 with a workload near
the expected 60% retained scope and review peak memory and disk behavior.
