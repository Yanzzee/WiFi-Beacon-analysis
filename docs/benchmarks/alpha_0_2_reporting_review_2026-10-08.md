# Alpha 0.2 reporting validation and storage review

Review date: 2026-10-08. This record consolidates existing complete-run evidence
and a current report-only regeneration. It does not claim controlled system
load, a runtime SLA, peak-memory measurements, or owner approval of all outputs.

## Representative source

Project: `2025-full`, schedule dates 2025-11-10 through 2025-11-14,
`America/Denver`, 1.024-second analysis bins, primary CU SSID `eduroam`.

Both complete runs below have input-set SHA-256
`3152b7cd1f057b9670de6bbc8a3f15c56e8e9d2e3fd4eb34fcc6ecbccdf835ce`.
The project contains 80 captures, 21 configured APs, 373 physical sessions,
6,511,028 selected observations, 2,847,339 radio/bin detail rows, and
11,336,578 station-rollup rows. The optional collection has 691 radio/session
plots. Both runs contain the CU, station, class-session, and radio-detail
packages, with all manifest stages marked completed.

## Existing complete-run timings

Manifest stage times are end minus start. These are costs of their named
stages, not isolated costs of each individual calculation.

| Observation | `20261004T165518132804Z-3152b7cd` | `20261004T202139962640Z-3152b7cd` |
|---|---:|---:|
| Extraction workers | 4 | 8 |
| Elapsed through report generation | 2:23:53 | 2:18:51 |
| Extraction/normalization/staging | 5,781.773 s | 5,523.049 s |
| Session selection, including derived station/radio-bin construction | 1,930.932 s | 1,922.241 s |
| Selected-observation artifact write | 491.184 s | 485.487 s |
| Radio/bin report write, correlations, and all optional plots | 279.238 s | 249.808 s |
| Station report package write | 148.279 s | 148.436 s |
| Class-session report package write | 0.244 s | 0.415 s |
| CU report package write | 1.461 s | 1.850 s |
| Radio plot dimensions | 1680 x 700 px | 3360 x 1400 px |

The later run already uses doubled graphics dimensions. The worker count and
runtime conditions also differ, so the elapsed-time difference does not isolate
the cost of larger figures or demonstrate an eight-worker performance gain.
Station supplementation and construction are part of session selection;
the station report write timing does not measure those calculations again.

## Existing complete-run storage

Package sizes are recursive published file sizes, not allocated filesystem
space. One MiB is 1,048,576 bytes. PNG compression means doubled width and
height do not imply four times the file size.

| Package | Earlier run, bytes | Later run, bytes |
|---|---:|---:|
| Class sessions | 311,241 | 358,949 |
| CU coverage/streak | 1,039,781 | 1,241,644 |
| Radio detail including 691 plots | 161,132,589 | 292,895,009 |
| Station count | 31,855,430 | 31,897,832 |

The later radio-detail package is about 279.326 MiB, including 235.090 MiB of
optional plot PNGs. The radio/bin Parquet itself is 44,772,558 bytes, about
42.699 MiB. Station reporting occupies about 30.420 MiB.

## Current report regeneration

The review reconstructs reports from the later run's published summaries,
radio/bin detail, station rollups, and class-session context. It reads Parquet
one existing physical-session row group at a time for the two large derived
tables. Their largest recorded per-session parts are 38,676 radio/bin rows
and 79,688 station-rollup rows.

The currently present raw sample CSV byte hashes differ from the source
manifest. They are therefore not substituted into this review. Schedule
context comes from the published class-session report. AP context comes from
the published identity map, after confirming that all 21 configured APs are
represented and each AP has a single classroom/model tuple. Parquet array
values are restored to Python lists for the class-session reconstruction so
CSV formatting matches the original in-memory summary inputs.

Outputs are written separately under
`output/previews/alpha_0_2_review_20261008/`; source run files are unchanged.
These are review exports rather than another atomically published capture run.
The local one-time script is `review.py` in that directory. The successful
review's `measurements.json` records exact elapsed seconds, bytes, output
counts, and source-manifest hash.

Successful review directory:
`output/previews/alpha_0_2_review_20261008/20261008T165046Z/`.
Its `default/reports/` directory contains the four default packages;
`all_radio_plots/reports/radio_class_detail/` contains the current optional
collection and CSV plot index. Git HEAD at review was
`1758c8dd2e6888382bbfc001ae6821033d8eb3b5`, with the 2026-10-08 plot
refinements present as local changes. The source run's recorded revision was
`c700c71127654a8d01ebb60f23abaa184d9d2612`, clean. The source-manifest
SHA-256 is `d341c890988576cde9cb14581f6d981ebd41979f908d3e5d8051cb62a37d39f8`.

| Current regenerated package | Seconds | Bytes | MiB | PNG count |
|---|---:|---:|---:|---:|
| Default radio detail, including two density plots | 12.575 | 46,284,818 | 44.141 | 2 |
| Station count | 30.453 | 31,897,799 | 30.420 | 2 |
| Class sessions | 0.375 | 358,916 | 0.342 | 2 |
| CU coverage/streak | 1.969 | 1,241,644 | 1.184 | 8 |
| Radio detail with all optional plots | 268.102 | 312,519,985 | 298.042 | 693 |

The four default packages sum to **45.372 s and 76.087 MiB**, excluding the
copied authoritative summary/identity files and run-summary text. Enabling all
691 radio/session plots adds **255.527 s (4:15.527) and 253.902 MiB** to the
radio-detail case. The 693 PNG count includes its two always-created density
plots. All 707 PNG files across both cases passed Pillow file-integrity
verification; duplicate density plots are counted once per generated case.

The default and optional-plot cases each stream the same source radio/bin
parts through the current writer. Their difference estimates the added
rendering/index cost under the observed cached conditions. It excludes capture
extraction, observation selection, station supplementation, original temporary
staging, and the cost of copying the small summary sources. File-cache and
system-load effects remain possible; this is one paired observation.
Focused tests ran concurrently with part of optional plotting, so these
figures should not be interpreted as a measurement under exclusive system
load or a precise end-to-end incremental runtime forecast.

## Correctness and output review

Regenerated CU/classroom/band tables, the physical class-session table,
station enrollment-weighted aggregate, and CU/station correlation table match
the published source values. Comparisons use logical CSV content with
`rtol=atol=1e-12` for floating-point values, not PNG or CSV byte identity.
Both regenerated large derived tables retain the source row counts. The
optional collection is checked against the source index's 691 plots.

The current corrected-semantic, station, class-session, radio-detail,
capture-integration, and run-lifecycle tests passed: 46 tests. This includes
verification of the frozen comparison's shared-input hash and corrected
CU/streak snapshot; it executes no historical calculation.

Representative graphics were visually inspected for context-box readability,
local-time titles/axes, reduced side padding, CU fill/gaps, station markers,
band histogram detail, enrollment/peak separation, and CU/station density
panels. This agent review is distinct from owner acceptance. The published
small UTech preview also supplies a bounded unavailable-station gap example.

## Remaining release checkpoint

The initial accepted Alpha 0.2 report families are implemented. The
[one-time migration guide](../original_to_corrected_migration.md) documents
historical differences without restoring legacy functions or requiring future
Original replay.

Owner review of current representative output and release validation/publication
remain outstanding. Additional 5/6 GHz statistics, admission-capacity metrics,
beacon-rate diagnostics, and classroom-level CU aggregates require separately
reviewed definitions before implementation.

## Subsequent admission-capacity checkpoint

The admission-capacity gate was subsequently accepted and implemented under
ADR-0013. Its separate [reporting review](alpha_0_2_admission_reporting_review_2026-10-09.md)
records preservation checks, cached time/storage and final graphics. This does
not change the earlier measurements above, execute Original again, or close
the remaining Alpha 0.2 scope and publication decisions.
