# Exploratory beacon-quality validation

Date: 2026-10-12. Scope: ADR-0014 implementation, focused semantics, retained
small-capture checks, presentation, and local reporting-cost measurements.
Existing published `2025-full` output was not rewritten or rerun.

## Retained capture checks

Small retained files were decoded with the existing TShark extraction and
vendor adapters. The preview mapped supported AP names to a test classroom,
used only resolved radios, and defined test sessions around the retained
observation spans. These are not project schedule/exposure measurements.
Selected observations and the existing radio/bin detail were compared before
and after quality construction and remained unchanged.

| Retained capture | Profile | Resolved observations | Stream rows | Window rows | Diagnostic construction seconds |
|---|---|---:|---:|---:|---:|
| `Aruba_ERM_Capture_635_type0_2026-09-14.pcapng` | ERM type 0 | 178 | 10 | 40 | 0.031 |
| `Aruba_ERM_Capture_635_type3_2026-09-14.pcapng` | ERM type 3 | 288 | 10 | 30 | 0.033 |
| `Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng` | ERM type 6 | 1452 | 9 | 65 | 0.035 |
| `Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng` | Native | 4148 | 15 | 60 | 0.081 |

Writing the combined nine-file package took 0.606 seconds and 249853 bytes.
Extraction, selection, and the existing reporting calculations are outside
that package-write measurement. Values above are one local measurement, not
an incremental full-project runtime or a performance guarantee.

## Synthetic workload

A 30-minute, four-radio native/ERM fixture deliberately varies captured slots,
CU, station count, and stale flags for presentation review. Its constructed
missingness is not evidence about measured CU or beacon loss.

- Pre-selection observations: 32878.
- Derived quality windows: 704.
- Diagnostic construction including native-only timing assessment: 0.490 seconds.
- Nine-file package write: 0.473 seconds.
- Package size: 203538 bytes; window Parquet: 15501 bytes.

The implementation processes one session at a time and uses bounded cohort
statistics and slot-gap counters. It adds no TShark decode pass. Complete-run
elapsed time/storage and representative owner review remain release checkpoints.

## Semantic and presentation checks

The regression suite passes 332 tests, with 16 external-tool/short-capture
tests deselected, in 85.59 seconds. The four retained-capture checks above
were run separately with TShark. Python compilation and `git diff --check`
also pass; existing corrected golden inputs and expectations are unchanged.

Focused tests cover ERM effective caps without canonical deletion, usable and
unusable native TSF, mixed families, any-beacon unions versus member counts,
advertised interval mismatches and explicit overrides, exact context/window
splitting, missing lagged CU, stale-assisted station pairs, constant/centered
correlations, empty packages, manifest exports, and failure cleanup.
The new logical CSV/Parquet golden is separate from the unchanged corrected
metric oracle. No PNG bytes are compared.

Preview graphics use enlarged canvases, separate native/ERM panels, and
explicit unknown-exposure/no-threshold labels. Context-band panels use shared
categories and show paired-window counts; a missing category is not displayed
as measured zero loss. The occupied-slot-gap CDF remains an empirical step
function, not a smoothed transmission-loss estimate.

## Remaining evidence

Unknown listening exposure, pooled receivers, and unknown ERM role prevent
interpreting empty slots as confirmed RF losses. Descriptive associations do
not resolve missing-CU bias. Paired ERM/native experiments, exposure-qualified
threshold analysis, and AP-555/AP-635 periodic-loss corrections remain deferred.
