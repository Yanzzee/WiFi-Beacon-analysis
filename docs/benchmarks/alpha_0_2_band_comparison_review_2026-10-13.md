# Detailed band-comparison validation

Date: 2026-10-13. Scope: ADR-0015 semantics, synthetic fixtures, cached
full-project reporting, presentation and storage. No published run was rewritten
and no new full capture decode was launched.

## Cached full-project checks

The source was immutable `2025-full` run
`20261012T161033680867Z-3152b7cd`, with its existing radio/bin and station-rollup
Parquets, authoritative identity/radio summaries, and corresponding sample
schedule/AP inputs. Reporting used one session at a time; preview files were
created under `output/previews/band_comparison_20261012/`.

- Physical sessions: 373.
- Radio/session comparison rows: 691.
- Radio/bin rows examined: 2847339.
- Source CU valid/missing/no-beacon durations, means, high/very-high durations,
  and four authoritative observed/possible streak fields match all new detailed
  radio/session rows. Source files were read-only.

| Measurement | Initial preview | Final preview |
|---|---:|---:|
| Radio-detail read and accumulation | 10.762 s | 9.916 s |
| Station-rollup read and accumulation | 13.947 s | 12.674 s |
| Package finalization/write | 2.223 s | 1.915 s |
| Total cached reporting | 26.932 s | 24.505 s |
| Eighteen-file package size | 2389120 bytes | 2332753 bytes |

The final package is approximately 2.22 MiB. It includes eleven CSVs, five
graphics, JSON context and methodology. The final preview incorporates shared
normalized density scaling and distinguishes structural absence from measured
station means/peaks. The runs were not controlled CPU/disk benchmarks; small
differences should not be attributed to a single code change.

These timings include narrow cached-source reads. Production accumulation
shares already-loaded report session parts and adds no decode or separate read
pass. Neither measurement is an isolated incremental full-capture runtime.
Full-run timing/storage and owner output review remain release checkpoints.

## Synthetic and semantic checks

The focused fixture contains mixed rooms, two 5 GHz radios on one AP, 6 GHz,
2.4 GHz, explicitly enabled but unobserved APs, stale assistance, zero clients,
missing station totals and terminal partial bins. Tests cover:

- default/explicit enablement, invalid tokens and observed-data contradictions;
- AP versus classroom populations and project-wide classification;
- valid-radio-duration and equal-entity/session weighting;
- same-radio/bin positive-station ratios and zero-client CU;
- simultaneous complete adoption totals, separate 5+6/all-band denominators,
  stale duration, and exclusion of overlapping AP/classroom contributions;
- common-valid-bin AP matching and multiple radio pairs;
- unknown versus structurally absent bands, including empty sessions;
- deterministic logical tables, create-only output, cleanup, and manifest exports;
- exact density-grid time accounting without clipping statistical inputs.

Final regression: 348 passed, 16 external-tool/short-capture tests deselected,
in 93.82 seconds. The finalized focused band tests also pass. Compilation and
`git diff --check` pass.

The logical report golden is `tests/golden/band_comparison_v1.json`;
the authoritative corrected golden and its inputs remain unchanged. PNG and
Parquet container bytes are not compared.

## Presentation review

Actual full-project and synthetic charts were visually reviewed. Figures use
doubled canvases, empirical unsmoothed CU CDFs, clearly labelled AP/classroom
cohorts, explicit no-data states, and common CU/paired-comparison scales.
Density panels normalize within each cohort with one logarithmic color scale;
their last station bucket represents counts at least 245, while CSV statistics
retain the original counts. Adoption plots measure association-time, not
device capability or distinct people/devices.

## Remaining limitations

Observed sample topology is not a complete hardware inventory. Explicit
enablement can identify installed/enabled but unsampled 6 GHz; it cannot supply
missing CU/station readings. Comparisons remain descriptive and capture-quality
bias is not removed by matching. Year-over-year joins are manual initially,
with scope/coverage/configuration review. No bandwidth/power inference,
automated lifetime AP identity, throughput/latency claim or causal model is added.
