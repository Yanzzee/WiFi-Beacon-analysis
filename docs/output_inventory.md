# Corrected Output Inventory

All supported outputs are owned by one immutable run directory:

```text
Output/runs/<project>/<run-id>/
```

Parquet artifacts and the manifest are authoritative. CSVs, text summaries,
and plots are derived review exports produced inside the same publication gate.

## Durable logical artifacts

| Artifact | Grain and purpose |
|---|---|
| `project_identity_map.parquet` | One row per in-scope BSSID. Records BSSID to radio to AP to classroom identity, SSID, AP model when configured, band/channel/frequency evidence, resolution methods, status, and diagnostics. |
| `selected_observations/` | Date-partitioned selected beacon rows. At most one row per BSSID/class-session/bin, with source lineage, observation/TSF timing status, AP/radio identity, QBSS CU, station count, and admission capacity. |
| `radio_class_summary.parquet` | One row per stable radio/class session. Records valid-CU coverage and average, missing-CU/no-beacon duration, high/very-high duration, actual/possible streaks, CU value counts, member/preferred BSSIDs, band/channel, and class boundaries. |
| `classroom_class_summary.parquet` | One row per physical classroom session. Records radio counts by band and maximum high/very-high actual/possible streaks with responsible radio IDs. |
| `manifest.json` | Run/project identity, input hashes, code provenance, requested and selected time ranges, effective configuration, capture profiles/field plans/adapters, stage results, schema/metric identifiers, and artifact/export references. |

## Compact CSV exports

`exports/csv/` contains:

- `project_identity_map.csv`
- `radio_class_summary.csv`
- `classroom_class_summary.csv`

These are direct review copies of the corresponding Parquet tables. The large
selected-observation dataset is Parquet-only by default.

## CU, coverage, and streak package

`reports/cu_coverage_streak/` contains:

- `run_summary.txt`: scope, input normalization, retained/staged/selected row
  counts, identity and per-band radio counts, elapsed time, output row counts,
  and stage warning/error totals;
- `methodology.txt`: metric definitions and a description of every durable and
  derived file;
- `radio_class_report.csv`;
- `classroom_class_report.csv`;
- `band_population_report.csv`; and
- `plots/` with:
  - `radio_average_cu_distribution.png`
  - `radio_cu_vs_coverage.png`
  - `radio_coverage_and_missingness.png`
  - `radio_bin_cu_histogram.png`
  - `radio_bin_cu_cdf.png`
  - `classroom_streak_distributions.png`
  - `radio_bin_cu_histogram_by_band.png`
  - `radio_bin_cu_cdf_by_band.png`

CU histograms and CDFs use one valid selected value per radio/bin. Band panels
separate 2.4, 5, and 6 GHz. CDFs label the cumulative share at 50% and 75% CU.
The band-population table compares all 5 GHz radios, 5 GHz radios on APs with a
resolved 6 GHz radio, and those APs' 6 GHz radios without reweighting.

## Lifecycle log

Small JSON-lines logs are stored under:

```text
Output/runs/<project>/_logs/<run-id>.jsonl
```

They record run start, input normalization, understood failure/interruption,
abandoned-staging cleanup, and successful publication. They are operational
records rather than analysis artifacts.

## Planned additions

| Item | Status |
|---|---|
| Station-count rollups | Entity grains are accepted: retain every BSSID/bin station count, then derive radio/bin, SSID/classroom/bin, and band/classroom/bin totals. Missing-member/partial-total and staleness rules still require owner review. |
| Corrected thematic reports | Planned replacements for useful all-observation, class-session, and radio-summary views, derived only from corrected artifacts. |
| Richer 5/6 GHz comparison | Minimal band distributions and paired-AP population summary are implemented. Comparability/quality warnings and broader statistics remain planned. |
| Beacon-rate diagnostics | Future pre-selection analysis should separate active cadence, ERM/controller delivery pauses, capture coverage, and true over-the-air behavior. |
| Additional vendor adapters | Extreme and Aruba AP-name paths are reviewed. Additional vendor AP-name fields require representative capture evidence. |

## Historical comparison

The former result set is not generated. Its last supported refactored revision
is Git commit `4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb`.
`tests/golden/legacy_corrected_comparison_v1.json` preserves one small
same-input comparison explaining intentional semantic differences without
keeping the former runtime in production or tests.
