# Corrected Output Inventory

Analysis outputs are owned by one immutable run directory:

```text
output/runs/<project>/<run-id>/
```

The five canonical artifacts listed below are authoritative. Reporting Parquets,
CSVs, HTML/text summaries and plots are derived review exports produced inside
the same publication gate.

## Human review entry point

`reports/index.html` is a self-contained, offline HTML overview under ADR-0017.
It displays existing compact-report values with coverage/units, links curated
figures, groups physical sessions by classroom with schedule-local times, and
exposes optional radio plots through their index. Each family has a collapsed
file inventory and methodology links; absent packages are explicitly unavailable.
CSV links open/download according to browser settings. No radio/bin metric is recomputed
and no large observation/bin Parquet is read for navigation. All links remain
within the run and are validated before publication, except the manifest which
is written next in the same gate. The manifest registers the overview as a
derived HTML export; it is not another authoritative artifact.

The CLI prints its path and `reports/run_summary.txt` names it as the full-run
entry point. It uses inline CSS, native disclosures and a small inline script
for current-section highlighting, with no remote assets. Native links remain
usable without scripting; the menu wraps on narrow screens.
Existing paths and optional-plot defaults remain unchanged. Retain the complete
run directory for portability; older published runs are immutable.

ADR-0018 refines the page from project-wide context/results to classroom detail.
Observation dates use selected-beacon bounds in the schedule timezone, not an
invented full-capture extent. Class-session hours sum scheduled room/session
durations, with simultaneous rooms contributing separately. Counts and selected
rows retain their manifest/source definitions. The enrollment headline preserves
the enrollment-weighted peak ratio, not a mean concurrent devices/person metric.
Full-width originals remain clickable and carry short explanations. Existing
Pearson/Spearman coefficients and pair coverage are visible; no regression R²
is claimed. Classroom headings show observed AP/radio/seat counts; rows show mean radio CU pooled
from exact compact radio/session components under ADR-0015, not rounded radio
means or a merged classroom/channel CU reading.

The current section order and table/figure placement are documented in
[reporting and publication](reporting_and_publication.md). Cohort sample counts
include classrooms; the expandable model table preserves sampled radio
configuration, and each classroom has a band summary above its session rows.
Means retain each source table's documented weighting. The matched-CU display
also gives the equally weighted mean of valid saved radio-pair/class-session
differences; it does not change their underlying common-bin calculations.

## Separate saved-report refresh bundles

`beacon_analysis.py report --from-run <completed-run>` creates
`output/reports/<project>/<source-run-id>/<report-id>/reports/index.html`.
It independently copies the source `reports/`, available `exports/`, and
manifest, then rebuilds HTML and supported figures from saved data. These
copies include derived reporting Parquets and optional radio plots when present;
they omit the root canonical Parquets, selected observations, captures and logs.
`report_refresh.json` identifies the source manifest, renderers and bounds
backfill: added columns, source/result hashes and arithmetic provenance. The original
run and its initial report remain unchanged. This is not yet a curated public
export, and it is not just a single HTML file.

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

These preserve the corresponding Parquet source columns. Radio review CSVs
also add raw/ratio CU mean `*_lower_bound` and `*_upper_bound` companion
columns and `cu_bounds_metric_id` under
[ADR-0020](decisions/0020-conservative-cu-missing-bin-bounds.md); canonical
Parquet schemas are unchanged. The large selected-observation dataset is
Parquet-only by default.

The first bounds stage also adds percent companions to CU radio/population
reports and band-comparison radio/population/model/classroom CSVs. Existing
mean fields remain observed. Empty populations have unavailable bounds;
nonempty populations with no CU have bounds 0-100%. Stage 2 adds equal-session,
zero-station and paired-window ranges to comparison CSVs and current/previous
mean ranges to `beacon_quality_windows.parquet`. Their distinct eligibility
and denominator rules are recorded in ADR-0020. Stage 3 adds numeric CDF
summaries, CU CDF shading and intervals in mean figures. Stage 4 adds station-time
ranges to exposure exports/chart and the CU subsection. Stage 5 integrates
HTML/text displays and supported historical table backfill. It preserves
original fields, adds companion columns, and leaves unsupported ranges
unavailable. See the
[full field inventory](cu_missing_bin_bounds_implementation_plan.md).

## CU, coverage, and streak package

`reports/cu_coverage_streak/` contains:

- `methodology.txt`: metric definitions and a description of every durable and
  derived file;
- `radio_class_report.csv`;
- `classroom_class_report.csv`;
- `band_population_report.csv`;
- `cu_cdf_bounds.csv`: observed/minimum/maximum cumulative percentages at the
  existing 50%/75% markers, with valid/expected radio-bin counts; and
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
separate 2.4, 5, and 6 GHz. CU CDFs retain observed lines and label observed/
minimum/maximum cumulative shares at 50% and 75% CU, with shaded missing-bin
ranges. An absent band is omitted; a present entirely missing band shows
its possible range without an observed curve. Exact expected bin counts
are retained as `expected_radio_bin_count` in the radio report and
`expected_radio_bin_values` in the population report; unknown counts stay blank.
The band-population table compares all 5 GHz radios, 5 GHz radios on APs with a
resolved 6 GHz radio, and those APs' 6 GHz radios without reweighting.
That original corrected population table remains observation-based; explicit
enablement declarations are applied in the detailed band-comparison package.

## Radio/class detail package

`reports/radio_class_detail/` contains:

- `radio_bin_detail.parquet`: one row per physical radio/class-session/bin,
  combining the selected CU state/value and source with the accepted complete-
  only radio station total, availability, and stale-assistance state;
- `methodology.txt`: source selection, lineage, station join, and unknown-CU
  presentation and relationship rules;
- `cu_station_correlation.csv`: overall and per-band valid-pair coverage,
  stale-assisted pair counts, and descriptive Pearson and tie-aware Spearman
  correlations at the same-radio/same-bin grain;
- `cu_station_hexbin.png` and `cu_station_hexbin_by_band.png`: bounded overall
  and per-band density plots for valid CU/station pairs; and, when explicitly
  enabled,
- `radio_class_plot_index.csv` plus `plots/*.png`: one indexed CU/station plot
  per radio/class session.

The project-level relationship plots are always created. The large per-radio
collection is disabled by default; `--radio-class-plots` or
`RADIO_CLASS_PLOTS=true` enables it. Unknown CU remains blank; a dashed blue
connection between surrounding measurements is a visual guide only and is
never stored or used as an imputed metric value.

## Admission-capacity package

`reports/admission_capacity/` contains:

- `admission_capacity_summary.csv`: overall/per-band valid coverage, zero/constant
  warnings, missing/invalid/no-beacon counts, disagreements and source diagnostics;
- `admission_capacity_correlation.csv`: capacity/CU and capacity/station valid-pair
  coverage, stale-assisted station pairs, source differences, and descriptive
  Pearson/tie-aware Spearman coefficients;
- `admission_capacity_histogram.png`, `admission_capacity_cdf.png`,
  `admission_capacity_cu_hexbin.png`, `admission_capacity_station_hexbin.png`,
  and their `_by_band` variants; and
- `methodology.txt`: ADR-0013 interpretation, validity, source and population rules.

The source is the extended `reports/radio_class_detail/radio_bin_detail.parquet`.
Its report-schema metadata is `beacon.schema/radio-bin-detail/v2`; existing CU
fields retain their meaning. Capacity has a separate
`beacon.metric/radio-admission-capacity/v1` identifier and source lineage.
No new authoritative artifact is added. Old immutable runs are not rewritten.

CU describes channel conditions, whereas capacity describes a radio's advertised
QoS admission allowance. Capacities are never summed across BSSIDs, radios,
bands or classrooms. Raw 0..31250 is converted to percent; zero is retained,
invalid values stay in source evidence, and unknown capacity is not stale-filled.
Histograms use 1-percentage-point bins centered on integer percentages without
rounding raw values; CDFs preserve discrete resolution.

## Exploratory beacon-quality package

`reports/beacon_quality/` contains:

- `beacon_cadence_sessions.csv`: BSSID/capture-family/class-session raw views,
  occupied/expected capture slots, effective extras, interval and timing
  diagnostics, edge/interior empty slots, and source-capture references;
- `beacon_quality_windows.parquet`: bounded radio/family/session windows,
  member and any-beacon availability, current/previous-window CU and station
  context, known durations, and station stale durations;
- `beacon_quality_associations.csv`: descriptive previous-window CU/station
  Pearson associations, pair coverage, and stale-assisted station pair counts
  by family/model/band/project-observed 5/6-GHz radio-group counts;
- `beacon_quality_covariate_bands.csv`: descriptive context bands and their
  empty/possible member-slot fractions, retaining those same cohorts;
- `beacon_slot_gap_counts.csv` and `beacon_slot_gap_cdf.png`: exact counts and
  empirical distribution of gaps between occupied capture slots;
- `cu_capture_slot_availability.png` and
  `station_capture_slot_availability.png`: pooled descriptive band comparisons,
  with ERM and native panels separate; and
- `methodology.txt`: baseline assumptions, unknown exposure/role, provenance,
  association weighting, and explicitly deferred inference/correction.

The source is pre-selection, session-admitted observations, not the thinned
`selected_observations/` artifact. The expected interval defaults to 100 TU;
ERM counts at most one effective BSSID view per ten intervals and ignores TSF
for this diagnostic. Native slots use observation timestamps after conservative
native-only duplicate recognition; TSF never controls slot placement. Extra
ERM views remain in canonical processing. The cadence CSV also records native
TSF jump counts, step/offset differences and anomalous capture/frame/time/TSF
lineage. Clock anomalies appear in the run log and capture-quality web section.
Unknown listening exposure means empty slots are not confirmed RF losses.
No missing CU, safe threshold, or AP-model loss correction is inferred.

The metric is `beacon.metric/beacon-capture-quality/v2` and report schema is
`beacon.schema/beacon-quality-reports/v2`. The package adds no durable artifact
and does not rewrite published runs. Core metric v2 uses observation-time bins
with null historical TSF-origin/mapped-time fields. See
[ADR-0019](decisions/0019-observation-time-and-native-tsf-diagnostics.md).

## Run-wide summary

`reports/run_summary.txt` is the first-read report shared by every report
family. It records scope, input normalization, retained/staged/selected row
counts, identity and per-band radio counts, elapsed time, output row counts,
and stage warning/error totals. It is kept outside the CU directory because
its scope is the complete run.

## Station-count package

`reports/station_count/` contains:

- `station_bin_rollups.parquet`: one row per class-session/bin/rollup entity
  for radio, SSID, band, and classroom totals, with nullable station count,
  availability, and stale-assistance state;
- `station_class_report.csv`: availability counts and the maximum available
  station total for every rollup entity, plus combined enrollment and `peak
  client count per enrolled` on classroom rows;
- `station_aggregate_report.csv`: enrollment-weighted overall, per-SSID, and
  per-band `peak client count per enrolled`, with the component peak and
  enrollment sums, eligible-session count, and stale-assisted peak count; and
- `methodology.txt`: sample/data scope, session membership, 30-second
  extension, complete-total, enrollment, and maximum-statistic limitations;
- `classroom_bin_station_histogram.png`: the all-band/all-SSID complete
  classroom/bin station-count distribution; and
- `classroom_bin_station_histogram_by_band.png`: complete band/classroom/bin
  distributions with stale-assisted values identified.

The package is a manifest-registered derived view built one physical session
at a time. `selected_observations/` remains authoritative. Empty sessions
produce unavailable classroom/bin rows rather than a numeric zero. No station
value is inferred for those bins.

## Class-session package

`reports/class_sessions/` contains:

- `class_session_report.csv`: one row per physical classroom session joining
  deterministic course/section context, combined enrollment, descriptive room
  capacity status, radio counts, CU coverage, accepted streak maxima, station
  availability, peak classroom client count, and peak count per enrolled;
- `peak_client_count_per_enrolled_histogram.png`: eligible class-session ratio
  distribution;
- `enrollment_vs_peak_client_count.png`: combined enrollment versus accepted
  peak classroom client count, with stale-assisted peaks distinguished; and
- `methodology.txt`: scope, schedule-context, capacity, CU, and station rules.

Room capacity is context only. Missing capacity remains null and conflicting
simultaneous-section capacity values become null with a conflict flag. No room
capacity ratio is calculated. QBSS admission capacity is reported separately,
not as a classroom/enrollment metric.

## Potentially affected station package

`reports/affected_stations/` is a derived ADR-0016 package:

- `entity_session_exposure.csv`: radio/AP/classroom session rows with scope,
  band, source keys, schedule context, paired station-time components and shares,
  coverage/staleness, complete-bin concurrent peaks and peak bin/time provenance;
- `population_exposure.csv`: overall/band/AP-cohort/classroom-cohort exposure
  pooled from radio rows once, with exact components and coverage;
- `classroom_peak_distribution.csv`: available/missing session peak counts,
  minimum, median, p90 and maximum; never a sum of independent peaks;
- `station_time_exposure.png` and `classroom_peak_cdf.png`: two bounded review
  graphics, with detailed coverage in the tables;
- `context.json` and `methodology.txt`: versions, thresholds, source references,
  units, complete-bin policy and interpretation limits.

Station-time is association count times duration, not unique people/devices or
proven impairment. High includes very high. Missing CU remains unknown; accepted
stale station assistance participates. Pair coverage uses radio time, while
complete-bin peak coverage uses entity time. No sixth authoritative artifact is
introduced, and existing published runs are not modified.

The current HTML displays station-time exposure after the CU streak subsection.
ADR-0020 adds `missing_cu_station_time_us`, `cu_bounds_metric_id`, and high/
very-high quantity and percentage `*_lower_bound` / `*_upper_bound` fields
to the two exposure CSVs. Quantity bounds are observed qualifying station-time
through that amount plus available station-time with missing CU. Percentage
bounds divide by all available station-time; the observed percentage still
uses paired CU/station time. Accepted stale counts participate. Unknown station
counts remain outside these bounds. The chart adds min/max whiskers and the
CU subsection reports ranges with station/association-time coverage. There
is no additional file or capture decode; the package still has seven files.

The web report deliberately excludes `classroom_peak_distribution.csv` and
`classroom_peak_cdf.png` from both the displayed content and file inventories.
Those files are still produced and retained in run/refresh bundles; their
presence does not imply that they are part of the current web presentation.

## Detailed band-comparison package

`reports/band_comparison/` contains eleven CSVs, five bounded graphics, context,
and methodology:

- `deployment_summary.csv`: overall configured/observed AP and classroom counts,
  6 GHz enablement percentages, mixed-room and enabled-but-unobserved counts,
  and sampled radio counts for all three bands;
- `ap_infrastructure.csv` and `classroom_infrastructure.csv`: reporting
  enablement, its evidence/source, models, and sampled topology;
- `population_summary.csv`: overall, bands, AP cohorts, and classroom cohorts,
  with duration-weighted CU/coverage/high shares, equal entity/session views,
  station/capacity coverage, CU relative to stations, component sums and counts;
- `model_population_summary.csv`: the same metrics split by model and observed
  numbers of 5/6 GHz radio groups;
- `classroom_band_summary.csv`: pooled radio-measurement summaries by space/band
  for manual year-over-year comparisons, not CU sums;
- `radio_class_comparison.csv`: every physical radio/session, with contextual
  topology, local/UTC boundaries, enrollment, accepted streaks and detailed metrics;
- `matched_ap_radio_sessions.csv`: same-AP/session 5/6 radio pairs, common-valid
  duration/coverage, matched CU means and signed percentage-point differences;
- `station_adoption_sessions.csv` and `station_adoption_summary.csv`: actual
  station use by AP/classroom/session and aggregate, including both 6/(5+6)
  and all-band denominators, eligible/stale duration and component sums;
- `cu_distribution.csv`: exact raw-CU counts and valid durations by population;
- `cu_cdf_bounds.csv`: observed/minimum/maximum cumulative percentages at
  50%/75% CU, weighted by radio duration, with exact valid/expected weights;
- `cu_cohort_cdf.png`, `cohort_metrics.png`, `matched_ap_cu.png`,
  `station_adoption.png`, and `cohort_cu_station_density.png`;
- `comparison_context.json`: project/run/version/configuration/capture context,
  units, weighting rules, source locations and manual cross-project boundaries;
- `methodology.txt`: [ADR-0015](decisions/0015-band-comparison-and-longitudinal-reporting.md)
  interpretation and scope.

The report metric is `beacon.metric/band-comparison/v1`; its schema is
`beacon.schema/band-comparison-reports/v1`. Enablement comes from optional
`6GHz_Enabled` or project-observed 6 GHz evidence. Primary CU weighting is valid
radio duration; CU is never summed or filled. Actual use is association-time,
not unique clients or device capability. Stale-assisted complete station totals
participate. No additional durable artifact or decode pass is introduced.

## Lifecycle log

Small JSON-lines logs are stored under:

```text
output/runs/<project>/_logs/<run-id>.jsonl
```

They record run start, input normalization, understood failure/interruption,
abandoned-staging cleanup, and successful publication. They are operational
records rather than analysis artifacts.

## Planned additions

| Item | Status |
|---|---|
| Station-count plots and cross-metric analysis | Classroom/bin and per-band station distributions plus same-radio/same-bin CU/station hexbins and descriptive Pearson/Spearman correlations are implemented. Classroom-level CU aggregation, inferential regression, and other percentiles remain deferred. |
| Corrected thematic reports | Initial all-class distributions, class-session detail, radio summaries, and opt-in time series are implemented under ADR-0010/0011/0012. Further report selections need review. |
| Radio admission capacity | ADR-0013 implements independent radio/bin selection and overall/per-band distributions and CU/station relationships. No capacity sums or admission guarantees. |
| Richer 5/6 GHz comparison | ADR-0015 implements overall/AP/classroom/cohort summaries, actual station use, common-bin AP matching, enablement context and manual longitudinal exports. Complete-run evidence exists; final owner output review remains. |
| Beacon-rate diagnostics | ADR-0014 implements minimal native/ERM capture-slot diagnostics. Exposure-qualified inference and calibration remain deferred. |
| Additional vendor adapters | Extreme and Aruba AP-name paths are reviewed. Additional vendor AP-name fields require representative capture evidence. |

## Historical comparison

The former result set is not generated. Its last supported refactored version
is the public
[Original release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original).
`tests/golden/legacy_corrected_comparison_v1.json` preserves one small
same-input comparison explaining intentional semantic differences without
keeping the former runtime in production or tests.
The one-time informational comparison and guidance for interpreting or rerunning
older projects are in [`original_to_corrected_migration.md`](original_to_corrected_migration.md).
Original is not a recurring release-validation dependency.
