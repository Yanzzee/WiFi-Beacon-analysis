# First corrected CU, coverage, and streak report package

**Status:** Owner-approved and implemented on 2026-09-29. The 2025-full owner
review found the overall output correct so far; the run-summary and plot/readme
refinements recorded below followed that review.

This document preserves the CU package's calculations and dated implementation
history. The current human entry point is `reports/index.html`; use
[reporting and publication](reporting_and_publication.md) for current section
placement, saved-run refresh and hosting status. The plain-text run summary
remains `reports/run_summary.txt` and is not the only review entry point.

## Purpose

Update 2026-11-16: All five stages of
[conservative missing-bin bounds](decisions/0020-conservative-cu-missing-bin-bounds.md)
add mean lower/upper fields, numeric CDF summaries, shaded CU CDF envelopes
and selected mean-figure intervals. Observed mean columns, CDF curves and histogram
counts are unchanged. The two mean bounds assign all missing CU time 0% or
100%; they are not confidence intervals or filled readings. The existing CU
station-time subsection also reports high/very-high exposure bounds, conditional
on available station counts, with observed bars and min/max whiskers. HTML/text
displays and supported saved-run backfill are integrated, retaining observed
values and marking unsupported bounds unavailable. Details are in the
[implementation inventory](cu_missing_bin_bounds_implementation_plan.md).
The owner-reviewed presentation separates observed and possible values in HTML
metrics and tables. Cohort CDFs use separate panels. Matched-radio and
mean-CU-versus-coverage scatter plots remain observed-only; numeric bounds and
histogram count shading are retained.

The report package makes the implemented ADR-0004 radio/class and
classroom/class results easy to inspect. It should answer four practical
questions:

1. What CU was observed for each radio during each class?
2. How much of the class had valid CU, missing CU, or no beacon?
3. How long was CU above 50% and above 75%?
4. What were the longest actual-observed and possible-connected streaks, and
   which radio was responsible for each classroom maximum?

The package is a derived presentation of the five durable artifacts. It does
not create a sixth authoritative artifact or change metric definitions.

## Implemented run-local layout

```text
output/runs/<project>/<run_id>/
  reports/run_summary.txt
  reports/cu_coverage_streak/
    radio_class_report.csv
    classroom_class_report.csv
    band_population_report.csv
    methodology.txt
    plots/
      radio_average_cu_distribution.png
      radio_cu_vs_coverage.png
      radio_coverage_and_missingness.png
      radio_bin_cu_histogram.png
      radio_bin_cu_cdf.png
      classroom_streak_distributions.png
      radio_bin_cu_histogram_by_band.png
      radio_bin_cu_cdf_by_band.png
```

The report directory is generated inside the existing staging/publication
gate and referenced as a derived export from the radio/class and
classroom/class manifest records. Parquet remains authoritative.

`reports/run_summary.txt` is the concise first-read report shared by all
report families. It records project/run
identity, elapsed processing time through report generation, capture and worker
counts, configured/observed AP and radio counts, per-band radio counts, staged
and selected observation counts, summary-table row counts, and aggregate stage
warning/error counts. When the station package is present, it also presents
the enrollment-weighted overall, per-SSID, and per-band peak client count per
enrolled, with component sums and eligible-session counts. It is generated
from the same effective configuration and run-local reports inside the
publication gate rather than copied from terminal text.

The CU-specific tables, plots, and methodology remain under
`reports/cu_coverage_streak/`. This keeps the run-wide entry point outside a
single metric family without reorganizing the thematic packages.

## Required source fields

The report consumes only:

- `radio_class_summary.parquet`;
- `classroom_class_summary.parquet`;
- `project_identity_map.parquet`; and
- the run manifest.

The summaries now carry additive `class_start` and `class_end` UTC fields. The
report does not parse timestamps out of `class_session_id`, and the source
schedule is not a durable run artifact. `ap_id` is joined from the identity
map through the already validated radio membership. The radio/class artifact
also carries a fixed 256-value `valid_cu_raw_counts` vector. It is populated
during the existing ADR-0004 radio/bin pass after preferred-BSSID fallback,
so it records at most one valid CU value per radio/bin without rescanning
selected observations.

## Radio/class report

`radio_class_report.csv` has one row per radio/class session. It presents the
existing corrected fields in human units:

| Area | Proposed columns |
|---|---|
| Session | `class_session_id`, `classroom`, `class_start_utc`, `class_end_utc`, optional configured-local start/end |
| Identity | `ap_id`, optional `ap_model`, derived `ap_band_radio_count`, `ap_has_6_ghz`, `capture_cohort`, `radio_group_id`, `band`, `channel`, `member_bssids`, `preferred_bssid` |
| Coverage | class, valid-CU, missing-CU, and no-beacon seconds; `cu_coverage_percent` |
| CU | `average_cu_percent` |
| Above 50% | high-duration seconds, possible-connected streak seconds, actual-observed streak seconds |
| Above 75% | very-high-duration seconds, possible-connected streak seconds, actual-observed streak seconds |

The authoritative microsecond and ratio values remain in Parquet. Seconds and
percentages are display conversions only. List-valued BSSID fields use the
same compact JSON-array CSV representation as existing review exports.

## Classroom/class report

`classroom_class_report.csv` has one row per classroom/class session and
contains:

- session/classroom timestamps;
- total and per-band radio counts;
- the maximum possible-connected and actual-observed streak at both 50% and
  75%; and
- every tied responsible radio ID.

For coverage context, it also derives clearly named distributions across that
classroom's radio rows:

- minimum, median, and maximum radio CU coverage percent; and
- count of radios with and without any valid CU time.

These are explicitly distributions of per-radio coverage. The package does
not invent a single combined classroom CU average or silently weight radios
against one another.

## Band-population report

`band_population_report.csv` is a derived three-row project summary:

1. all resolved 5 GHz radios;
2. the subset of 5 GHz radios whose AP also has a resolved 6 GHz radio; and
3. the 6 GHz radios on those APs.

The first and second populations overlap intentionally. Each row reports its
run/schema/metric provenance; radio/class, distinct-radio, AP, class-session,
and valid-radio/bin counts;
class radio-hours; valid-CU radio-hours; valid-CU radio-time coverage;
duration-weighted average CU; and high/very-high shares of valid CU time.
Empty populations remain present with zero entity counts and null CU/coverage
results. AP membership comes from configured hostname identity, while band
comes from resolved beacon evidence. No AP-model, inverse-coverage, or
equal-AP weighting is applied.

## Plots

All plots are project-level summaries so output count remains bounded.

1. `radio_average_cu_distribution.png`
   - distribution of duration-weighted radio/class average CU;
   - separate or colored by 2.4, 5, 6, and unknown band;
   - 128 fixed buckets, each spanning two raw CU values;
   - reports how many null averages were omitted.
2. `radio_cu_vs_coverage.png`
   - x-axis: radio/class CU coverage percent;
   - y-axis: duration-weighted average CU percent;
   - each point is one radio/class session;
   - one panel per band, so overlapping channel numbers across bands cannot
     combine unlike radios;
   - color: AP model plus the observed count of same-band radios on that AP;
   - makes low-coverage averages visibly qualified rather than suppressing
     them.
3. `radio_coverage_and_missingness.png`
   - distributions of valid-CU, beacon-with-missing-CU, and no-beacon shares;
   - uses actual class duration as the denominator.
4. `radio_bin_cu_histogram.png`
   - one valid selected CU value per radio/analysis bin;
   - one fixed bucket for every raw CU value from 0 through 255;
   - excludes missing-CU and no-beacon bins rather than converting them to
     zero.
5. `radio_bin_cu_cdf.png`
   - exact cumulative distribution of the same radio/bin values;
   - fixed 0-255 domain displayed as percent, with high and very-high
     thresholds marked and the cumulative share labeled at 50% and 75% CU.
6. `classroom_streak_distributions.png`
   - separate panels for CU above 50% and above 75%;
   - compares actual-observed with possible-connected classroom maximums;
   - bucket width equals the smallest compatible configured analysis-bin
     interval, with the axis displayed in seconds;
   - both threshold panels use the same horizontal and vertical scales;
   - labels possible-connected values as estimates that may bridge unknown
     bins, never as continuous observed time.
7. `radio_bin_cu_histogram_by_band.png`
   - separate 2.4, 5, and 6 GHz panels;
   - one valid selected CU value per radio/analysis bin;
   - retains empty panels when a band has no data.
8. `radio_bin_cu_cdf_by_band.png`
   - CDF panels for the same band-separated radio/bin populations;
   - marks the high and very-high CU thresholds and labels the cumulative share
     at 50% and 75% CU in each populated panel;
   - does not treat missing-CU or no-beacon bins as zero.

PNG is recommended for the first package because it matches the existing
workflow and adds no interactive-report dependency. Tables remain the review
and accessibility path.

## Missing-value and interpretation rules

- Missing CU never becomes zero.
- Null average/high/streak results remain blank in CSV and are omitted from a
  plotted numeric series with an explicit omitted count.
- A measured or calculated zero remains zero.
- No minimum coverage suppression is added. Coverage is always shown beside
  average CU and streak information.
- Actual-observed streak is the primary corrected streak.
- Possible-connected streak is always named in full and visually secondary.
- High means CU greater than 50%; very high means CU greater than 75%.
- Durations use terminal partial-bin duration exactly as stored by the
  corrected summary calculation.
- `ap_model` is optional text copied from the configured AP CSV. The
  same-band radio count is derived from resolved project identity, not from a
  model-name rule. These fields label review cohorts only: they do not impute
  beacons, alter CU/coverage, or apply inverse-coverage/model weights.

## Relationship to useful original reports

| Report purpose | Current implementation |
|---|---|
| Aggregate CU histogram/PDF/CDF | Exact raw-value histogram and CDF at one selected value per stable radio/bin, plus the radio/class average-CU distribution and CU-versus-coverage plot. This intentionally replaces duplicated beacon-row weighting with stable radio/time sampling. |
| `High_CU_streaks.png` and zoomed variant | One corrected classroom streak distribution with separate thresholds and actual-versus-possible meanings. |
| Text logs containing CU/streak aggregates | Reviewable radio/class and classroom/class CSV tables with manifest provenance. |
| Per-radio CU/station time-series plots | Implemented as an indexed opt-in collection from `reports/radio_class_detail/radio_bin_detail.parquet`. Unknown CU is shaded and any dashed interpolation is visual only. |
| Station/enrollment reports | Implemented under `reports/station_count/` using ADR-0010 semantics. |
| Beacon-rate reports | Later implemented under ADR-0014/0019 in `reports/beacon_quality/`: observation-time native/ERM capture-slot cadence, gaps and descriptive availability relationships. Listening exposure remains unknown; RF-loss inference, AP-model baseline corrections and estimated CU are deferred to Alpha 0.3. |

Later report families may add useful all-observation, class-session, and
radio-summary views. They must be derived from corrected artifacts and
semantics. Their exact table and plot selections remain a report-design review
item.

## Generation and validation

The report writer should:

1. read the staged authoritative Parquet artifacts;
2. validate schema/metric IDs and cross-artifact radio/session joins;
3. perform only unit conversion, explicit coverage-distribution derivation,
   and plotting;
4. write into the owned run staging directory;
5. record report paths in the manifest; and
6. allow publication only when the complete configured package succeeds.

Focused tests should cover null versus zero, percentage/seconds conversion,
coverage partitions, responsible-radio ties, UTC/local display conversion,
stable filenames, bounded plot count, run-local paths, and absence of any
metric recalculation from selected beacon rows.

## Implemented sequence

1. Added `class_start` and `class_end` to both corrected summary artifacts and
   their CSV exports, with fixture/golden updates.
2. Implemented the radio/class and classroom/class presentation CSVs and six
   initial bounded plots.
3. Connected all nine files to the existing atomic publication gate and
   manifest-derived-export records.
4. Added focused semantics and runner integration coverage.
5. Added the minimal PR-0045 band-population CSV plus band-separated
   radio/bin histogram and CDF, reusing the accepted CU population without a
   second calculation path.

## Multi-radio preservation audit

The current pipeline does not select one radio per AP. Configured-hostname
scope retains every matching beacon; project identity groups BSSIDs within an
AP by band plus channel; timing/deduplication and bin selection operate per
BSSID; and ADR-0004 preferred/fallback selection operates within each resolved
radio. A focused invariant now sends two 5 GHz radios on separate channels
through scope, resolution, per-BSSID selection, and radio/class summary and
requires both radios to survive with observed CU time.

The published `2025-test` run `20261001T175718655011Z-97b6eeb5` provides a
second, real-project check. Each of the four configured AP-555 APs resolved to
two independent 5 GHz radio groups. Every group had selected observations and
a complete set of radio/class summary rows. Within each AP, the two radios had
nearly equal selected-row counts and nearly equal mean coverage. This is not
the pattern expected from accidentally dropping one radio.

That audit does **not** establish a lower raw ERM capture rate by model. The
published difference is valid-CU bin coverage: the AP-515 rows averaged about
97.3%, while the combined AP-555 radio/class rows averaged about 69.3%. The
shortfall was entirely no-beacon time rather than missing QBSS CU, and all
selected rows in that run used observation-time fallback because TSF was
frozen or reversing. Location, capture-source, and class-window differences
remain confounded, so model-based adjustment or weighting is not justified by
the current captures.

Beacon rate by AP model remains a future analysis item, especially before
interpreting correlations with channel utilization or station count. That
work should use pre-selection in-scope observation evidence and report capture
coverage alongside rate; the current radio/class coverage field is not a
substitute for a raw capture-rate measurement.

Follow-up analysis of low-utilization Aruba ERM captures from an AP-635 6 GHz
radio and an AP-555 radio found normal active-flow cadence for each BSSID at
about 1.024 seconds (about 0.9765 observations/second), interrupted by recurring
ERM/controller-delivery pauses. Across 76 characteristic pauses, durations
ranged from 8.35 to 11.16 seconds with a 10.96-second median. Across 74
pause-start intervals, the median was 120.32 seconds, the shortest was 59.23
seconds, and the maximum was 182.99 seconds. This is consistent with an
underlying roughly one-minute pause opportunity that may be skipped, producing
observed one-, two-, or occasionally three-minute spacing.

The embedded TSF values in this evidence were unusable, so the data establishes
an Aruba ERM/controller/capture-path delivery interruption, not confirmed
over-the-air beacon loss. The reduced session-average rate must not be treated
as a slower transmitter beacon cadence. This remains evidence to explore, not
an AP-model correction rule: no rate correction, weighting, or imputation is
applied. Future beacon-rate work should preserve AP model, band, radio
configuration, capture profile, active-flow cadence, pause duration/frequency,
and class-window coverage so controller delivery can be separated from true
over-the-air behavior.

Fresh `2025-test` run `20261001T205900259971Z-97b6eeb5` published the revised
package successfully on 2026-10-01. The remaining acceptance action is owner
review of that report package before release cutover.

The project-level package and bounded radio/bin Parquet source are generated by
default. Per-radio/class time-series plots remain opt-in through
`--radio-class-plots` or `RADIO_CLASS_PLOTS=true`.

## 6 GHz evidence boundary

The existing Aruba AP-635 tri-band and Extreme common-AP 5/6 GHz captures are
enough to validate extraction, band classification, AP membership, empty-band
behavior, and report structure. They are not a representative empirical
comparison: the Aruba radios were captured sequentially, and the Extreme 5
and 6 GHz examples were captured on different dates. Before drawing
performance conclusions, use a project sample containing the same APs' 5 and
6 GHz radios during overlapping class windows for sufficient duration, with
QBSS CU and capture coverage available for both bands.
