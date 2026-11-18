# Conservative CU missing-bin bounds: implementation inventory

Reviewed: 2026-11-16.

Status: All five stages implemented 2026-11-16: reusable weighted-mean bounds and
ordinary, equal-session, zero-station, paired-window and cadence-window mean
exports. Original observed logical values and canonical schemas are unchanged.
CDF numeric summaries, shaded CDF envelopes and mean-figure intervals are also
implemented. High/very-high station-time bounds now appear in entity/population
exports, the exposure chart and the existing CU subsection. HTML/text displays
and supported historical-report backfill are integrated. See
[ADR-0020](decisions/0020-conservative-cu-missing-bin-bounds.md) for the fields
and the separate `beacon.metric/cu-missing-bin-bounds/v1` component identifier.

## Required behavior

- Preserve every existing observed value, weighting rule, threshold, eligibility
  rule, correlation, selected observation and streak calculation.
- Add lower/upper bounds next to reported CU means, including less prominent
  means in downloadable tables. Display existing point values as observed.
- Retain observed CU CDF lines, add lower/upper curves and a shaded envelope,
  and include bounded values in the corresponding numeric summaries.
- Use 0% and 100% CU as the two extreme scenarios for missing CU bins. Include
  both no-beacon and beacon-with-unavailable-CU states in missing CU weight.
- Add high/very-high station-time bounds where complete station counts exist.
- Use the existing report sections and layout. No new report section is needed.
- Keep deterministic missing-bin bounds separate from confidence intervals,
  inferred CU, ERM baseline normalization and capture-path calibration.

## Mathematical contract for the implementation

For a specified in-scope radio/bin population, let `T` be total expected
weight, `V` valid-CU weight, `M = T - V` missing-CU weight, and `S` the sum of
observed CU percentages multiplied by their weights.

- Existing observed mean: `S / V`, unavailable when `V = 0`.
- Lower mean: `S / T`.
- Upper mean: `(S + 100 * M) / T`.
- A known nonempty population with no usable CU has bounds 0% to 100% and an
  unavailable observed mean. A nonexistent/empty population has unavailable
  bounds; it is not a measured zero.

For a CDF threshold `x` below 100%, let `A(x)` be valid weight at or below `x`:

- Existing observed CDF: `A(x) / V`.
- Lower CDF: `A(x) / T`.
- Upper CDF: `(A(x) + M) / T`.
- Enforce the support endpoints: below 0% both bounds are zero; at or above
  100% both are one for a nonempty population. With no valid CU, show the
  possible envelope without inventing an observed line.

Retain each existing CDF's weighting. Radio/bin CDFs give each bin equal
weight; infrastructure-cohort CDFs use bin duration. Consequently missing-bin
counts and missing duration must remain distinct. Existing threshold cutoffs
remain raw CU 128 and 192; do not round/redefine them while adding labels.

These ranges describe unknown analysis bins. They do not account for missed
variation inside bins that contain a selected reading, uncertainty in the
beacon's own measurement, or unobserved radios outside the accepted sample.
They are not 95% confidence intervals and do not imply that an endpoint is
likely. Full coverage makes the bounds coincide with the observed statistic.

## Complete modification inventory

### 1. Shared bounds and component accounting

Introduce one small reusable calculation module for weighted means, CDF
envelopes, and bounded threshold shares. Use existing component sums rather
than rounded displayed means. Validate nonnegative weights, `V <= T`, CU
support, zero/full coverage, no valid CU and nonexistent populations. Preserve
bounded working sets and avoid an additional capture decode.

### 2. Core radio/class-session means and review exports

Sources: `corrected_summary_output.py`, `corrected_csv_exports.py`.

The canonical radio/class summary contains `average_cu_raw` and
`average_cu_ratio`, with exact class/valid/missing/no-beacon durations.
`exports/csv/radio_class_summary.csv` exposes these means directly. Companion
report bounds must cover these source means as well as their percent-formatted
report copies. Preserve the original fields and observed calculations. Prefer
derived companion fields to changing the five authoritative artifact schemas;
document any derived fields added to the CSV review export.

### 3. CU report radio and population means

Source: `corrected_cu_reports.py`.

- `reports/cu_coverage_streak/radio_class_report.csv`: each radio/class-session
  `average_cu_percent`.
- `band_population_report.csv`: all existing overall, band and hardware/radio
  population `average_cu_percent` values, retaining their duration weighting.
- `radio_average_cu_distribution.png`: label the distribution as observed
  radio/class-session means and provide the companion mean bounds in its
  source. Review a legible endpoint/interval presentation; endpoint histograms
  are not automatically minimum/maximum counts for each histogram bucket.
- `radio_cu_vs_coverage.png`: retain observed points. After the owner reviewed
  the interval display, remove its plotted intervals for legibility; retain
  each radio/class-session's companion mean bounds in the source table.

### 4. Overall CU reading CDF

Source: `corrected_cu_reports.py`.

Update `radio_bin_cu_cdf.png`: preserve its observed step curve, add lower and
upper step curves and a shaded missing-bin range. Add observed/lower/upper
numeric values at the existing 50% and 75% markers. The denominator is all
expected radio/bin entries in the same accepted population, not the number of
beacons or BSSIDs. Expected bin counts must handle partial terminal bins and
mixed configured bin widths correctly.

### 5. CU reading CDFs by band

Source: `corrected_cu_reports.py`.

Apply the same changes to `radio_bin_cu_cdf_by_band.png`, retaining the existing
count weighting and observed-band panel rules. Distinguish an absent band from
a present population whose CU is entirely unavailable. Do not invent a
measurement for an absent band.

### 6. Band, cohort, model and classroom means

Source: `corrected_band_comparison_reports.py`.

Append companion mean-bound fields to:

- `radio_class_comparison.csv`;
- `population_summary.csv` (overall, band, AP cohort and classroom cohort);
- `model_population_summary.csv`;
- `classroom_band_summary.csv`.

Preserve `mean_cu_percent` and integer component sums. Update the mean-CU panel
in `cohort_metrics.png` with observed values and lower/upper intervals. Include
all sampled bands and existing population rows, retaining no-data states.

### 7. Equal-AP and equal-classroom/session means

Source: `corrected_band_comparison_reports.py`, `_summary`.

The same summary exports also contain `equal_ap_session_mean_cu_percent` and
`equal_classroom_session_mean_cu_percent`. These average entity/session means;
they are not pooled duration-weighted means. Calculate the component intervals
at the entity/session grain and aggregate using the same equal weights and
eligibility as the reported observed statistic. Record that eligibility
explicitly; do not silently change which entities participate.

### 8. CU conditional on zero associated stations

Source: `corrected_band_comparison_reports.py`, `_metrics` and `_derived`.

`mean_cu_with_zero_stations_percent` requires its own denominator: time with
an available station count of zero, including periods where CU is missing.
Current exports retain valid-CU/zero-station duration but not that complete
conditional denominator. Accumulate it from the bounded radio/bin view for
new runs; use a narrow saved-detail read when refreshing older runs if the
necessary source is present. Unknown station counts cannot establish zero
stations. Leave the existing conditional observed mean unchanged.

### 9. Matched 5/6 GHz means

Sources: `corrected_band_comparison_reports.py`, `corrected_report_overview.py`.

`matched_ap_radio_sessions.csv` contains `matched_mean_5_cu_percent` and
`matched_mean_6_cu_percent`; `matched_ap_cu.png` plots them. The HTML also
reports the equal-comparison mean signed difference.

Keep the existing common-valid-bin comparison intact. Missing-pair scenarios
need explicitly named companion bounds with a stated expected paired window;
full-class means must not be presented as the same estimand as the original
common-valid-only means. Keep the two radios' alignment and the existing
equal-comparison weighting. Any displayed bounds for the difference must be
derived from that paired definition rather than an unrelated population's
coverage. No regression or correlation recalculation is part of this work.
After owner review, the matched scatter remains observed-only for legibility;
the paired-window ranges remain in the table and difference-histogram shading.

### 10. Infrastructure-cohort CU CDFs

Source: `corrected_band_comparison_reports.py`, `build_cohort_cu_cdf`.

Update `cu_cohort_cdf.png` with observed/lower/upper lines and shaded ranges.
Use duration totals for each AP/classroom cohort, alongside the exact observed
raw-CU frequency table in `cu_distribution.csv`. Preserve the single-view
behavior when AP and classroom cohorts coincide. Plot each infrastructure
cohort in its own panel with shared axes; add a classroom-cohort row only when
mixed classrooms make that comparison distinct. Provide the numeric bounds
needed to reproduce the figure and threshold labels.

### 11. Capture-quality window and previous-window CU means

Source: `corrected_beacon_quality_reports.py`.

`beacon_quality_windows.parquet` exports `cu_mean_percent` and
`previous_window_cu_mean_percent`. Add companion bounds for current and
previous windows using exact window/valid durations, including a partial
terminal window. The first window has no preceding in-class window; it must
not acquire a fabricated previous-window range. Keep pooled CU context,
observed predictor buckets, pair eligibility and correlation calculations
unchanged. Independent capture-family CU context belongs to Alpha 0.3.

### 12. High/very-high station-time bounds

Source: `corrected_affected_station_reports.py`.

Existing `entity_session_exposure.csv` and `population_exposure.csv` already
retain `available_station_time_us`, `paired_station_time_us`,
`high_station_time_us` and `very_high_station_time_us`.

For each threshold, let `A` be association-time with available complete station
counts, `P` that association-time with usable CU, `U = A - P`, and `H` the
observed association-time at/above the threshold:

- Existing observed exposure remains `100 * H / P`.
- Lower/upper exposure association-time is `H` to `H + U`.
- Lower/upper exposure percentage over available station-time is
  `100 * H / A` to `100 * (H + U) / A`.

Use the existing high/very-high cutoffs and accepted stale station counts.
These ranges are conditional on available station counts; they do not bound
station-time in periods with unknown counts. Keep that limitation and station
coverage visible. No positive denominator means an unavailable percentage,
not a measured 0%. Preserve existing observed percentages, peaks and streaks.
Add the ranges to the existing exposure table/graphic and CU subsection.

### 13. HTML mean displays, CDF metrics and existing explanatory text

Source: `corrected_report_overview.py`.

Update all of the following in their current sections:

- separate Overview mean-CU cards for the overall sample and each sampled band;
- Channel utilization reported mean and CU-threshold/CDF figures;
- station-time high/very-high CU cards, table and figure;
- band and AP-infrastructure-cohort result tables;
- AP-model/configuration table;
- classroom-by-band summaries;
- class-session pooled mean radio CU in each classroom session table;
- matched-radio mean/difference presentation;
- descriptions and legends for affected mean/CDF figures.

The class-session pooled mean is calculated in `_session_radio_stats`; it
needs its own pooled total expected radio duration, not a mean of per-radio
coverage percentages. Use separate observed and possible-range metrics or
columns; keep numbers and units in values and explanations in subsection prose.
Overview subtext says “Between … and … possible.” No
new navigation item or report section is proposed.

### 14. Text summary, refresh, metadata and compatibility

Sources: `corrected_cu_reports.py`, `corrected_cached_plots.py`,
`corrected_report_refresh.py`, `corrected_run.py`, `corrected_capture_run.py`.

- Add bounds wherever `reports/run_summary.txt` reports a CU mean or high-CU
  station-time figure, including cohort lines.
- Make `report --from-run` derive supported bounds from saved exact components
  and regenerate affected tables/figures in its new bundle.
- Rebuild the overall CU CDF too; current cached plot refresh handles only
  selected existing figures and does not redraw that overall CDF.
- Prefer compact CSV components; use bounded/narrow Parquet reads only for
  conditional denominators absent from older exports. Do not fill an absent
  denominator using an assumed default bin width or rounded percentage.
- Label bounds unavailable when required evidence is absent. Preserve the
  source run, manifest and observed values during refresh.
- Register additive report schemas/bounds methodology and renderer provenance
  where needed. Do not mislabel historical exports as containing the feature.

## Recommended implementation sequence

- [x] Shared math and ordinary mean exports: reusable mean helpers, radio/session
   and population/cohort/model/classroom means, exact denominator accounting.
- [x] Remaining mean definitions: equal-entity/session, zero-station, matched-radio
   and cadence-window means; preserve their distinct conditioning/weighting.
- [x] CU CDFs and affected mean figures: numeric lower/upper values, all three CDF
   plot groups, observed legends, step envelopes and mean interval displays.
- [x] Station-time bounds: existing entity/population exports, plot and CU
   subsection; explicit availability-conditioned denominator.
- [x] Complete presentation and integration: every HTML/text occurrence, saved-run
   backfill/refresh, schema/methodology records and representative report review.

Each step must keep previously observed logical values unchanged, including
nulls. The feature is not complete until every inventory item is covered.

Stage 5 covers the Overview, CU and CDF summaries, band/cohort/model/classroom
tables, pooled class-session means, matched-window difference ranges and the
text summary. `report --from-run` backfills supported companion fields into a
separate copy. Missing denominators remain unavailable. See the
[representative review](benchmarks/alpha_0_2_cu_bounds_review_2026-11-16.md)
for preservation checks, rendering and refresh runtime.

## Validation and release evidence

- Focused numerical fixtures: zero, partial and full coverage; all CU missing;
  no radios; real observed zeros; terminal partial bins; mixed bin widths;
  unequal group coverage; exact 0/100% CDF support and threshold conventions.
- Conditional fixtures: missing CU with zero/unknown/stale station counts;
  unequal radio eligibility; no common matched bins; first/last cadence windows.
- Station exposure fixtures: known station counts during missing CU, all CU
  missing, unknown station counts, zero station-time and very-high nesting.
- Compare every old observed field against its previous logical values while
  checking additive bounds separately. Do not compare PNG bytes.
- Verify full-run and saved-run refresh agree wherever source evidence suffices.
- Inspect representative CDF shading, interval visibility, cards and tables at
  desktop/narrow widths; validate all links and measure incremental cost.
- Update [current pipeline](current_pipeline.md),
  [output inventory](output_inventory.md),
  [CU methodology](corrected_cu_report_package.md),
  [release notes](alpha-0.2-release-notes.md) and accepted decisions as the
  corresponding implementation lands.

Unchanged calculations include admission capacity, station totals and their
30-second assistance, enrollment comparisons, station adoption, CU/station
ratios and correlations, capture-slot availability and existing streaks.
Beacon-gap, admission-capacity and station-adoption CDFs are not CU CDFs.

ERM/native calibration and estimated missing CU are separately planned in
[the Alpha 0.3 roadmap](alpha_0_3_roadmap.md).
