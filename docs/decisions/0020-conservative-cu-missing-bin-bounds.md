# ADR-0020: Conservative missing-bin CU bounds

- Status: Owner-requested scope accepted 2026-11-16; all five stages implemented.
- Scope: additive missing-bin bounds; existing observed calculations unchanged.
- Implementation inventory: [stages and affected outputs](../cu_missing_bin_bounds_implementation_plan.md).
- Release boundary: final owner report review and publication remain separate.

## Decision

Use 0% and 100% CU as the extreme values of missing analysis bins. Add
companion lower/upper values to observed CU summaries rather than replacing
measurements, altering coverage, interpolating CU or applying an ERM model.
Include both no-beacon and unavailable-CU duration in missing weight.

For exact total/valid weights `T`/`V`, and an observed weighted CU sum `S` in
percent units, the mean range is `[S/T, (S + 100*(T-V))/T]`. Keep the existing
observed mean `S/V` and its original eligibility. No expected population means
unavailable bounds. A nonempty all-missing population has bounds 0-100% but
no observed mean.

These are deterministic missing-bin ranges, not confidence intervals or
estimates of the missing values. They do not cover missed variation within
an observed bin, uncertainty in the beacon's CU measurement, or unobserved
infrastructure outside the accepted sample. Thresholds, station assistance,
admission capacity, correlations and streaks are unchanged.

## Stage 1 implementation

`corrected_cu_bounds.py` supplies reusable weighted-mean bounds with explicit
support/weight validation and Python-integer arithmetic for large products.
The component identifier is `beacon.metric/cu-missing-bin-bounds/v1`.

New ordinary mean companions are:

- `exports/csv/radio_class_summary.csv`: `average_cu_raw_lower_bound`,
  `average_cu_raw_upper_bound`, `average_cu_ratio_lower_bound`,
  `average_cu_ratio_upper_bound`.
- `reports/cu_coverage_streak/radio_class_report.csv` and
  `band_population_report.csv`: `average_cu_percent_lower_bound` and
  `average_cu_percent_upper_bound`.
- `reports/band_comparison/radio_class_comparison.csv`,
  `population_summary.csv`, `model_population_summary.csv` and
  `classroom_band_summary.csv`: `mean_cu_percent_lower_bound` and
  `mean_cu_percent_upper_bound`.

Each affected table carries `cu_bounds_metric_id`. Band-comparison context
records the implemented ordinary-mean stage, scenarios and denominator;
methodology documents describe the partial implementation.

Band-comparison sums retain the exact existing integer raw-CU/duration
components. Canonical radio summaries store a full-precision raw mean and
exact durations, not an integer weighted sum. Their review bounds use those
persisted values without rounding a displayed mean or fabricating an exact
integer numerator. This retains ordinary floating-point representation limits.

The five canonical artifact schemas, source fields and existing metric IDs
remain unchanged. The added derived CSV fields are compatible extensions
under ADR-0006's open-column contract; their separate component ID identifies
their meaning. Core CSV identity/schema/metric columns still describe the
canonical source, not an alteration to its observed mean calculation.

## Stage 2 mean definitions and exports

The same bounds component now covers these additional derived outputs:

- `equal_ap_session_mean_cu_percent_{lower,upper}_bound` and the corresponding
  `equal_classroom_session_*` fields in population/model/classroom summaries.
  Calculate each eligible entity/session's duration-based interval and average
  the endpoints with the original equal weights. Preserve existing eligibility
  (positive valid CU duration) and counts. With no eligible entities the equal-
  session mean and its bounds are unavailable; do not add all-missing entities
  to that statistic. If every eligible entity is fully observed, both endpoints
  equal the observed mean exactly.
- `mean_cu_with_zero_stations_percent_{lower,upper}_bound`, with new
  `zero_station_available_duration_us` components in radio and aggregate
  comparison tables. This denominator includes missing-CU time only when a
  complete station count is available and zero; accepted stale counts qualify.
  Unknown station counts do not establish a zero-station condition.
- `paired_window_mean_{5,6}_cu_percent_{lower,upper}_bound` and
  `paired_window_6_minus_5_cu_percentage_points_{lower,upper}_bound` in
  `matched_ap_radio_sessions.csv`. Their denominator is the existing
  `possible_duration_us`. Only common-valid-bin observations constrain these
  conservative paired-window scenarios. Persist `common_5_cu_raw_duration_sum`
  and `common_6_cu_raw_duration_sum`; an unpaired reading is not used to tighten
  this shared-comparison range. Keep observed `matched_*` fields unchanged.
  The difference interval subtracts the opposite radio's endpoints. No common
  CU gives unavailable observed means, radio bounds 0-100%, and a difference
  range of -100 to 100 percentage points. The distinct field names identify
  the broader paired-window target rather than relabeling the common-only mean.
- `cu_mean_percent_{lower,upper}_bound` and
  `previous_window_cu_mean_percent_{lower,upper}_bound` in
  `beacon_quality_windows.parquet`. Retain exact current/previous raw weighted
  sums and `previous_window_duration_us`. Current bounds use the actual current
  duration; previous bounds shift the preceding window's own interval. The
  first window has no previous in-class interval. Absent context components
  leave bounds unavailable; a known window with zero valid CU has bounds
  0-100%. Full-coverage bounds equal the observed window mean exactly.

Cadence context remains pooled across capture families. Predictor buckets,
pair eligibility and all existing correlations remain unchanged. The new
Parquet columns are compatible additions to the derived window schema; the
five canonical artifact schemas remain untouched. No extra capture decode is
required; integer sums are retained within the existing bounded session pass.

## Stage 3 CDF summaries and mean figures

The overall and by-band radio/bin CU CDFs retain their observed step curves and
equal-bin weighting. Cohort CDFs retain duration weighting. Each adds minimum
and maximum step curves, shaded envelopes, and observed/minimum/maximum values
at the existing raw `<128` and `<192` cutoffs (50%/75% markers).

For cumulative observed weight `A`, total expected weight `T` and missing
weight `M`, bounds below 100% CU are `A/T` to `(A+M)/T`. Both endpoints reach
one at 100% CU. A present population with no valid CU has a possible envelope
and no observed curve; an absent population has unavailable bounds and no
curve. Full coverage collapses the envelope onto the observed line.

Both `reports/cu_coverage_streak/cu_cdf_bounds.csv` and
`reports/band_comparison/cu_cdf_bounds.csv` export the two threshold rows per
population. Fields include `weighting`, `valid_weight`, `expected_weight`,
`cdf_observed_percent`, `cdf_lower_bound_percent`, `cdf_upper_bound_percent`,
the raw exclusive cutoff, and `cu_bounds_metric_id`. The first file uses
`radio_bins` weights; the second uses `radio_duration_us`. They are distinct
statistics even when their displayed values happen to agree.

The CU radio report adds nullable `expected_radio_bin_count`; its population
report adds `expected_radio_bin_values`. Exact per-radio counts come from
`radio_class_comparison.csv`. When that table is absent, a single explicitly
configured width permits `ceiling(class_duration_us / width)`, counting a
terminal partial bin once. Mixed widths without per-radio counts leave count
bounds unavailable. No width is inferred from coverage or assumed by default.

Mean-CU cohort bars retain their observed values and add duration-based
intervals. CU-versus-coverage points remain observed-only after the owner's
presentation review; their companion bounds remain in the source tables.
The radio/class-session mean histogram
keeps its observed counts and adds conservative per-bucket count ranges from
the mean intervals: a bucket's minimum counts intervals wholly within it;
its maximum counts every intersecting interval. These maxima need not occur
together. Matched-radio points and observed difference histograms keep their
common-valid eligibility. The scatter plot is observed-only; histogram shading
describes the separately named complete paired-window scenario, using the same
eligible comparisons. Cohort CDFs use separate panels to avoid overlapping
ranges. HTML cards and tables separate observed numbers from possible ranges;
the Overview uses separate cards for sampled bands and very-high CU exposure.

Saved-run plot regeneration reads these compact components, including the
overall CDF. Exact saved population counts/durations support CDF ranges for
older runs; missing expected components leave ranges unavailable. Mean ranges
are drawn when their companion fields exist. Complete historical mean-field
backfill and all HTML/text metric displays were completed in Stage 5 below.
No capture decode or project-wide detail table is added by Stage 3.

## Stage 4 station-time with high/very-high CU

`entity_session_exposure.csv` and `population_exposure.csv` in
`reports/affected_stations/` retain all existing observed values and add:

- `missing_cu_station_time_us`: available association-time without usable CU,
  `available_station_time_us - paired_station_time_us`;
- `high_station_time_us_lower_bound` / `high_station_time_us_upper_bound` and
  corresponding `very_high_*` quantities in exact association-microseconds;
- `high_exposure_percent_lower_bound` / `high_exposure_percent_upper_bound`
  and corresponding `very_high_*` percentages;
- `cu_bounds_metric_id`, identifying the same bounds component as CU means/CDFs.

For available association-time `A`, paired association-time `P`, and observed
qualifying association-time `H`, quantity bounds are `H` to `H + A - P`.
Percentage bounds are `100*H/A` to `100*(H+A-P)/A`. The observed percentage
remains `100*H/P`. Thus 30 high-CU association-hours among 80 paired hours,
plus 20 hours with missing CU, yields 37.5% observed and bounds 30-50% over
the 100 available association-hours. These are different denominators, not
a confidence interval around a measurement.

Both no-beacon and unavailable-CU time participate when a complete radio
station count is available, including accepted stale counts. Availability is
assessed per radio; entity/population rows pool the original radio components.
They do not require every entity member to be simultaneously available as
the separate peak calculation does. Unknown station counts cannot bound
association-time in those periods, even when CU is known. Missing CU with
zero stations contributes no missing association-time. With positive `A` and
no CU, percentages are unavailable observed and bounded 0-100%. When `A=0`,
percentages are unavailable; quantity bounds are zero within the empty
known-count population. Very-high quantity/percentage bounds remain nested
inside high bounds. Integer quantities remain exact above int64.

The exposure chart retains observed bars and adds min/max whiskers, explicitly
distinguishing paired versus available station-time. Its scale includes the
upper bounds; no observed bar is invented for all-missing CU. The existing CU
subsection shows bounded percentages and association-hours, station-count
coverage, CU coverage of available station-time, and the conditioning limits.
No new report section or peak presentation is introduced. Overview cards and
the text summary were completed in Stage 5 below.

New runs export these companions within the existing seven-file package;
canonical schemas and observed metric IDs stay unchanged. `context.json`
records the component, denominator, scenarios and exclusion of unknown station
counts. Saved-plot regeneration uses bound companions when present and records
the shared math/renderer hashes. Historical table backfill is covered by Stage 5.

## Stage 5 presentation and saved-run integration

All HTML CU mean displays show the observed value and minimum/maximum range:
Overview and per-band card detail, CU reported metrics, band/cohort/model/
classroom summaries, and pooled class-session radio means. The latter pool
exact raw sums and valid/expected durations across radios, including all-
missing radios in expected weight; they do not average coverage percentages.
The matched summary keeps its observed common-valid mean difference and
separately averages paired-window difference endpoints with the same eligible
comparisons and equal weights. CDF numeric displays retain their existing
observed cutoffs/weighting alongside the corresponding range. High/very-high
station-time cards and text summaries retain their observed values and state
the availability-conditioned bounds. Existing sections and unrelated wording
remain intact apart from explanations needed for these additions.

`corrected_cu_bounds_refresh.py` operates on independent refresh copies:

- Ordinary/equal-session bounds use saved component sums and exact durations.
  Core review exports use their persisted full-precision raw means, with
  compact canonical Parquet as a fallback when the review CSV is absent.
- Expected bin counts use saved per-radio counts or one explicitly recorded
  width; mixed widths without counts are never replaced by a default.
- If needed, one narrow pass over saved radio/bin detail obtains zero-station
  denominators and exact matched common-CU sums. Matched joins retain at most
  one class session. Saved detail must keep each class session contiguous, as
  the application writer does; inconsistent paired durations fail clearly.
- If detail is absent, zero-station bounds stay unavailable. Matched means
  can supply unrounded persisted percent means plus exact common/possible
  durations. No exact integer numerator is fabricated from a floating mean.
- Cadence-window Parquet is processed in bounded batches. Exact raw sums are
  preferred; older windows can use their unrounded persisted Parquet means
  and exact valid/window durations. First lag remains unavailable; a partial
  terminal window uses its actual duration, while its lag uses the preceding
  window's duration. Unsupported raw-sum fields stay null.
- CU CDF summaries use saved count/duration marginals. Core raw-count exports
  can support count CDFs when the comparison marginal is absent; they cannot
  establish per-value duration weights. Unsupported bounds remain unavailable.
- Every original CSV cell and original Parquet field is retained. Only
  companion columns and the two CDF summary CSVs are added. The text report
  replaces only its existing band/exposure summary paragraphs. Methodology
  notes distinguish the refreshed presentation from historical source status.

`report_refresh.json` retains the original source-manifest hash and adds the
bounds component, added-column lists, source/result hashes, loader/math hashes
and the persisted-mean policy. The copied manifest continues to describe the
source analysis. Full runs register their additive exports normally. No
capture decode, selection change, guessed observation, or original-run rewrite
is introduced. Floating-point persisted-mean fallbacks retain their ordinary
representation limits; these are not modeled estimates of missing CU.

## Verification and publication

Tests cover the 40% observed/80% coverage example (32-52%), observed zero,
full coverage, no CU, absent populations, partial terminal bins, invalid
components, full-precision persisted means and integer products above int64.
The historical band-comparison golden verifies all pre-existing logical
columns unchanged. A separate mean-bounds golden freezes the additive tables.
Core writer tests verify that bound columns appear only in the review CSV,
not in the canonical Parquet.

The Stage 1 focused and integration run passed 163 tests, with one external
capture test deselected. Full-coverage tests require the bounds to equal the
observed means exactly, including floating-point unit conversion. A local
microbenchmark evaluated 10,000 scalar CU mean bounds in 0.0203 seconds; this
is helper timing, not a full-project runtime measurement. No additional
capture decode is introduced.

The Stage 2 focused and integration run passed 194 tests, with one external
capture test deselected. New cases cover equal-weight eligibility, known-zero
versus unknown station counts, stale zero counts, no common paired CU, exact
full-coverage endpoints, mixed capture families, partial terminal windows and
the first window's absent lag. Explicit projections verify every old band
and quality logical field and all Stage 1 bounds unchanged. Separate
conditional-band and window-mean goldens freeze the additional fields.

Stage 3 adds a separate logical CDF golden, preserving all prior observed and
mean-bound goldens. Tests check count/duration weighting, support endpoints,
all-missing/absent populations, exact counts with mixed widths, partial bins,
threshold conventions, unchanged plotted observations, and saved-plot sources.
Representative CDF and mean figures are visually reviewed without comparing
PNG bytes.

The Stage 3 integration check passed 225 tests with one external-capture test
deselected. After the final all-missing mean-label refinement, the focused
plot, band-comparison and HTML checks passed 107 tests. Local documentation
links were checked, and seven representative CDF/mean figures were inspected.

Stage 4 checks hand-calculated partial/full/no-CU ranges, measured zeros,
zero/unknown/stale station counts, no-beacon versus missing-field gaps, empty
populations, terminal partial bins, nesting, large integer quantities, unchanged
observed bars, range-inclusive axes, HTML denominator explanations and saved
plot dependencies. A separate `affected_stations_cu_bounds_v1.json` freezes
the additive exports; projecting out only the new columns still matches every
original field in `affected_stations_v1.json`, including all peak results.
The Stage 4 integration check passed 267 tests with one external-capture test
deselected. Partial-coverage and all-missing charts were inspected, generated
HTML links were validated, and local documentation links were checked.

Stage 5 preservation and representative rendering/runtime evidence is recorded
in the [bounds review](../benchmarks/alpha_0_2_cu_bounds_review_2026-11-16.md).
Existing published runs remain unchanged until an explicit new report is
generated and, separately, selected for publication.

ERM/native calibration and inferred CU are planned separately in
[Alpha 0.3](../alpha_0_3_roadmap.md).
