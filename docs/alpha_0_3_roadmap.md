# Alpha 0.3 roadmap: capture missingness and CU estimation

Created: 2026-11-16.

Status: future analysis plan. These research and implementation stages are
not implemented or release promises. Alpha 0.2 is final and owner-approved;
its implemented missing-bin bounds are tracked in
[their implementation inventory](cu_missing_bin_bounds_implementation_plan.md).

## Purpose and release boundary

Measure how capture availability relates to channel utilization and station
count, characterize sustained ERM delivery, and determine whether paired
native observations support useful estimates for missing ERM CU bins.

Core questions:

- Is ERM missingness associated with native-measured CU or station count?
- Does availability decrease gradually or change sharply at particular CU
  levels, and how consistent is that pattern across days and APs?
- What active-flow cadence and periodic delivery interruptions occur by
  AP model, sampled radio topology and capture path?
- How much observed shortfall exceeds an empirically supported baseline?
- Can a missing-bin CU model outperform simple alternatives on held-out data,
  with honest uncertainty and clearly stated applicability?

Observation timestamps remain authoritative under
[ADR-0019](decisions/0019-observation-time-and-native-tsf-diagnostics.md).
TSF must not retime measurements. Existing observed CU, station totals,
enrollment, admission capacity, correlations and streak definitions remain
independent of any additional estimated results.

## A3-01: Capture exposure, identity and provenance

Prerequisites:

- Identify the same transmitting AP/radio and, where practical, common BSSIDs
  across ERM and native capture paths.
- Establish simultaneous listening intervals, channels, file continuity and
  timestamp alignment. Distinguish confirmed coverage from unknown exposure.
- Retain receiver/exporter identifiers and capture mode when known. Do not
  infer self-transmit versus neighbour-receive from signal strength alone.
- Retain AP model, observed radio topology, ERM profile and known firmware/path
  context. Missing context remains unknown rather than being guessed.
- Keep physical duplicate uncertainty distinct from the existing one-effective-
  observation-per-slot accounting.

Prefer a bounded radio/capture-family/bin or window derivative with source
capture/frame lineage. Reuse extraction and per-session processing rather
than retaining a project-wide raw packet table or decoding the same capture
for each metric. Define source/metric versions and denominators before coding.

## A3-02: Simultaneous ERM/native case study

Start with the owner's `2026-hbll` capture data. The original combined run used
v1 timing and is historical evidence only. A fresh v2 run,
`20261116T184042802936Z-9e329a5c`, is now complete and is the retained publication
source, with two shared radios on an AP-635; see the
[closeout audit](benchmarks/alpha_0_2_publication_closeout_2026-11-18.md).
Use its source captures for the independent paired comparison or a bounded
paired re-extraction. The corrected native-only run can support verification.

Current beacon-quality windows separate capture-family availability but use
pooled CU/station context. They do not supply independent native CU during
an ERM gap. Build the paired reference explicitly rather than treating pooled
values as native observations.

For simultaneous, known-exposure windows, retain four availability states:
both paths available, ERM missing/native available, native missing/ERM
available, and both missing. Within each shared radio:

- Compare native CU distributions and means when ERM is available versus
  missing, including missing-bin bounds where the native reference is absent.
- Relate ERM availability to native-measured CU and complete station counts.
- Compare matched CU values when both paths are available to assess timestamp
  alignment and differences in sampling instants.
- Describe leading/interior/trailing gaps, durations and periodicity.
- Preserve missing-CU IE versus no-beacon states and fresh/stale station flags.

Native is an independent reference with its own limitations, not guaranteed
ground truth. A one-AP case study cannot establish a universal AP-model
correction or calibrate a different model such as AP-555.

## A3-03: Sustained ERM cadence baselines

Characterize active-flow cadence separately from sustained delivery including
pauses. Report nominal expected slots, raw views, capped effective occupied
slots, exposure duration, best sustained/typical rates and the distribution of
pause lengths/spacing. Use per-BSSID accounting and radio unions where
appropriate; do not add SSID rates as independent radio readings.

The [existing retained-capture observations](corrected_cu_report_package.md)
provide a starting hypothesis: active cadence near 1.024 seconds on sampled
AP-555/AP-635 radios, 76 characteristic pauses lasting 8.35-11.16 seconds
(median 10.96), and 74 pause-start intervals with median 120.32 seconds
(range 59.23-182.99). These are capture-path observations, not an accepted
model correction or a slower transmitted beacon interval.

Define the sustained baseline window and low-load/reference selection before
using a maximum rate. A short burst or additional neighbour/capture views can
inflate a literal maximum. Stratify by relevant model/topology/path evidence
and retain support counts and uncertainty. A missing stratum must not inherit
a correction from a different AP model by default.

## A3-04: CU/station-count relationships with ERM missingness

Use source-specific native CU during ERM gaps wherever available. Existing
previous-window correlations remain descriptive and are not sufficient to
identify the missing CU distribution.

- Show availability across native CU and station-count ranges, separately by
  supported model/band/path strata.
- Preserve the accepted 30-second station assistance and identify its use.
  Do not carry station counts indefinitely through long gaps.
- Control the comparison for capture exposure and characterize periodic
  baseline gaps before treating additional missingness as load-related.
- Report supporting APs, radios, days/classes, paired duration and reference
  coverage. Many neighbouring bins from one AP are not independent AP samples.
- Validate temporal patterns on held-out days/classes, with interval methods
  that respect temporal dependence if inferential intervals are introduced.

Do not declare 50% or 75% safe/unsafe reliability thresholds without evidence.
Correlated CU and capture shortfall do not establish CPU or forwarding
congestion; such attribution needs corresponding telemetry or an experiment.

## A3-05: Baseline-normalized capture shortfall

After an empirical baseline is supported, retain both the measured availability
and its difference from the matched baseline. One candidate relative measure
is `1 - observed_availability / baseline_availability`, for a positive baseline.
For example, 72% observed versus 90% baseline is a 20% relative shortfall, or
an 18-percentage-point absolute availability difference.

Define baseline provenance, applicable strata, uncertainty and zero-baseline
behavior. Preserve observations exceeding the estimated baseline for diagnostic
review; do not hide model failures through silent clipping. A one-sided excess
shortfall may be displayed separately with an explicit definition.

Label this capture shortfall relative to baseline, not confirmed RF packet
loss or proven loss caused by CPU, throughput or channel utilization. Retain
the distinction between BSSID capture-slot missingness and radio CU-bin
missingness; another BSSID can provide usable CU in the same radio bin.

## A3-06: Estimated CU for missing ERM bins

Proceed only if the paired study supports a useful model. Capture shortfall
alone does not determine the CU values that were missed.

- Define the target as CU in missing analysis bins, with the same accepted
  radio/bin geometry as the observed reports.
- Train using native CU during ERM-missing periods. Use predictors available
  in the intended ERM-only application, with explicit missing-predictor rules.
- Validate against held-out native observations using the actual ERM gap
  patterns. Compare prediction error and aggregate mean/CDF recovery against
  simple alternatives and the uncorrected observed summaries.
- Keep predicted CU within 0-100% and aggregates within the accepted
  conservative bounds. Do not force an upward correction if evidence does not
  support it. Never replace an existing observed value with a prediction.
- Report observed results, missing-bin estimates, combined estimated results,
  uncertainty, coverage and model applicability separately. A deterministic
  missing-bin envelope is not a statistical confidence interval.
- Leave unsupported models, long gaps and missing predictor combinations
  explicitly unestimated. Do not silently borrow a calibration from another
  project or AP model.

Collect broader paired AP/model evidence before claiming general applicability.
An exploratory release may legitimately stop at a case study and supported
baselines if estimated CU does not validate adequately.

## A3-07: Integration, efficiency and validation

Before implementation, record accepted decisions for exposure, paired matching,
baselines, estimation targets, model versions, uncertainty and fallback rules.
Prefer the existing CU/capture-quality report sections and bounded derivatives;
any broader report redesign needs its own review.

Requirements:

- Source and model provenance for every derived baseline or estimated result.
- Explicit weighting and cohort denominators in tables and methodology.
- Small synthetic fixtures for known delivery pauses, missingness correlated
  with CU, independent path loss, overlapping views and unavailable references.
- Logical CSV/Parquet regression checks proving observed outputs unchanged;
  no PNG byte comparisons.
- Saved-run refresh only where retained evidence supports reconstruction;
  otherwise require the appropriate bounded re-extraction or fresh run.
- Representative plot/text review, link validation and measured runtime/storage.
- Manual project/year comparison with matching definitions; no requirement
  for an automated cross-project system, GUI or new hosting service.

## Recommended order

Exposure/provenance -> paired case study -> sustained baselines and
CU/station relationships -> normalized capture shortfall -> validated
missing-bin CU estimates -> report integration and broader applicability.

Dependencies may share one bounded processing pass. Neither a normalized
shortfall nor a predictive correction is an Alpha 0.2 publication requirement.
