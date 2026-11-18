# Beacon Analysis Roadmap

Updated: 2026-11-18. **Alpha 0.2 is the current, final owner-approved release.**
The report review and analysis scope are accepted. Current release notes are in
[Alpha 0.2](alpha-0.2-release-notes.md); future analysis work is tracked separately
in the [Alpha 0.3 roadmap](alpha_0_3_roadmap.md). Dated benchmark records retain
their original status and measurements.

## Alpha 0.2 completion

- [x] Supported one-command capture-to-report workflow, input preflight,
  classroom/AP/schedule intersection, consistent SSID decoding, weekend
  schedules and date-specific local-time display.
- [x] Reviewed Aruba ERM 0/3/6 and native 802.11 capture profiles, Aruba and
  Extreme AP-name adapters, and AP plus band/channel radio identity.
- [x] Observation-time class admission, ordering, bins and cadence slots under
  [ADR-0019](decisions/0019-observation-time-and-native-tsf-diagnostics.md).
  Native TSF supplies independent diagnostics and conservative duplicate
  recognition; it never retimes or removes measurements by a mapped clock.
- [x] Five authoritative artifacts, bounded extraction/selection, atomic
  publication, explicit failure cleanup, compact exports and provenance.
- [x] Corrected CU coverage, missingness, durations, actual/possible streaks and
  channel-utilization histograms/CDFs.
- [x] Complete station totals by radio, SSID, band and classroom, accepted
  30-second assistance, enrollment-weighted class peaks, bounded radio/bin
  detail and optional radio/class-session plots.
- [x] Radio admission capacity, detailed infrastructure/band comparisons,
  descriptive CU/station relationships and station-time with high/very-high CU.
- [x] Exploratory native/ERM capture-slot cadence and previous-window
  availability relationships, without treating capture gaps as proven RF loss.
- [x] Conservative missing-bin bounds for CU means/CDFs and station-time
  exposure conditional on available counts, preserving observed results.
- [x] Owner-reviewed offline HTML navigation, explanations, summary metrics,
  classroom/session detail and independent saved-run report refresh.
- [x] Four fresh v2 publication runs, source/link/storage audits and final
  regression: **638 regular plus 16 real-capture/TShark tests passed, no skips**.
- [x] Final owner scope/output approval on 2026-11-18 and a functional
  [public example-report site](https://yanzzee.github.io/WiFi-Beacon-analysis/example-reports/).

The [original-output mapping](alpha_0_2_original_output_inventory.md) explains
which purposes were kept, replaced, dropped or deferred. The
[Original comparison](original_to_corrected_migration.md) was performed once for
historical interpretation; maintaining or rerunning legacy functions is not a
release requirement.

## Accepted release boundary

The [decision index](decisions/README.md) is the source for metric definitions;
[current pipeline](current_pipeline.md) and [output inventory](output_inventory.md)
describe their implemented files and execution.

- CU describes observed channel activity; admission capacity describes the
  radio's advertised QoS medium-admission allowance and is not summed across
  radios.
- Session-active BSSID membership and complete-only station totals are used.
  Previous counts and leading counts can fill eligible gaps for up to 30
  seconds; accepted assisted counts participate in the approved statistics.
- Peak stations per enrolled uses classroom/class-session maxima divided by
  combined section enrollment. The project aggregate is enrollment-weighted;
  zero/missing enrollment is excluded and instructors are not added.
- CU means use their stated valid-duration or conditional denominators.
  Histograms/CDFs retain their own stated count or duration weights.
  Missing-bin ranges are conservative scenarios, not imputed CU or confidence
  intervals. Unknown station counts are outside station-time bounds.
- Pearson r and Spearman rho satisfy relationship reporting. Regression
  R-squared, unique-client counts, throughput, device capability and causal
  deployment effects are not claimed.
- Cross-project/year comparisons remain manual. Sampled 2.4 GHz is supported
  alongside 5/6 GHz; inferred or unconfigured radio scope is not invented.
- The verified analysis environment is Windows, 64-bit Python 3.13 and TShark
  4.6.6. Linux capture scripts do not establish broader analysis-platform support.

## Release evidence and publishing

The [2026-11-18 closeout](benchmarks/alpha_0_2_publication_closeout_2026-11-18.md)
records the four selected runs, 654-test result, unchanged measurement checks,
and the original 68.78-MiB prepared WebP package. The owner subsequently edited
the catalog, uploaded the reports and confirmed the live Pages site; that
prepared size/hash inventory is not a fresh measurement of later edits.

Earlier full-run/cached evidence is retained in the
[2026-10-29/31 checkpoint](benchmarks/alpha_0_2_release_readiness_2026-10-29.md),
[reporting review](benchmarks/alpha_0_2_reporting_review_2026-10-08.md),
[admission review](benchmarks/alpha_0_2_admission_reporting_review_2026-10-09.md),
[beacon-quality review](benchmarks/alpha_0_2_beacon_quality_review_2026-10-12.md),
[band comparison review](benchmarks/alpha_0_2_band_comparison_review_2026-10-13.md),
[affected-station review](benchmarks/alpha_0_2_affected_station_review_2026-10-16.md)
and [CU-bounds review](benchmarks/alpha_0_2_cu_bounds_review_2026-11-16.md).
Their timings have different boundaries and input scopes and are not isolated
feature-cost or runtime guarantees.

Synchronize the cleanup using the [branch instructions](release_branch_sync.md).
Public report files stay on their own branch under `example-reports/`.
Creating a release tag/GitHub Releases entry is an operational publishing step;
no additional report-review gate is being introduced.

## Next analysis phase: Alpha 0.3

Use the detailed [Alpha 0.3 roadmap](alpha_0_3_roadmap.md) before implementation.
Its prerequisites remain explicit and no new metric is accepted by this list:

1. Establish paired ERM/native identity, simultaneous listening exposure,
   capture continuity, timestamp alignment and independent native CU context.
2. Characterize active-flow ERM cadence and periodic delivery interruptions by
   AP model, sampled radio topology and capture path.
3. Measure CU/station relationships with subsequent capture availability,
   retaining missing predictors, paired coverage and unknown exposure.
4. Define and validate baseline-adjusted excess shortfall before describing it
   as estimated beacon loss; calibration requires representative evidence.
5. Evaluate missing-bin CU estimates on held-out data, separate from observed
   results and within the accepted conservative bounds.

Automated cross-year comparison, calibration thresholds and causal inference
require their own reviewed definitions.

## Further engineering work

Performance work should measure CPU, disk and memory over complete runs, plus
per-capture extraction and per-session selection/write costs, before optimizing.
Extraction has dominated measured runs, but input scope and concurrent load
prevent attributing runtime differences to a single report feature.

Additional vendor adapters require representative AP-name, timing, QBSS and
band/channel capture evidence. Inferring AP membership solely from MAC patterns
is separate work. A richer interface, automated cross-project browsing,
installable packaging and consumer setup automation remain future product
choices. Ctrl+C cleanup followed by restart is supported; resume is not planned.
