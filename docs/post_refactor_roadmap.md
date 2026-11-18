# Beacon Analysis Roadmap

The post-refactor corrected pipeline is now the supported workflow. This file
tracks only remaining work; accepted semantics are recorded in the ADRs under
[`decisions/`](decisions/README.md).

## Current release boundary

Owner-requested addition, 2026-11-16: conservative missing-bin bounds for all
reported CU means and CU CDFs, with station-time high-CU bounds where counts
are available. The [implementation inventory](cu_missing_bin_bounds_implementation_plan.md)
records the full affected surface and staged work. All five stages now implement
shared bounds, ordinary/conditional/paired/cadence mean exports, numeric CDF
summaries, shaded CU CDFs and mean-figure intervals, plus availability-conditioned
station-time ranges in exposure exports/chart and the CU subsection. HTML/text
presentation and supported historical-report backfill are integrated. Existing observed values and
report sections must be preserved. ERM calibration and estimated missing CU
are separately planned in [the Alpha 0.3 roadmap](alpha_0_3_roadmap.md).

Status: Alpha 0.1 remains published. Alpha 0.2's reporting candidate is
implemented with full-project validation; final owner sign-off and publication
remain separate. See [candidate release notes](alpha-0.2-release-notes.md) and
the [release-readiness record](benchmarks/alpha_0_2_release_readiness_2026-10-29.md).

Reconciled 2026-11-13: the current report includes the accepted November
presentation refinements and saved-run refresh. See
[reporting and publication](reporting_and_publication.md) for the current
section order, displays, measured linked-file footprint and planned public
report boundary. Dated benchmark counts below describe their original runs.

Implemented and owner-reviewed:

- `beacon_analysis.py run` as the one-command workflow;
- direct AP/classroom/schedule CSV loading and physical-session expansion;
- Aruba ERM type 0/3/6 and native 802.11 capture-profile detection;
- reviewed Aruba and Extreme AP-name adapters;
- configured-hostname scope filtering;
- BSSID to radio to AP to classroom identity using AP name plus band/channel;
- observation-time admission, ordering and bin placement, with independent
  native TSF diagnostics and conservative duplicate recognition (ADR-0019);
- first-observed BSSID/bin selection with terminal partial bins;
- corrected CU coverage, missing-CU/no-beacon, high/very-high duration, and
  actual/possible streak semantics;
- five durable artifacts, compact CSVs, project report package, run summary,
  manifest, atomic publication, failure cleanup, and create-only runs;
- bounded concurrent extraction and bounded per-session selection; and
- dedicated corrected semantic golden plus optional real-capture replay.

The verified environment is Windows with Python 3.13 and TShark 4.6.6. Other
platforms may work but are not yet claimed by release evidence.

Post-review timing change (2026-11-15): ADR-0019 advances core timing and
capture-quality metrics to v2 after the HBLL forward-TSF-jump investigation.
The owner has now completed four fresh v2 publication candidates, audited in
the [2026-11-18 closeout record](benchmarks/alpha_0_2_publication_closeout_2026-11-18.md).
These satisfy the fresh full-project checkpoint. Earlier benchmark runs remain
dated evidence of v1 behavior; report refresh cannot repair those runs.

## Alpha 0.1 checkpoint

Release validation, real-capture replay, a complete end-to-end CLI smoke run,
documentation review, and public source-checkout publication are complete.
The release does not claim an installable package or comprehensive vendor and
capture-platform support.

Ctrl+C with cleanup and restart-from-beginning is the accepted cancellation
model. Resume is not planned. Exact within-capture percentage remains deferred
because it requires another decode pass or a larger streaming redesign.

## Corrected Reporting Expansion candidate (Alpha 0.2)

The goal is to recover the useful analytical coverage of the original reports
without recreating the original calculation pipeline or requiring identical
legacy results. Corrected artifacts and semantics remain authoritative.

Recommended sequence:

1. Inventory every original output from the public Original release and assign
   `keep`, `replace`, `drop`, or `defer`, with a brief reason and the corrected
   source grain for each retained purpose.
2. Implement the station-count semantics accepted in
   [`ADR-0010`](decisions/0010-station-count-rollup-and-staleness-semantics.md)
   as a bounded derived view, then add station summary tables and reports.
3. Decide the smallest reusable radio/bin reporting source needed for corrected
   time-series and detailed class/radio reports. Prefer a bounded derived view;
   add a durable artifact only if repeated reconstruction is too expensive or
   cannot preserve the accepted summary semantics.
4. Implement report families incrementally: all-class observations,
   class-session detail, radio summary/time series, then additional 5/6 GHz and
   beacon-rate diagnostics.
5. Give each family human-readable tables, bounded plots, focused fixture
   expectations, manifest registration, and methodology text.
6. Document one informational Original-versus-corrected comparison for people
   interpreting old projects or rerunning them with current versions. Reuse
   the frozen same-input comparison already executed before legacy cleanup.
   Numerical identity is not required, and this is not a recurring release
   gate or a reason to retain legacy functions.
7. Measure complete-run time and storage after each report family, and publish
   Alpha 0.2 only after owner review of representative full-project output.

The step-1 inventory and implemented-purpose mapping are recorded in
[`alpha_0_2_original_output_inventory.md`](alpha_0_2_original_output_inventory.md).
Accepted metric decisions are in ADR-0010 through ADR-0018, not inferred from
the inventory. Unaccepted/deferred purposes remain outside the implemented
candidate and require owner decisions before any additional calculation work.

The durable artifact contract is in [`output_inventory.md`](output_inventory.md), existing
report calculations and plot meanings are in
[`corrected_cu_report_package.md`](corrected_cu_report_package.md), and accepted
identity, binning, CU, and station-count semantics are in the ADRs. These define
the implemented candidate; additional or deferred report purposes still need
explicit definitions and owner review before implementation.
The CU radio/bin source and the ADR-0012 report families are implemented.
Admission-capacity rules are accepted and implemented under
[`ADR-0013`](decisions/0013-radio-admission-capacity-reporting.md).
Exploratory beacon-quality rules are accepted and implemented under
[`ADR-0014`](decisions/0014-exploratory-beacon-cadence-and-capture-quality.md),
with exposure-qualified loss/threshold inference and model correction deferred.
Potentially affected association reporting is accepted under
[`ADR-0016`](decisions/0016-potentially-affected-station-reporting.md), with
station-weighted exposure and complete-bin concurrent peaks rather than
unique-client or proven-impairment claims.
Detailed overall and 5/6 GHz comparisons are accepted and implemented under
[`ADR-0015`](decisions/0015-band-comparison-and-longitudinal-reporting.md).
They are central to the project purpose, with manual year-over-year exports.
The scope of
the one-time informational comparison was accepted on 2026-10-08; see
[`original_to_corrected_migration.md`](original_to_corrected_migration.md).

The accepted report families and offline overview are implemented and validated
as a release candidate. The candidate does not claim unimplemented legacy
statistics or unaccepted metrics. `defer` still requires a decision before
implementation; final owner scope/output acceptance remains a publication gate.
Useful purposes are served by corrected reports, not mandatory graph-for-graph
legacy reproduction. Redundant or misleading presentations are not restored.

## Station-count, radio/bin, and initial report families implemented

[`ADR-0010`](decisions/0010-station-count-rollup-and-staleness-semantics.md)
accepts:

- session-active membership limited to BSSIDs represented in the class data;
- previous-value extension for at most 30 seconds and leading-edge backward
  extension for the same limit;
- complete-only radio/bin, SSID/classroom/bin, band/classroom/bin, and
  all-band/all-SSID classroom/bin totals, with null unavailable totals rather
  than partial numeric sums;
- a bounded, per-session derived view before considering another durable
  artifact; and
- maximum classroom/bin client count, including stale-assisted bins, divided
  by combined simultaneous-section enrollment when enrollment is positive.

The bounded implementation now writes radio, SSID, band, and classroom/bin
rollups, a compact class-session peak/enrollment table, and an approved
enrollment-weighted overall/SSID/band summary under `reports/station_count/`.
The aggregate is the ratio of eligible peak sums to corresponding enrollment
sums and is also shown concisely in the run-wide summary. It is
manifest-registered, processes one physical session at a time, records bounded
row counts, and has focused semantic and capture-run integration tests.

Representative `2025-full` output established complete-run elapsed time and
station-package storage. ADR-0011 implements the reporting source as a bounded
combined CU/station radio/bin Parquet view. Detailed radio/class plots remain
off by default and are enabled only by a flag or environment setting. ADR-0012
adds complete classroom/bin and per-band station
distributions, same-radio/same-bin CU/station density plots with descriptive
Pearson and Spearman correlations, and a physical class-session review
package. The class-session table preserves combined course/section context and
enrollment; room capacity is context only, with missing/conflicting values
left null and no capacity ratio. Classroom-level CU aggregation,
inferential regression, and other
unreviewed cross-metric statistics remain deferred. No historical calculation
pipeline should be restored.

### Radio admission capacity implemented (2026-10-08)

ADR-0013 extends the bounded radio/bin view with one independently available
admission-capacity reading, separate source lineage, and disagreement/invalid
candidate diagnostics. Valid capacity from the unchanged CU source is preferred;
otherwise stable preferred-SSID/member fallback is used. Zero, missing, invalid,
and no-beacon are distinct, and no capacity is stale-filled or summed across
radios. CU describes channel conditions; admission capacity describes the radio's
advertised QoS medium-admission allowance.

`reports/admission_capacity/` contains overall/per-band histograms and exact
CDFs, capacity/CU and capacity/station hexbins, coverage/quality tables, and
descriptive Pearson/Spearman coefficients. Permitted stale-assisted station
totals participate. Fixed display grids and marginal frequencies, followed by
one narrow derived-Parquet read, avoid project-wide observation/joint-pair
retention. The five durable artifacts and existing CU/station semantics remain
unchanged. Representative cached reporting evidence is recorded separately
from complete capture-run timing.
See the [admission reporting review](benchmarks/alpha_0_2_admission_reporting_review_2026-10-09.md):
60 affected tests pass, existing CU/station values are preserved across all
373 sessions, and final cached reporting takes 27.44 seconds. The view adds
20.52 MiB and the admission package adds 1.914 MiB. This is not a measured
incremental full-capture runtime. The later complete run and audit are recorded
in the [release-readiness record](benchmarks/alpha_0_2_release_readiness_2026-10-29.md);
final owner output sign-off remains separate.

### Exploratory beacon quality implemented (2026-10-12)

ADR-0014 adds a compact pre-selection package under `reports/beacon_quality/`.
Native TSF diagnostics remain separate from ERM observation-time diagnostics.
The transmitted expectation defaults to 100 TU, with an explicit diagnostic
override; ERM uses at most one effective BSSID observation per ten intervals.
Effective extras do not delete canonical observations. Session-active membership,
class-window denominators, edge/interior empty slots, and unknown listening
exposure are explicit: this reports capture availability, not proven RF loss.

Bounded 100-interval windows retain member/any-beacon availability and existing
radio CU/station context. Descriptive previous-window associations include
available stale-assisted station counts, pair coverage, and family/model/band/
sampled-radio-topology cohorts. CU is never filled. Known exposure, own/neighbour
classification, simultaneous ERM/native validation, reliability thresholds,
and AP-555/AP-635 periodic-loss corrections are deferred pending evidence.
The package adds no decode pass or new durable artifact; existing published
runs are unchanged. Full-run timing/storage and link checks are now recorded
in the release-readiness record; final owner output sign-off remains separate.
Small-capture and synthetic reporting evidence is in the
[beacon-quality review](benchmarks/alpha_0_2_beacon_quality_review_2026-10-12.md).

### Validation checkpoint (2026-10-08)

Both complete `2025-full` runs on 2026-10-04 contain all four report packages
and 691 optional radio/class plots. The later run already used doubled figure
dimensions. It used eight extraction workers, while the earlier run used four;
their total elapsed times cannot isolate the cost of the figure-size change.
The consolidated record is in
[`benchmarks/alpha_0_2_reporting_review_2026-10-08.md`](benchmarks/alpha_0_2_reporting_review_2026-10-08.md).

Current report regeneration from the published artifacts provides a separate
measurement of default reporting and the optional plot collection, and checks
that presentation refinements preserve the published report values. This is
cached reporting evidence, not another end-to-end capture run. Owner review
of representative current output and Alpha 0.2 publication remain separate
checkpoints; Alpha 0.2 is not yet published.

## Detailed band comparison implemented (2026-10-12)

ADR-0015 establishes this as a central reporting family, not an optional legacy
parity graph. `reports/band_comparison/` adds overall/band/AP/classroom cohorts,
model and sampled-radio-topology summaries, same-AP/session common-bin CU
comparisons, and actual 6 GHz station-use shares. AP and classroom classification
remain separate and mixed rooms are flagged. 2.4 GHz participates when sampled.

Optional `6GHz_Enabled` accepts an explicit AP declaration; blank/auto uses
project-wide observed 6 GHz. Contradictory false declarations are rejected and
enabled-but-unobserved APs remain visible. Primary CU summaries use valid radio
duration, with equal entity/session views alongside them. CU relative to
associated station count and station-use shares use matching observations and
explicit component sums, never independent peak ratios or partial totals.

Infrastructure and stable detailed/aggregate exports support manual comparison
of projects/years. Capture scope, coverage, configurations and project-scoped
identities remain explicit; unique devices, capability, throughput and causal
effects are not claimed. Comparison accumulation shares existing session streams
and adds no decode pass. Complete-run timing/storage is recorded in the
release-readiness record; final owner approval remains a publication gate.
Automated cross-year joining, calibration and causal
models are deferred.
Cached full-project validation and storage are recorded in the
[band-comparison review](benchmarks/alpha_0_2_band_comparison_review_2026-10-13.md):
373 sessions and 2.85 million radio/bin rows, unchanged CU/streak values across
691 radio/session rows, 24.51-second final cached reporting and a 2.22-MiB package.
This is not incremental full-capture timing or owner approval of the new charts.

Band plus channel identifies radios only within an AP. Different APs on the
same band/channel remain separate, and channel number is never interpreted
without band.

## Potentially affected station reporting (2026-10-16)

ADR-0016 adds `reports/affected_stations/` from the existing per-session
radio/bin stream. Radio, AP and classroom/session rows retain all-band and
sampled-band views, station-time components, paired-data coverage, stale
contributions and peak bin provenance. Project/band/infrastructure-cohort
exposure is a ratio of pooled station-time sums, never an average of ratios
or a sum of overlapping radio/AP/classroom views.

Session peaks sum qualifying stations in the same bin before taking a maximum.
Only complete CU/station pairs across every session-active radio qualify for
an entity peak; complete-bin coverage is separate from pooled pair coverage.
Classroom/session peak distributions in the saved package do not add independent
peaks. The web report omits their table/CDF and presents station-time exposure
under CU instead. Existing
high/very-high thresholds and stale-station assistance apply; CU is never filled.
Complete-run runtime/storage and representative checks are recorded in the
release-readiness record; final owner output acceptance remains a checkpoint.
The [affected-station review](benchmarks/alpha_0_2_affected_station_review_2026-10-16.md)
records full-project cached checks of 691 radio/session rows, about 22 seconds
of accumulation/finalization, and a 2.09-MiB derived package. This is not an
isolated incremental full-capture timing measurement.

## Reporting organization and user navigation

Owner-requested follow-up, 2026-10-16: the growing output collection needs a
human-oriented review path, consistent formatting and a clearer entry point.
This is reporting work, distinct from new metric semantics or a GUI framework.

The first navigation checkpoint is implemented under
[`ADR-0017`](decisions/0017-offline-run-report-overview.md): a self-contained
`reports/index.html` with headline values/coverage, curated figures, local-time
classroom/session disclosures, optional radio-plot links and per-family file
inventories. The CLI prints its path. Existing files and metrics are unchanged;
the overview reads compact reports only and validates local links before
publication. Owner layout/navigation review remains before broader reorganization.
The [offline overview review](benchmarks/alpha_0_2_run_overview_review_2026-10-16.md)
records cached generation/link validation at 0.122 seconds for an approximately
80-KiB page, with desktop/narrow layout checks. No full capture rerun or metric
change was needed.
Owner-reviewed hierarchy refinements are implemented under
[`ADR-0018`](decisions/0018-web-report-context-and-explanations.md): project
observation dates/scope, class-hours and selected-row context; existing peak
client/enrollment and radio station metrics; full-width explained figures;
Pearson/Spearman tables; classroom heading AP/radio/seat context; and per-session
pooled mean radio CU. The current order is Overview, CU (including station-time
exposure), Stations & enrollment, CU/station, Admission capacity, Band comparison,
Capture quality, Class sessions and Evidence. Cohort sample sizes include
classrooms; an expandable model/configuration table and classroom-by-band
tables expose existing compact results. Native links are enhanced by a small
inline current-section script; desktop navigation fits its content and narrow
navigation wraps. Raw file capture bounds are not inferred from selected timestamps,
and no regression/R-squared or merged classroom-channel CU is introduced.

Remaining closeout checkpoints before Alpha 0.2 publication:

- Finish the owner's report review and any final edits. Detailed radio plots
  remain opt-in; their links are listed directly inside classroom disclosures.
- Review the owner's new full runs of previous datasets and record their actual
  plot counts/storage. These supplement the recorded 2025 benchmark; they do
  not change the accepted analysis definitions.
- Refresh the regular regression suite, local documentation/report links and
  representative desktop/narrow checks on the final candidate. The latest
  focused report/refresh/CLI suite passed 58 tests; it is not a full-suite result.
- Commit the reviewed changes, complete scope/output sign-off, and obtain
  explicit tag/release authorization. No publication has been performed.

Selected public reports on GitHub Pages are planned separately. A curated
public export and deployment workflow are not implemented. Full-resolution
plots for three week-long runs are likely to exceed one Pages site's size
limit; confirm storage after the new runs. Public-site preparation must not
copy arbitrary local bundles or change their immutable source runs.

A richer interactive interface, filters and automated cross-project browsing
remain a separate next-phase design proposal. ADR-0007's interface technology
choice is still unresolved; this checkpoint does not select a framework or
authorize a hosted service. Basic report navigation should not depend on it.

## Performance and scalability

The 80-capture, 49.37-GiB project has completed successfully with bounded
memory, but observed wall time varies substantially between otherwise
identical four-worker runs. Future performance work should first collect:

- CPU, disk throughput/latency, and memory over complete runs;
- per-capture extraction/normalization/staging timings;
- session-selection and selected-Parquet write throughput; and
- repeat runs under controlled system load.

Extraction is currently the dominant stage, followed by selection and selected
observation writing. Optimize only after the variability is characterized, and
require the corrected semantic golden to remain unchanged.

## Additional vendor support

The adapter/field planner is modular, but a vendor is supported only after a
representative capture proves its AP-name field, timing, QBSS, BSSID, and
band/channel behavior. Candidate future adapters include Arista, Mist, and
Cisco. MAC-pattern AP inference without a hostname remains separate future
work and is not required by the current workflow.

## Optional product work

The following are not required for the current technical-user release:

- installable wheel or packaged console command;
- graphical interface;
- resume/retry UI;
- hosted deployment; and
- consumer-oriented setup automation.

They may be reconsidered if the user base or deployment model expands.

## Known limitations

- Station-count rollups, radio/bin detail, and the class-session package are
  derived reports, not authoritative summary artifacts. Full-run stage/storage
  evidence and current cached report regeneration are recorded separately.
  Owner output review remains required before publication; the large per-radio
  plot collection remains disabled by default.
- APs without a supported hostname IE require a future explicit BSSID mapping.
- Aruba ERM can show periodic controller/capture-path delivery pauses even
  while the active per-BSSID cadence remains about 1.024 seconds.
- No runtime SLA is claimed.
- Support is evidence-based and currently narrower than every theoretically
  decodable capture/vendor combination.

## Historical reproduction

The former multi-command workflow and original golden fingerprint are not
maintained in this repository. The public
[Original release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original)
is the recovery point for that version.
`tests/golden/legacy_corrected_comparison_v1.json` retains one small same-input
semantic comparison without executing historical code.
The owner-approved comparison is a one-time informational migration record.
Future work maintains corrected semantics and their tests; it does not maintain
legacy functions, reproduce old bugs, or rerun Original as a release oracle.

## Next owner checkpoint

Owner release-scope confirmation, 2026-10-29:

- Existing Pearson r and Spearman rho satisfy relationship reporting; regression
  R-squared is not an Alpha 0.2 requirement.
- Beacon-loss calibration remains future work, not a publication prerequisite.
- Cross-project/year comparisons remain manual; automated comparison is not
  required for Alpha 0.2.

A fresh `2025-full` run with radio/class-session plots enabled completed
successfully: 5h 02m 26s CLI wall time, 477.04 MiB total output, 373 class
sessions, 6,511,028 selected observations and all 691 optional plots. Input hashes
and corrected identity/radio/classroom summary fingerprints match the previous
same-input full run. Overview, manifest and plot-index checks pass. The one
logged pandas FutureWarning was fixed afterward; cached report regeneration
preserves all 2,689 class rows and eight aggregate rows exactly, including nulls.
No second full run is required for that typed-report compatibility fix.
The [2026-10-29 full-run checkpoint](benchmarks/alpha_0_2_release_readiness_2026-10-29.md)
records the final timing/storage, audit and test evidence. The plot default is
restored to false for the candidate; explicit opt-in remains available.

## Candidate closeout checklist

- [x] Accepted reporting families and offline navigation implemented.
- [x] Complete `2025-full` run, timing/storage and corrected-summary audit.
- [x] Pandas warning fixed; cached logical values and nulls unchanged.
- [x] The 2026-10-31 checkpoint passed 392 non-external tests, including the historical SSID reference adapter.
- [x] That checkpoint passed all 16 real-capture/TShark tests using the owner's retained
  known-good references; results are recorded in the readiness record.
- [x] Detailed radio plots restored to opt-in in the active configuration.
- [x] Candidate release notes and current-status documentation prepared.
- [x] Current report wording, navigation, summary displays and saved-run refresh
  documented through 2026-11-13; earlier dated evidence is retained.
- [x] Implement the 2026-11-16 CU mean/CDF bounds and availability-conditioned
  station-time bounds using the staged
  [implementation inventory](cu_missing_bin_bounds_implementation_plan.md).
- [x] Audit the owner's four fresh v2 publication runs, timing/storage, source
  links and registered artifacts (2026-11-18).
- [x] Prepare a validated WebP publication package, band-specific watermarked
  examples, verified shared HBLL plots, local `example-reports` branch and
  reviewable main README link; no push or deployment yet.
- [x] Final local regression after the last edits: 638 regular plus 16
  real-capture tests passed without skips (2026-11-18); source and curated-site
  links, case, CSV preservation and all public Git-blob bytes verified.
- [ ] Final owner browser review and deployed-site smoke checks; push the
  example branch and README link, promote the reviewed application source to
  public main, and configure Pages after owner review.
- [ ] Final owner scope/output approval, including the inventory's listed
  non-claims and remaining deferred purposes.
- [ ] Explicitly authorized release tag/publication.

No automated cross-project comparison, R-squared, beacon-loss calibration,
GUI framework or additional unreviewed statistic is required by the accepted
candidate. The informational Original comparison is complete and not rerun.
These closeout updates do not authorize publication or imply final owner sign-off.
