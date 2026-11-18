# Alpha 0.2 original-output inventory and implementation mapping

- Status: Implemented-purpose mapping; final owner release-scope/output review pending
- Date: 2026-10-03
- Reconciled: 2026-11-13
- Historical source: public `Original` tag, commit
  `4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb`
- Scope: generated analysis intermediates, tables, logs, and plots from the
  Original workflow

This began as the planning inventory for the Corrected Reporting Expansion and
now maps implemented purposes to their accepted corrected replacements. It
does not itself approve an unaccepted metric, change the corrected artifact
contract, or authorize additional report implementation. Station-count semantics were subsequently
accepted in
[`ADR-0010`](decisions/0010-station-count-rollup-and-staleness-semantics.md).
CU radio/bin time-series and the initial class-session/relationship reports
are implemented under ADR-0011/0012. Admission-capacity reporting is accepted
and implemented under ADR-0013. Minimal exploratory beacon-quality reporting
is accepted and implemented under ADR-0014; exposure-qualified inference and
calibration remain deferred. The accepted families are implemented and the
2026-10-29 complete-run evidence is recorded in the
[release-readiness record](benchmarks/alpha_0_2_release_readiness_2026-10-29.md).
Unaccepted/deferred purposes remain non-claims of the candidate, not newly
approved calculations. Final owner scope/output acceptance remains separate.
ADR-0015 additionally implements detailed overall and 5/6 GHz comparisons as a
central project purpose, beyond one-for-one Original graph replacement.
Its `reports/band_comparison/population_summary.csv` provides the central
overall/band/cohort numeric lookup, with detailed table definitions in that
package's `methodology.txt`. Following the 2026-10-15 presentation review,
cohort charts show one shared view when no classrooms mix AP enablement, and
both views otherwise; both CSV definitions remain available. AP/session and
classroom/session adoption histograms retain their distinct counting units
and show use only where 6 GHz is enabled, retaining genuine zeros and annotating
unavailable/excluded sessions and near-zero shares. Detailed tables and overall
project adoption retain all sampled infrastructure. No metric formulas change.

## Disposition vocabulary

The disposition applies to the analytical purpose, not to preservation of the
legacy bytes, filenames, or calculations. Corrected artifacts and semantics
remain authoritative.

| Disposition | Meaning for Alpha 0.2 |
|---|---|
| `keep` | An accepted corrected output already serves the useful purpose. No new legacy-style report is needed. |
| `replace` | Serve the useful purpose with a corrected definition/presentation, not legacy calculations. The row states whether its gate is open or its replacement is implemented. |
| `drop` | Do not reproduce the artifact or its purpose as a published corrected output. |
| `defer` | The purpose may be useful, but a named semantic or source decision must be accepted before implementation. |

## Evidence and inventory boundary

The inventory was built from the `Original` tag's pipeline documentation,
runtime modules, output contract in `tests/pipeline_harness.py`, and the
surviving Original-run files under `output/`. Root folder spellings in this
inventory follow the current lowercase layout; the public Original release
used capitalized storage roots. The surviving run contains:

- 228 radio/class CSV rows and 228 matching per-radio/session plots;
- 7 all-class-observation plots;
- 7 radio-summary/beacon-rate plots;
- 4 class-session plots;
- 2 CSV tables; and
- 4 timestamped text logs.

This is 252 surviving legacy files, excluding corrected run directories under
`output/runs/`. The dynamic families below cover every generated file named by
the public Original pipeline even when the corresponding `data/` or `sample/`
file is no longer present locally. Optional capture-script `.pcapng` files are
source evidence for analysis, not reporting outputs, and are outside this
matrix. `group_data_cleanup.py` mutated grouped Parquet files in place and did
not create another logical output.

## Corrected source grains

| Corrected source | Grain available for a retained purpose |
|---|---|
| `project_identity_map.parquet` | One row per in-scope BSSID, with BSSID-to-radio-to-AP-to-classroom identity and band/channel evidence. |
| `selected_observations/` | At most one selected observation per BSSID/class-session/bin, including source lineage, CU, station count, and admission capacity. |
| `radio_class_summary.parquet` | One row per stable radio/class session with accepted CU coverage, missingness, average, durations, and streaks. |
| `classroom_class_summary.parquet` | One row per physical classroom session with radio counts and accepted classroom maximum-streak results. |
| Manifest and current derived report package | Run/input/configuration provenance plus bounded human-readable CU, coverage, streak, and band-population reports. |
| Bounded radio/bin detail view | Implemented at `reports/radio_class_detail/radio_bin_detail.parquet` with one selected CU state/value and the accepted radio station total per physical radio/bin. |
| Implemented bounded station view | `reports/station_count/station_bin_rollups.parquet` implements ADR-0010 session-active membership, 30-second stale assistance, and complete-only radio/SSID/band/classroom totals from selected observations. |
| Exploratory pre-selection cadence view | `reports/beacon_quality/beacon_quality_windows.parquet` implements ADR-0014 radio/family/session windows from raw admitted observations, with unknown exposure and existing radio/bin CU/station context. |

## Generated intermediates and durable tables

| Original output | Original purpose or grain | Draft disposition | Corrected source/destination and reason |
|---|---|---|---|
| `data/processed_data/<capture>.pcapng.parquet` | One extracted table per capture | `drop` | Extraction is an internal bounded stage. Publish lineage in `selected_observations/` and the manifest, not a duplicate decoded-capture product. |
| `data/grouped_data/<AP_name-or-BSSID>.parquet` | Cross-capture rows grouped by AP name or BSSID | `drop` | The corrected pipeline resolves identity explicitly and stages data internally. Publishing hostname-named working sets would recreate the legacy pipeline. |
| In-place cleaned `data/grouped_data/*.parquet` | Timestamp/index cleanup | `drop` | This was a mutation of a working artifact, not a report. Corrected normalization and its provenance belong in selected rows and the manifest. |
| `sample/sample_aps.parquet` | Converted AP configuration | `drop` | Read the configured CSV directly; preserve effective input hashes/configuration in the manifest and resolved identity in `project_identity_map.parquet`. |
| `sample/sample_classrooms.parquet` | Converted classroom configuration | `drop` | Read the configured CSV directly; retain resolved classroom identity in corrected artifacts. |
| `sample/sample_classes.parquet` | Expanded class-session schedule | `drop` as a public artifact | Physical sessions expand in memory. ADR-0012 places course, enrollment, and descriptive capacity context in the bounded class-session report; no legacy schedule cache is restored. |
| `data/sampled/<AP_name>_class_beacons.parquet` | Per-AP, in-class, near-deduplicated beacon rows | `drop` | `selected_observations/` is the accepted traceable observation artifact at BSSID/class-session/bin grain. The per-AP append layout is not needed. |
| `data/all_class_beacons.parquet` | Combined in-class beacon rows | `drop` | `selected_observations/` replaces the useful evidence role without legacy duplicate-row weighting. |
| `data/radio_summary.parquet` | One inferred radio/AP/class-session row | `keep` | Its accepted identity and CU purposes are already served by `radio_class_summary.parquet`, `classroom_class_summary.parquet`, and the identity map. Legacy peak/median station and consecutive-row streak fields are not retained. |
| `data/all_class_beacons_processed.parquet` observation/location fields | Combined observations enriched with classroom | `drop` | The identity map and selected observations already carry resolved classroom identity. A second large observation table would be redundant. |
| `data/all_class_beacons_processed.parquet` `room_scount` field | Approximate room station total using a +/-5-second lookup | `replace` | ADR-0010 replaces it with a complete-only classroom/bin total using session-active BSSIDs and explicit 30-second stale assistance. Source: `selected_observations/` through the bounded derived station view. |

## Tables and text logs

Some legacy files mixed accepted CU purposes with unresolved station or rate
purposes. Those files are split by purpose below so an accepted subset cannot
silently authorize the unresolved subset.

| Original output or portion | Original purpose | Draft disposition | Corrected source/destination and reason |
|---|---|---|---|
| `output/radio_summary.csv`: identity, class bounds, radio count, CU, and very-high-CU fields | Review table at inferred radio/AP/class-session grain | `keep` | Use the existing `radio_class_report.csv` and `classroom_class_report.csv`, backed by corrected summaries and identity. Coverage and missingness must accompany CU. |
| `output/radio_summary.csv`: station-count fields | Per-radio/session high, median, and low station counts | `replace` | The station class report now supplies per-radio/session peaks with availability and stale-assistance counts. Median/low statistics were not restored and require review if still useful. |
| `output/radio_summary.csv`: beacon-count and graph-reference fields | Observation quantity and link to an unbounded plot | `replace` beacon diagnostics; `drop` graph path | ADR-0014 uses pre-selection family/BSSID/session evidence with raw/effective views, explicit class-window expected slots and unknown exposure. Legacy plot paths are not data. |
| `output/filtered_radio_summary.csv` | Duplicate radio table filtered to at least 300 beacons, positive duration/SSID count, with class duration and beacon rate | `drop` as a separate table | Do not preserve an arbitrary 300-beacon filtered copy. If rate diagnostics are accepted, publish a purpose-built table with explicit population, active-cadence, pause, and coverage fields. |
| `output/analyze_data_log_<timestamp>.txt`: progress and repeated per-radio values | Operational trace plus prose copies of radio summaries | `drop` | The lifecycle JSONL log, run summary, manifest, and review CSVs provide structured provenance and results. Do not duplicate every radio/session into a text log. |
| `output/analyze_all_class_beacons_log_<timestamp>.txt`: CU distribution lines | Beacon-row-weighted CU rates, CDF points, mean, and median | `keep` purpose only | Existing corrected radio/bin histogram/CDF and radio/class reports serve the useful distribution purpose with stable radio/time sampling and explicit coverage. |
| Same log: station aggregates and station/CU correlations/regression | Station distributions and relationships | `replace` descriptive subset; defer regression | Classroom/bin and per-band station distributions plus same-radio/same-bin CU/station hexbins and descriptive Pearson/Spearman correlations are implemented. Inferential regression and classroom-CU aggregation remain deferred. |
| Same log: admission-capacity correlations | Admission capacity versus CU/stations | `replace` | ADR-0013 implements descriptive same-radio/bin correlations, coverage and source/disagreement diagnostics. No legacy duplicate-BSSID weighting or inference. |
| `output/analyze_radio_summary_log_<timestamp>.txt`: corrected-CU-reviewable subset | Counts and CU/streak/coverage-adjacent radio summaries | `keep` purpose only | Use corrected review tables and run summary; do not restore prose-only aggregates or the statement that every row is a 5 GHz radio. |
| Same log: station correlation | Peak station count versus median CU | `replace` | The legacy mixed summary is replaced by same-radio/same-bin valid pairs with explicit pair coverage, stale-assisted pair counts, and descriptive Pearson/Spearman coefficients. No p-values or independence claim is made. |
| Same log: beacon-rate OLS summaries | Rate versus highest CU split by AP radio count | `drop` in this form | The legacy rate and highest-CU variables are coverage-sensitive, and the 1-versus-2-radio split is not a sufficient corrected population definition. Future accepted diagnostics need not reproduce OLS. |
| `output/analyze_class_sessions_log_<timestamp>.txt`: CU duration/streak summaries | Project aggregates of classroom high-CU incidence, duration, and longest streak | `replace` | The class-session review table and CU/streak reports provide accepted coverage and per-radio/classroom maximum streak facts. The legacy location-wide duration/incidence aggregate is not reproduced; any additional project aggregate requires its own definition. |
| Same log: peak devices per enrolled | Maximum classroom station count divided by enrollment | `replace` | Implemented as the accepted maximum available classroom/bin count, including stale-assisted bins, divided by combined simultaneous-section enrollment. Overall/SSID/band aggregates use the enrollment-weighted ratio of sums. Zero/missing enrollment is excluded. The web label is `Peak stations per enrolled`; stored field/file names are unchanged. |
| Same log: stations affected by high CU | Project station count associated with high-CU periods | `replace` | The web report presents paired station-time exposure and coverage under CU. ADR-0016 concurrent-peak files remain generated, but the classroom peak table/CDF are deliberately omitted from the page and inventories. Missing CU is not below-threshold. These are associations under observed channel conditions, not unique clients or proven impairment. |

## Plot matrix

| Original plot output | Original purpose | Draft disposition | Corrected source/destination and reason |
|---|---|---|---|
| `output/plots/<AP>_<class-start>_R<radio>.png` (228 surviving) | Unbounded per-radio/session CU and `total_scount` time series | `replace` | Implemented from the bounded radio/bin view as an indexed opt-in collection. Measured CU is solid with fill; unknown CU is blank with a dashed blue visual bridge. Radio station count is orange, with hollow stale markers and a dashed visual bridge across unavailable gaps. A context box carries room, schedule, AP, and band metadata. Default runs create no such plots. |
| `all_class_beacons/Channel_use_histogram.png` | Distribution of CU across retained beacon rows | `keep` purpose only | `radio_bin_cu_histogram.png` and its by-band variant already provide the corrected stable-radio/bin population. |
| `all_class_beacons/Channel_use_cdf.png` | CU CDF and threshold context | `keep` purpose only | `radio_bin_cu_cdf.png` and its by-band variant already provide the corrected population and label 50%/75% shares. |
| `all_class_beacons/Channel_use_pdf.png` | Smoothed/density representation of the same CU histogram | `drop` | Redundant beside the exact discrete histogram and CDF; it adds no distinct review question. |
| `all_class_beacons/Scount_histogram.png` | Distribution of legacy per-radio `total_scount` on beacon rows | `replace` | Implemented with complete all-band classroom/bin totals plus separate band/classroom/bin panels. Unavailable totals are excluded and stale-assisted values are identified. |
| `all_class_beacons/Scount_channel_use_hexbin.png` | Station count versus CU relationship | `replace` | Implemented at same-radio/same-bin grain using valid CU and available station totals, with overall/per-band density, pair coverage, and descriptive correlations. |
| `all_class_beacons/Scount_adc_hexbin.png` | Station count versus admission capacity | `replace` | Implemented under ADR-0013 using available radio station totals and valid advertised radio capacity, including counted stale-assisted station pairs; overall/per-band panels. |
| `all_class_beacons/adc_channel_use_hexbin.png` | Admission capacity versus CU | `replace` | Implemented under ADR-0013 using same-radio/bin values with unchanged CU, independent capacity selection/lineage, valid-pair coverage and source diagnostics. |
| `radio_summary/Beacons_per_second_class_histogram.png` | Distribution of beacons/second/SSID after legacy filtering | `replace` purpose | ADR-0014 supplies pre-selection BSSID/family/session tables and slot-gap diagnostics instead of a coverage-sensitive pooled rate histogram. Listening exposure remains unknown; empty slots are not RF loss. |
| `radio_summary/Beacons_per_second_class_PDF.png` | Density of beacon rate split by AP radio count | `drop` | Redundant density view and an inadequate 1-versus-2-radio cohort definition. |
| `radio_summary/Beacons_per_second_class_CDF.png` | Beacon-rate CDF split by AP radio count | `replace` purpose | ADR-0014 supplies an exact occupied-slot-gap CDF separated by native/ERM family; model/band/sampled-topology distinctions remain in CSV cohorts. No pooled legacy rate CDF. |
| `radio_summary/Beacons_per_second_stacked.png` | Stacked rate histogram for AP radio counts 1 and 2 | `drop` | Stacking obscures distributions and excludes other radio configurations; a future diagnostic should use explicit band/model/profile cohorts. |
| `radio_summary/Beacons_per_second_class_cu.png` | Beacon rate versus highest CU | `replace` exploratory subset | ADR-0014 uses current radio/family/window empty-slot fraction versus previous-window available CU/station context, pair coverage, descriptive Pearson and context-band tables/plots. No highest-CU OLS, inferred safe threshold, or correction. |
| `radio_summary/Beacons_per_second_class_split.png` | Same relationship split by AP radio count 1 or 2 | `drop` in this form | The split is not vendor/band neutral and is visibly sparse for one cohort in the surviving example. A future diagnostic may use accepted cohorts instead. |
| `radio_summary/Beacons_per_second_class_linear_regression.png` | OLS rate-versus-highest-CU trend by AP radio count | `drop` | Do not restore an inferential model until a future proposal states the question, population, independence assumptions, coverage treatment, and acceptance criteria. |
| `class_sessions/High_CU_streaks.png` | Distribution of classroom maximum high/very-high streaks | `keep` purpose only | `classroom_streak_distributions.png` already replaces the legacy cross-radio streak with corrected per-radio maxima and actual-versus-possible meanings. |
| `class_sessions/High_CU_streaks_zoomed.png` | Duplicate streak histogram limited to 60 seconds | `drop` | The corrected bounded plot should use an honest shared scale; a separate fixed zoom risks hiding outliers and is unnecessary. |
| `class_sessions/stations_enrolled_histogram.png` | Distribution of maximum room stations divided by enrollment | `replace` | Implemented from the accepted maximum available classroom/bin count, combined enrollment, and zero/missing-enrollment exclusion; current web label: `Peak stations per enrolled`. Stored client-named files/fields remain unchanged. |
| `class_sessions/stations_enrolled_plot.png` | Enrollment versus maximum room station count | `replace` | Implemented with combined enrollment, accepted peak classroom count, and hollow markers for stale-assisted peaks. |

## Review gates and implementation status

1. **Station-count gate — implemented:** ADR-0010 scope, stale assistance,
   complete-only rollups, bounded placement, and enrollment comparison are in
   the default run. Representative output and runtime/storage were reviewed;
   the enrollment-weighted overall/SSID/band aggregate is implemented.
   Distribution plots are accepted under ADR-0012; additional unreviewed
   statistics remain gated.
2. **CU radio/bin source gate — implemented:** ADR-0011 selects a bounded
   manifest-registered derived Parquet view rather than a sixth durable
   artifact. It combines one CU value and the accepted station total per
   physical radio/bin. Time-series plots are opt-in by flag/environment.
3. **Schedule-context gate — implemented for class-session reporting:** course
   labels and source-section count remain traceable, enrollment is combined,
   and room capacity is descriptive context only. Missing/conflicting capacity
   is null with an explicit status; no capacity ratio is calculated.
4. **Admission-capacity gate — implemented:** ADR-0013 accepts one advertised
   radio/bin value, independent availability, CU-source/preferred-SSID fallback,
   0..31250 validity, no stale filling or cross-radio sums, and descriptive
   CU/station relationships. Capacity describes the radio's QoS admission
   allowance, while CU describes channel conditions. Full-run evidence is
   recorded; final owner output sign-off remains.
5. **Beacon-rate gate — minimal exploratory package implemented:** ADR-0014
   defines pre-selection native/ERM diagnostics, 100-TU expectation, effective
   ERM cap, class-window denominators, unknown exposure/role, bounded context
   windows and descriptive lagged CU/station associations. Exposure-qualified
   inference, thresholds, paired-capture validation and model correction are
   explicitly deferred. Full-run timing/storage and representative checks are
   recorded; final owner output sign-off remains.
6. **One-time informational comparison — scope accepted 2026-10-08:** use the
   frozen same-input execution and a documented definition/output mapping to
   explain old-project differences. No numerical parity tolerance, recurring
   Original replay, or legacy-function maintenance is required. See
   [`original_to_corrected_migration.md`](original_to_corrected_migration.md).

## Recommended next owner decision

Review the [candidate release notes](alpha-0.2-release-notes.md), completed
[full-run/cached validation evidence](benchmarks/alpha_0_2_release_readiness_2026-10-29.md)
and the actual offline report before final owner release-scope/output approval.
The navigation and explanatory presentation under ADR-0017/0018 are implemented,
including classroom heading AP/radio/seat context and per-session radio CU.
The 2026-10-31 record reports 392 non-external and 16 real-capture/TShark passes
using retained references, not regenerated expectations. Later report/refresh
checks passed 58 focused tests on 2026-11-13; these do not replace final-candidate
regression after owner edits. See [current reporting](reporting_and_publication.md).

Per the 2026-10-29 owner direction, existing R values suffice, loss calibration
is future work and cross-project comparison is manual. The candidate does not
claim unimplemented merged classroom/channel statistics or additional legacy
aggregate calculations. Any future additions still need explicit definitions.
The informational Original comparison is complete; no recurring Original
pipeline is restored. Final sign-off and authorized tagging/publication are
separate from preparing this mapping or the candidate notes.
