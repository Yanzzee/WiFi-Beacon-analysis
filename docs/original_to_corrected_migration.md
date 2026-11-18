# Original to corrected: one-time informational comparison

- Comparison scope accepted by the project owner: 2026-10-08.
- Purpose: help readers interpret archived projects and understand results when
  rerunning their captures with current versions.
- Historical recovery point: the public `Original` tag, commit
  `4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb`.
- Numerical example: the retained one-time execution in
  [`legacy_corrected_comparison_v1.json`](../tests/golden/legacy_corrected_comparison_v1.json).

This is an informational migration record, not a numerical parity contract.
The former calculation was executed once before legacy cleanup, and its
results were frozen. No legacy functions or compatibility mode need to be
maintained, and future releases do not need to rerun Original. Ongoing tests
validate corrected semantics; the existing frozen-comparison check executes
only the corrected calculation and verifies the retained input hash.

## Measured common-input example

The frozen comparison uses the same 15 selected observations for classroom
`ROOM1`, session `C1`, from `2026-01-15T10:00:00Z` to
`2026-01-15T10:00:08.500Z`. The nominal analysis interval is 1.024 seconds.
Input bytes are identified by SHA-256
`139d1ba8341487377d748672e354c7046e089ddcaf8b88dc9b8f80e513b201cb`.
The corrected snapshot uses `beacon.metric/corrected-cu/v1` and
`beacon.schema/analysis-results/v1`.

The historical calculation treated these selected observations as a single
location-wide stream. Its `room_scount` input was supplied by summing fixture
station values within each bin. This example isolates calculation differences;
it is not an end-to-end comparison of capture decoding, historical sampling,
or the current 30-second station supplementation algorithm.

| Result | Frozen former calculation | Corrected result | Interpretation |
|---|---|---|---|
| Radio count in the fixture | 3 | 3 | Counts agree for this controlled example; identity rules can still differ on real captures. |
| Longest high-CU streak | 2.048 s, location-wide | 2.356 s actual observed; 3.072 s possible connected | Corrected streaks are calculated per radio and then reduced to a classroom maximum. Actual and possible streaks are separate results. |
| Longest very-high-CU streak | 2.048 s, location-wide | 2.048 s actual observed; 2.048 s possible connected | Matching numbers do not make the underlying definitions interchangeable. |
| Terminal partial interval | Omitted | Included: 0.308 s | Exact session duration is retained instead of rounding the session down to whole bins. |
| Classroom CU average | 140.25 raw units across beacon rows | No classroom CU average | Corrected duration-weighted averages belong to individual radios with valid CU. A classroom CU aggregate has not been accepted. |

The former calculation also reported 5.120 s of location-wide high-CU time and
4.096 s of very-high-CU time. These are not compared with a sum or maximum of
corrected radio durations: different radios may be high at different or
overlapping times. A classroom union-duration statistic would require its own
definition.

## Definition changes relevant to old projects

The following mapping explains source-reviewed historical behavior and the
accepted corrected decisions. It does not imply that every change was measured
in the small numerical example above.

| Topic | Original interpretation | Corrected interpretation and effect |
|---|---|---|
| Radio identity | Historical Aruba transmitter/MAC-pattern inference and radio numbering | Configured AP hostname plus band/channel evidence, with BSSID/radio/AP/classroom identity recorded explicitly. Radio numbers are local identifiers, not a cross-version join key. |
| Sampling and duplicates | Historical retained beacon rows and near-duplicate filtering | At most one selected observation per BSSID/session/bin after source precedence and accepted timing/duplicate handling. Row totals and row-weighted statistics can change. |
| CU histogram/CDF | Retained beacon-row population | One valid CU value per physical radio/bin; primary SSID with accepted fallback. Multiple SSIDs do not duplicate the radio reading. Missing time is excluded and coverage is reported. |
| CU average | Beacon-row averages and some mixed-radio class summaries | Duration-weighted valid-CU average per radio/session. No accepted combined classroom CU average. |
| Missing CU and missing beacon | Historical row-oriented behavior and location-wide bin tests | Separate valid, missing-CU, and no-beacon states; unknown is distinct from measured zero. Actual streaks stop at unknown time; possible-connected streaks use their separately documented rules. |
| Class boundaries and local time | Historical schedule expansion and interval loops | Half-open physical sessions with exact terminal partial bins. Named IANA timezone applies the correct offset for the event date. Display is local; canonical timestamps are UTC. |
| Radio station total | Nearby-transmitter row lookup, with the radio total repeated on beacon rows | Sum session-active BSSID counts per radio/bin. Same-BSSID prior values may extend for 30 s, and leading gaps may use a later value for 30 s. Totals are null when any required member remains unavailable. |
| Classroom station total | Approximate room totals from nearby observations, including a +/-5-second lookup | Complete classroom/bin totals across represented APs, radios, bands, and SSIDs; separate SSID and band totals. Counts still describe the sampled scope, not an inventory of every device in the building. |
| Enrollment comparison | Maximum approximate room count divided by enrollment; project aggregate used a ratio of sums | Maximum available classroom/bin total, including stale-assisted bins, divided by combined simultaneous-section enrollment. Project/SSID/band means retain enrollment weighting. Missing or zero enrollment is excluded. Current web label: Peak stations per enrolled; stored names remain unchanged. |
| Room seats | Historical schedule context and some inherited reporting fields | Descriptive capacity only. Missing/conflicting values are unknown; no seat-utilization or admission-capacity ratio is implied. |
| CU/station relationship | Mixed summary statistics or duplicated beacon-row populations | Same-radio/same-bin valid pairs, descriptive Pearson/Spearman and density plots, with pair coverage and stale-assisted participation reported. No inferential regression is included. |
| Time-series gaps | Historical plot appearance | Dashed blue/orange bridges are visual guides for unknown CU/station intervals. They do not supply metric values. Stale-assisted station values are actual supplemented report inputs and are flagged separately. |
| Files and provenance | Mutable intermediate files and timestamped logs | Immutable run directories, versioned schema/metric identifiers, input hashes, manifest-linked artifacts, and derived review reports. |

Accepted definitions are maintained in ADR-0003/0004/0005 and
[ADR-0010](decisions/0010-station-count-rollup-and-staleness-semantics.md),
[ADR-0011](decisions/0011-bounded-radio-bin-reporting-source.md), and
[ADR-0012](decisions/0012-class-session-and-cu-station-reporting.md).
The complete keep/replace/drop/defer mapping is in
[`alpha_0_2_original_output_inventory.md`](alpha_0_2_original_output_inventory.md).

## Finding the current equivalent of an old output

| Archived output or question | Current place to look |
|---|---|
| Radio summary CSV and CU/streak facts | `reports/cu_coverage_streak/radio_class_report.csv`, classroom report, and authoritative radio/class summaries |
| Per-radio/session CU and station plot | Optional `reports/radio_class_detail/plots/` and `radio_class_plot_index.csv` |
| CU histogram/CDF | `reports/cu_coverage_streak/plots/radio_bin_cu_*`, including band panels |
| Station distribution and per-radio/SSID/band peaks | `reports/station_count/` |
| Devices per enrolled and class schedule context | `reports/class_sessions/` plus the enrollment-weighted station aggregate and `reports/run_summary.txt` |
| Station versus CU | `reports/radio_class_detail/cu_station_correlation.csv` and `cu_station_hexbin*.png` |
| Admission-capacity diagnostics | ADR-0013 now supplies one advertised radio/bin value, 0..31250 validity and percent conversion, independent source/missingness, and descriptive CU/station relationships; not legacy beacon-row weighting or cross-radio capacity sums |
| Beacon-rate diagnostics | `reports/beacon_quality/` contains accepted native/ERM capture-slot cadence, gap and availability relationships under ADR-0014/0019; unknown listening exposure prevents treating empty slots as proven RF loss. Baseline correction and inferred CU remain future work. |

## Reproducing and rerunning an archived project

For historical numerical reproduction, retain the Original checkout, its
environment and configuration, original input CSVs, capture files, and old
outputs together. The Original release is the recovery point for that workflow;
the current project does not maintain its functions.

To rerun an old project with corrected definitions, use the current command
with the archived capture scope, AP/classroom mapping, schedule dates, and
named schedule timezone. Preserve both versions' outputs and note the current
manifest's input hashes, code revision, schema ID, metric ID, and configuration.
Optional radio/class plots can be enabled explicitly for visual review.

Compare physical classroom/time intervals and resolved AP/band/channel
evidence before comparing values. Confirm enrollment, simultaneous-section
merging, capture coverage, and data eligibility. Do not equate rows by radio
number alone or substitute a classroom streak maximum for an old location-wide
duration. Current-version values should be described with their current metric
definitions rather than relabeled as reproduced Original results.

## Evidence boundary

The numerical comparison is the frozen shared-input example above. Historical
source review supplies the broader definition mapping. Surviving Original
project outputs lack a complete recorded input/environment identity suitable
for claiming an exact common-capture benchmark, so their project-wide numbers
are not presented as directly comparable with `2025-full`.

This document completes the informational comparison record at that scope.
There is no promise of numerical compatibility and no recurring legacy replay
requirement. Future validation and development use corrected semantics and
their own versioned evidence.
