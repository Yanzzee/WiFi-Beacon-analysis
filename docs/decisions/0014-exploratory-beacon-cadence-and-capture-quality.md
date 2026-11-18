# ADR-0014: Exploratory beacon cadence and capture quality

> Native slot timing and TSF use are revised by
> [ADR-0019](0019-observation-time-and-native-tsf-diagnostics.md). Native slots
> now use capture timestamps; validated TSF is diagnostic/deduplication only.

- Status: Accepted
- Date decided: 2026-10-12
- Owners/reviewers: Project owner
- Roadmap items: Alpha 0.2 beacon-cadence and capture-quality gate
- Related decisions: [ADR-0008](0008-aruba-capture-timestamp-authority-and-fallback.md),
  [ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md),
  [ADR-0010](0010-station-count-rollup-and-staleness-semantics.md),
  [ADR-0011](0011-bounded-radio-bin-reporting-source.md)

## Decision

Add a minimal exploratory package from session-admitted observations before
representative BSSID/bin selection. Keep native and Aruba ERM capture families
separate. Pool files within each family for this first version and retain their
source references; do not claim that these pooled streams represent independent
receivers. Do not change the five durable artifacts or existing CU, station,
admission-capacity, and selection calculations.

The nominal transmitted interval defaults to 100 TU, or 102400 microseconds.
An explicit `BEACON_INTERVAL_TU` or `--beacon-interval-tu` override changes only
the diagnostic expectation. Advertised intervals remain evidence, with missing
and mismatch counts; they do not silently replace the default.

For ERM, use preferred observation time and class-start-anchored slots of ten
nominal intervals: 1.024 seconds at the default. Count at most one effective
observation per BSSID/slot. Additional views are effective extras for this
metric, not proven packet duplicates, and remain in canonical processing.
Do not use TSF to diagnose ERM cadence. This baseline is an accepted project
assumption, not a claim about all Aruba firmware or capture modes.

For native capture, use the accepted per-BSSID/session TSF reliability,
deduplication, and placement rules. Use observation time when TSF is unusable
and disclose uncertain duplicate identification. Mixed-input sessions assess
native timing independently of ERM. Identical CU or station values never prove
that beacon views are duplicates.

## Denominators and provenance

Only BSSIDs observed in the physical class session and capture family enter
that family's expected population. Expected slots cover the entire class
window, including terminal partial slots. Report raw views, effective occupied
slots, extras, leading/interior/trailing empty slots, and occupied-slot gaps.
Class-window rates are not estimates of active listening or transmission rate.

Listening exposure and ERM own-transmit versus neighbour-receive role remain
explicitly unknown. Empty slots may arise from a missing file, an off-channel
receiver, class edges, RF contention, or capture/forwarding behavior. They must
not be labelled confirmed beacon loss. A radio's any-beacon measure unions
member slots; it does not add SSID beacon rates.

The compact radio/family/session window view uses 100 nominal intervals:
10.24 seconds at the default. CSV comparison cohorts retain model, band, and
the project-observed numbers of 5/6 GHz radio groups on the AP. These counts
describe sampled identity, not a complete AP hardware inventory. Class/radio
keys and source-capture references connect reports to manifest-hashed inputs.

## Exploratory CU and station associations

Compare current-window member-empty-slot fraction with the immediately previous
window's available same-radio CU and station means. Context comes from the
existing pooled radio/bin view and is duration-weighted across exact window
boundaries. It is not a separate CU measurement for each capture path.
CU is never interpolated or extended. Accepted complete radio station totals
retain their existing 30-second forward/leading-backward assistance; paired
stale assistance is counted. Missing previous-window context stays missing.

Report pair coverage and descriptive Pearson correlation, giving each paired
radio/family/window equal weight. Constants or fewer than two pairs yield null.
Descriptive CU bands 0–25, 25–50, 50–75, and 75–100 percent are display groups,
not learned reliability thresholds. Band-level empty-slot fractions are the
ratio of empty to possible member slots. Plots pool comparison cohorts only
for display; CSVs preserve the cohort distinctions.

Missing CU can depend on the CU that was not captured. Lagging the covariate
avoids requiring a contemporaneous reading during the gap but does not remove
that bias or establish causation. No inferred safe threshold at 50/75 percent,
significance test, regression, missing-CU estimate, or model correction is added.

## ERM self versus neighbour evidence

Mapping a known capture/exporting AP to the transmitting BSSID's resolved AP
can establish a useful own-versus-other relationship, subject to the capture
mode. Validated transmit/receive metadata can provide stronger evidence.
The current extraction does not retain a validated exporter/role contract.

The [Wireshark Aruba ERM dissector](https://github.com/wireshark/wireshark/blob/master/epan/dissectors/packet-aruba-erm.c)
describes transmit conventions for types 3 and 6 involving signal/FCS metadata.
They are not a universal role field across type 0, firmware, and capture modes;
a signal-strength value alone is not sufficient proof. Keep role unknown until
a representative capture validates the mapping and metadata together.

## Reporting and validation

`reports/beacon_quality/` contains a stream/session CSV, bounded window Parquet,
cohort associations and covariate-band CSVs, slot-gap counts, methodology, and
three bounded plots. Separate metric/report-schema identifiers and manifest
exports distinguish the exploratory contract. Build and stage one session at
a time, without another decode pass or a project-wide raw observation table.
Existing published runs are not rewritten.

Required tests cover ERM caps without canonical-row deletion, native reliable
and unusable TSF, mixed families, SSID slot unions, interval overrides/mismatches,
exact window boundaries, unknown exposure/profile, missing lagged CU, stale
station pairs, constant correlations, empty outputs, manifest registration,
create-only behavior, and failure cleanup. Existing corrected fingerprints
must remain unchanged. Review representative graphics and measure reporting
cost separately from full capture-run timing before publication.

## Deferred questions

- Validate independent receiver/exporter paths and cross-capture duplicates.
- Establish known listening exposure and ERM own-versus-neighbour classification.
- Compare simultaneous native and ERM captures when that experiment is available.
- Characterize model/radio-topology periodic delivery patterns, including AP-555
  and AP-635, before defining any baseline correction.
- Investigate CU/station associations and potential nonlinear thresholds using
  exposure-qualified data; do not infer unseen CU from surrounding beacons.
