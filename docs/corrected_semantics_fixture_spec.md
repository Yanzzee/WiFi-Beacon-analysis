# Corrected Semantics Fixture and Oracle Specification

> The mapped-TSF expectations below describe historical metric v1. The owner
> superseded that timing policy with [ADR-0019](decisions/0019-observation-time-and-native-tsf-diagnostics.md).
> V2 uses observation time for bins and slots, preserves the v1 fixture files
> as historical evidence, and tests the new policy and native TSF anomalies
> separately. The integrated fixture's CU/station measurement values remain
> valid; its old mapped-clock metadata is not a v2 expectation.

- Status: Accepted; machine-readable fixture data and corrected golden created
- Date drafted: 2026-09-09
- Scope: Documentation, synthetic fixture data, and expected results only
- Implementation: Active corrected timing/selection, identity, summary,
  artifact, manifest, and report pipeline with focused oracle tests
- Roadmap items: PR-0021, PR-0022, PR-0039, PR-0056
- Decisions: [ADR-0003](decisions/0003-ap-and-physical-radio-identity-contract.md),
  [ADR-0004](decisions/0004-missing-cu-and-missing-beacon-semantics.md),
  [ADR-0005](decisions/0005-class-interval-boundary-semantics.md),
  [ADR-0008](decisions/0008-aruba-capture-timestamp-authority-and-fallback.md),
  and [ADR-0009](decisions/0009-tbtt-tsf-beacon-slot-and-bin-semantics.md)

## Purpose

An oracle fixture is a deliberately small input whose correct output is written
down before implementation. These fixtures test the selected rules directly;
they do not attempt to imitate the statistical shape of a real capture.

The suite should have two layers:

1. focused rule fixtures, each small enough to review by eye; and
2. one compact integrated fixture whose five approved logical artifacts become
   the corrected-version golden.

No historical pipeline runs beside these fixtures. A frozen same-input
comparison is retained in
`tests/golden/legacy_corrected_comparison_v1.json`; it is explanatory rather
than a supported oracle. Representative Aruba/Extreme capture replay validates
adapter integration but is not the oracle for calculation semantics.

## Shared notation and initial configuration

Times use offsets from `S = 2026-01-15T10:00:00Z` unless an ISO timestamp is
shown. The focused fixtures use the accepted current-Aruba timing values:

| Name | Value | Meaning |
|---|---:|---|
| `R` | `0.1024 s` | Nominal raw beacon interval |
| useful metadata cadence | `1.024 s` | Ten nominal beacon intervals |
| `W` | `1.024 s` | Class-analysis bin width |
| high CU | `> 50%` | Raw QBSS CU `128..255` |
| very high CU | `> 75%` | Raw QBSS CU `192..255` |

`O` means the preferred observation timestamp: valid Aruba ERM time, otherwise
valid frame time. `F` means TSF from `wlan.fixed.timestamp`; TSF examples below
use microseconds. For a reliable BSSID/session, `B=min(O-F)` within the highest-
precedence available timestamp-source cohort is its best-effort absolute TSF
origin and `T=B+F` is its TSF-derived binning time. Fixture source
ranks are explicit inputs: within the same timestamp-source class, rank `1`
wins over rank `2`. Valid Aruba ERM time precedes frame-only time. The caller
declares capture-source rank; PR-0025 still owns how the Aruba adapter assigns
it. Equal ranks use earliest preferred observation time and stable source-
path/frame order.

CU averages are shown both on the raw `0..255` scale and as
`raw / 255 * 100`. Production output may retain only the accepted physical
representation, but it must produce the same value.

## Fixture matrix

| ID | Rule isolated | Required expected result |
|---|---|---|
| CF-01 | Observation-time precedence | Aruba time wins when valid; frame time is fallback; no valid time produces an explicit exclusion diagnostic |
| CF-02 | TSF anchor, binning, ordering, duplicates, gaps, and fallback | Timestamp admits class candidates; reliable TSF uses per-BSSID/session `B=min(O-F)` and `T=B+F` for bins/order; delayed first anchor, duplicate, skipped, outside-class, frozen/reset, and gross-mismatch cases have explicit results |
| CF-03 | Class and bin boundaries | Half-open class/bin membership, exact-edge placement, adjacent-class ownership, terminal partial bin, and session shorter than one bin |
| CF-04 | Fixed-bin first-observed selection | After duplicate-view reconciliation, select the earliest observation independently in each BSSID/class-start bin; no rolling minimum-spacing gate |
| CF-05 | Four radio/bin CU states | Observed high, observed non-high including measured zero, beacon with missing CU, and no beacon remain distinct |
| CF-06 | Member/preferred BSSID fallback and station-count grain | Preferred member remains stable; another member fills only a missing/invalid preferred CU bin; station count still retains every selected BSSID/bin |
| CF-07 | Coverage, durations, and average | Valid, missing-CU, and no-beacon durations partition the class; partial duration is exact; unknown CU is excluded from the duration-weighted average |
| CF-08 | High and very-high metrics | Raw threshold edges and independent duration/possible-connected/actual-observed streaks at both thresholds |
| CF-09 | Per-radio and classroom rollup | Radios are evaluated independently; alternating radios do not join; classroom maxima retain all logically tied responsible radios; counts are per band |
| CF-10 | Corrected artifact and manifest contract | Keys are unique, references resolve, requested/actual time ranges and concise log status are present, and no legacy result set is emitted |

## CF-01 — Observation-time precedence

### Input and expected selection

| Row | Aruba ERM time | Frame time | Expected `O` | Expected source | Expected disposition |
|---|---|---|---|---|---|
| T1 | `S+0.180` valid | `S+0.100` valid | `S+0.180` | Aruba ERM | Keep; Aruba wins even though frame time is earlier |
| T2 | missing | `S+0.200` valid | `S+0.200` | frame time | Keep using fallback |
| T3 | malformed | `S+0.300` valid | `S+0.300` | frame time | Keep using fallback and retain invalid-Aruba provenance |
| T4 | missing | missing | null | none | Exclude from class/bin placement with `source_timestamp_unavailable` diagnostic |
| T5 | `S+0.500` valid | `S+0.500` valid | `S+0.500` | Aruba ERM | Keep; source identity remains Aruba ERM |

No timestamp is repaired, averaged, or derived from TSF.

## CF-02 — TSF-derived time, binning, duplicates, and fallback

Use class `C-tsf = [S, S+3.072)` with `W=1.024 s`. Observation time first
admits candidates to the class. Duplicate views are reconciled before `B` is
calculated. All CF-02 rows use one timestamp-source cohort; focused CF-01 owns
Aruba-before-frame timestamp-source precedence.

### Input observations

| Row | BSSID | Input order | `O` | `F` | Source rank | Lineage | Payload |
|---|---|---:|---:|---:|---:|---|---|
| D1 | A | 1 | `S+1.050` | `1890000` | 2 | `L2` | P1 |
| D2 | A | 2 | `S+1.060` | `1890000` | 1 | `L1` | P1 |
| D3 | B | 3 | `S+1.040` | `1890000` | 1 | `L3` | P1 |
| D4 | A | 4 | `S+1.320` | `2204800` | 1 | `L4` | P2 |
| D5 | A | 5 | `S+1.100` | `2000000` | 1 | `L5` | P3 |
| D6 | C | 6 | `S+0.200` | `0` | 1 | `L6` | P4 |
| D7 | C | 7 | `S+1.200` | `0` | 1 | `L7` | P5 |
| D8 | D | 8 | `S+0.300` | `500000` | 1 | `L8` | P6 |
| D9 | D | 9 | `S+1.300` | `100000` | 1 | `L9` | P7 |
| D10 | E | 10 | `S+0.400` | `1000000` | 1 | `L10` | P8 |
| D11 | E | 11 | `S+1.400` | `3601000000` | 1 | `L11` | P9 |
| D12 | F | 12 | `S+3600.000` | `2500000` | 1 | `L12` | P10 |
| D13 | G | 13 | `S+0.010` | `1000000` | 1 | `L13` | P11 |
| D14 | G | 14 | `S+1.000` | `2024000` | 1 | `L14` | P12 |

### Expected TSF results

| BSSID | TSF status | Expected retained/order result | Expected diagnostics |
|---|---|---|---|
| A | usable/advancing | D2 represents `(A,1890000)` because source rank 1 wins. Origin candidates are `S-0.830` from D2, `S-0.900` from D5, and `S-0.8848` from D4, so `B=S-0.900`. TSF order is D2, D5, D4. Their `T` values are `S+0.990`, `S+1.100`, and `S+1.3048`; D2 therefore belongs to bin 0 even though its `O` belongs to bin 1. | One duplicate view collapsed; D5 rather than delayed first D2 anchors the session; forward gap from `2000000` to `2204800` notes one skipped nominal interval; no row synthesized |
| B | insufficient progression | D3 remains independent even though its TSF equals A's TSF; the BSSID/session uses `O` because one observation cannot establish reliable progression | `tsf_unusable_insufficient_progression` |
| C | frozen | Retain D6 and D7 and order by `O`; TSF does not identify them as one duplicate beacon | `tsf_unusable_frozen` |
| D | reversing/reset | Retain D8 and D9 and order by `O` | `tsf_unusable_reversing` |
| E | grossly incompatible | The one-hour-scale TSF change is incompatible with the one-second observation-time change; retain and bin both rows by `O` for the entire BSSID/session | `tsf_unusable_time_mismatch` |
| F | outside class by `O` | Exclude D12 before TSF reliability or origin calculation | `outside_class_observation_time` |
| G | usable with mapped edge exclusion | Origin candidates are `S-0.990` and `S-1.024`, so `B=S-1.024`. D13 maps to `T=S-0.024` and is excluded rather than clamped into bin 0. D14 maps to `T=S+1.000` and remains in bin 0. | `tsf_time_outside_class` for D13 |

The implemented gross-mismatch sanity check compares the elapsed TSF span with
the elapsed observation-time span for already admitted rows. It falls back only
when those spans differ by more than the complete scheduled class duration.
This catches the accepted wrong-hour/day case without drift fitting or a fine-
grained clock-quality profile.

The diagnostic codes shown here are now pinned by the isolated selector tests.
Their eventual durable placement remains adapter/output integration work. The
semantic assertions are class admission, timing method, origin choice, bin,
status, retained rows, ordering, duplicate count, and zero synthesized
observations.

## CF-03 — Class membership and bin geometry

Use class `C-edge = [S, S+2.560)` with `W=1.024 s`.

### Expected bins

| Bin | Interval | Actual duration | Width status |
|---:|---|---:|---|
| 0 | `[S, S+1.024)` | `1.024 s` | complete |
| 1 | `[S+1.024, S+2.048)` | `1.024 s` | complete |
| 2 | `[S+2.048, S+2.560)` | `0.512 s` | partial |

### Boundary observations

| Observation `O` | Expected class result | Expected bin |
|---|---|---:|
| `S-0.000001` | outside | — |
| `S` | included | 0 |
| `S+1.023999` | included | 0 |
| `S+1.024000` | included | 1 |
| `S+2.048000` | included | 2 |
| `S+2.559999` | included | 2 |
| `S+2.560000` | excluded | — |

If a second class starts exactly at `S+2.560`, the final observation belongs
only to that second class. A separate short class `[S, S+0.400)` must produce
one partial bin of exactly `0.400 s` rather than no bins or one full-width bin.

## CF-04 — Fixed-bin first-observed selection

The corrected metric applies no rolling minimum-spacing gate. This focused
case uses observation-time fallback, so `O` is its selection coordinate. After
duplicate-view reconciliation, representative selection operates independently
within each BSSID/bin. A selection in one bin cannot suppress a candidate in
another.

### BSSID X: adjacent-bin independence

| Candidate `O` | Bin | Representative result |
|---:|---:|---|
| `S+0.1000` | 0 | selected for bin 0 |
| `S+0.5000` | 0 | not selected because it is later in bin 0 |
| `S+1.0500` | 1 | selected for bin 1 |
| `S+1.0800` | 1 | not selected because it is later in bin 1 |

### BSSID Y: multiple observations in one bin

| Candidate `O` | Bin | Representative result |
|---:|---:|---|
| `S+0.0100` | 0 | selected for bin 0 |
| `S+0.9900` | 0 | not selected because it is later |

The expected selected rows are therefore exactly `(X,0,S+0.1000)`,
`(X,1,S+1.0500)`, and `(Y,0,S+0.0100)`. In particular, X's bin-1 result does
not depend on the time selected in bin 0.

## CF-05 through CF-10 — Integrated corrected fixture

These cases share one small class so radio fallback, missingness, durations,
thresholds, classroom rollup, artifact keys, and manifest values agree across
the complete golden.

### Class and expected bins

Class `C1` is `[S, S+8.500)`. Bins 0 through 7 are complete `1.024 s`
intervals. Bin 8 is `[S+8.192, S+8.500)` and is partial with duration
`0.308 s`.

### Project identity map input

Short IDs are fixture aliases used to keep the oracle reviewable by eye.

| BSSID alias | SSID | Radio | AP | Classroom | Band | Channel | Mapping expectation |
|---|---|---|---|---|---|---:|---|
| A24 | eduroam | R24 | AP1 | ROOM1 | 2.4 GHz | 6 | resolved |
| A5 | eduroam | R5 | AP1 | ROOM1 | 5 GHz | 36 | resolved |
| B5 | guest | R5 | AP1 | ROOM1 | 5 GHz | 36 | resolved member of the same radio as A5 |
| A6 | eduroam | R6 | AP1 | ROOM1 | 6 GHz | 5 | resolved |

Expected classroom radio counts are `2.4 GHz=1`, `5 GHz=1`, `6 GHz=1`, and
derived total `3`.

Counts use resolved radio identity, not channel number. Two distinct resolved
radios on the same band and channel remain two radios and add two to that
band's count. Channel is descriptive/separation evidence, not the counting key.

### Normalized observation input

These rows have valid `O`. Where there are enough advancing values, their
reliable TSF-derived times produce the same listed bins as `O`. A24 has only
one observation and therefore demonstrates whole-BSSID/session observation-
time fallback. The focused fixtures above separately test timestamp
precedence, lower-envelope anchoring, cross-bin TSF placement, and fallback.

| Row | `O` offset | BSSID | Bin | Raw CU | Note |
|---|---:|---|---:|---:|---|
| I01 | `0.100` | A5 | 0 | 153 | First valid R5 member; A5 becomes preferred |
| I02 | `0.150` | B5 | 0 | 153 | Same-radio member; not needed for radio CU |
| I03 | `1.100` | A5 | 1 | 204 | Preferred supplies valid CU |
| I04 | `1.130` | A5 | 1 | 204 | Duplicate view of I03; lower source priority; discard |
| I05 | `2.100` | A5 | 2 | null | Preferred beacon received without CU |
| I06 | `2.120` | B5 | 2 | null | Other member also lacks CU |
| I07 | `4.200` | A5 | 4 | null | Preferred lacks CU |
| I08 | `4.220` | B5 | 4 | 204 | Valid fallback for this bin only |
| I09 | `5.200` | A5 | 5 | 0 | Measured zero, not missing |
| I10 | `6.200` | B5 | 6 | 204 | Preferred absent; valid fallback |
| I11 | `7.200` | A5 | 7 | 153 | Preferred returns |
| I12 | `8.250` | A5 | 8 | 204 | Preferred in terminal partial bin |
| I13 | `0.200` | A6 | 0 | 204 | R6 very high |
| I14 | `1.200` | A6 | 1 | 204 | R6 very high |
| I15 | `2.200` | A6 | 2 | 0 | R6 measured zero |
| I16 | `0.300` | A24 | 0 | 0 | R24 measured zero |

I03 and I04 share one `(BSSID, TSF)` value in the machine fixture. I03 wins by
declared source priority. No R5 member observation occurs in bin 3. All rows
not explicitly marked as duplicate views are distinct beacons.

### Expected selected-observation result

The corrected selected-observation artifact contains I01, I02, I03, I05
through I16. I04 is absent. Its uniqueness key is
`(class_session_id, BSSID, bin_number)`. Both A5 and B5 remain in bin 0 because
selection is per BSSID, not per radio. Missing CU remains null.

### Expected R5 radio/bin states and CU source

| Bin | Duration | R5 state | Radio CU raw | CU source | Fallback used |
|---:|---:|---|---:|---|---|
| 0 | 1.024 | observed high, not very high | 153 | A5 | no |
| 1 | 1.024 | observed high and very high | 204 | A5 | no |
| 2 | 1.024 | beacon missing CU | null | none | no valid member |
| 3 | 1.024 | no beacon | null | none | no |
| 4 | 1.024 | observed high and very high | 204 | B5 | yes |
| 5 | 1.024 | observed non-high, measured zero | 0 | A5 | no |
| 6 | 1.024 | observed high and very high | 204 | B5 | yes |
| 7 | 1.024 | observed high, not very high | 153 | A5 | no |
| 8 | 0.308 | observed high and very high | 204 | A5 | no |

A5 remains the preferred R5 BSSID for the whole class. B5's valid values in
bins 4 and 6 do not change that preference.

CU-source preference does not apply to station count. In bin 0, A5 contributes
10 stations and B5 contributes 5, so R5's radio/bin station count is 15 even
though A5 alone supplies radio CU. At classroom grain for that bin, the
expected SSID totals are `eduroam=21` (A5 10 + A6 8 + A24 3) and `guest=5`;
the expected band totals are `2.4=3`, `5=15`, and `6=8`. These values pin the
aggregation entities only. Missing/partial station-total semantics remain a
separate PR-0053 decision.

### Expected compact state sequences for the other radios

| Radio | Bins with valid CU | State sequence summary |
|---|---|---|
| R6 | 0=`204`, 1=`204`, 2=`0` | bins 0–1 high and very high; bin 2 measured non-high; bins 3–8 no beacon |
| R24 | 0=`0` | bin 0 measured non-high; bins 1–8 no beacon |

### Expected radio/class summaries

Percentages below are rounded for display only; tests should compare exact
accepted numeric representations or an explicitly selected tolerance.

| Radio | Band | Members | Preferred | Class duration | Valid CU | Missing CU | No beacon | CU coverage | Avg CU | High duration | High possible | High actual | Very-high duration | Very-high possible | Very-high actual |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| R5 | 5 | 2 | A5 | 8.500 | 6.452 | 1.024 | 1.024 | 75.9059% | 60.9547% | 5.428 | 3.072 | 2.356 | 3.380 | 2.048 | 1.024 |
| R6 | 6 | 1 | A6 | 8.500 | 3.072 | 0.000 | 5.428 | 36.1412% | 53.3333% | 2.048 | 2.048 | 2.048 | 2.048 | 2.048 | 2.048 |
| R24 | 2.4 | 1 | A24 | 8.500 | 1.024 | 0.000 | 7.476 | 12.0471% | 0.0000% | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

Each radio/class row also carries a fixed 256-position
`valid_cu_raw_counts` vector populated from the same preferred/fallback
radio/bin choices. The only nonzero entries for this fixture are:

| Radio | Raw CU counts |
|---|---|
| R5 | `0:1`, `153:2`, `204:4` |
| R6 | `0:1`, `204:2` |
| R24 | `0:1` |

The sum of each vector is the number of radio bins with valid CU. Missing-CU
and no-beacon bins do not increment raw value zero.

R5's duration-weighted mean is:

```text
((153 * 1.024) + (204 * 1.024) + (204 * 1.024) +
 (0 * 1.024) + (204 * 1.024) + (153 * 1.024) +
 (204 * 0.308)) / 6.452
= 155.4346 raw
= 60.9547%
```

For every radio:

```text
valid_cu_duration + missing_cu_duration + no_beacon_duration
= class_duration
```

### Expected classroom/class summary

| Field | Expected value |
|---|---|
| `class_session_id` | C1 |
| classroom | ROOM1 |
| class duration | `8.500 s` |
| 2.4 GHz radio count | 1 |
| 5 GHz radio count | 1 |
| 6 GHz radio count | 1 |
| derived total radio count | 3 |
| high possible-connected maximum | `3.072 s`, responsible `{R5}` |
| high actual-observed maximum | `2.356 s`, responsible `{R5}` |
| very-high possible-connected maximum | `2.048 s`, responsible logical set `{R5, R6}` |
| very-high actual-observed maximum | `2.048 s`, responsible `{R6}` |

The logical tie set is exact and is stored as a Parquet `list<string>` in the
accepted physical contract.

### Expected run manifest facts

| Manifest fact | Expected value |
|---|---|
| requested sample start | `2026-01-15T10:00:00Z` |
| requested sample end | `2026-01-15T10:00:08.500Z` |
| actual first selected observation | `2026-01-15T10:00:00.100Z` |
| actual last selected observation | `2026-01-15T10:00:08.250Z` |
| nominal raw interval | `0.1024 s` |
| useful cadence | `1.024 s` |
| bin width | `1.024 s` |
| timing policy | class admission by `O`; reliable per-BSSID/session `B=min(O-F)` and `T=B+F` binning; whole-BSSID/session `O` fallback |
| thresholds | high `>50%`; very high `>75%` |
| stage log summary | each completed stage has start, end, status, warning count, and error count |
| detailed log | optional path/reference; full text is not embedded |
| artifacts | references to selected observations, identity map, radio/class summary, and classroom/class summary |
| legacy artifacts | none |
| schema and metric identifiers | `beacon.schema/analysis-results/v1`; `beacon.metric/corrected-cu/v1` |
| project identity | display name `Corrected Semantics Oracle`; slug `oracle-corrected` |
| input provenance | full aggregate SHA-256 plus the three fixture input file hashes, sizes, roles, and references |
| code provenance | fixture revision `beacon.fixture/corrected-semantics/v1` |

## Cross-artifact invariants

The machine tests derived from this specification must assert:

1. selected-observation keys are unique;
2. each selected BSSID resolves through the identity map or carries an explicit
   unresolved/BSSID-grain state;
3. source lineage identifies every selected observation;
4. every selected observation records `O` and its timing method; usable TSF
   rows also record `F`, `B`, and `T`, while fallback rows retain the unusable-
   TSF reason;
5. no missing beacon row is synthesized in selected observations;
6. missing CU remains null and never becomes measured zero;
7. each radio/bin has exactly one of the four accepted CU states;
8. duration components equal scheduled class duration exactly;
9. all CU averages exclude unknown bins and weight the terminal partial bin by
   its actual duration;
10. high and very-high metrics are distinct at raw boundary values;
11. classroom streaks are maxima of individual radio streaks, never a mixture;
12. per-band radio counts agree with the project identity map;
13. every output artifact references the same run, schema, and metric versions;
    and
14. preferred/fallback CU selection does not remove another member BSSID's
    selected station count; the fixture's R5 bin-0 station total is 15; and
15. CSV/plot generation, when tested, consumes these artifacts and does not
    recalculate different metrics.

## Exact threshold-edge supplement

CF-08 also needs four one-bin rows independent of the integrated fixture:

| Raw CU | Percent | High `>50%` | Very high `>75%` |
|---:|---:|---|---|
| 127 | 49.8039% | no | no |
| 128 | 50.1961% | yes | no |
| 191 | 74.9020% | yes | no |
| 192 | 75.2941% | yes | yes |

Because raw QBSS CU is an integer on `0..255`, exact 50% and 75% values are not
representable. These four raw values pin the practical meaning of “greater
than” without floating-point boundary ambiguity.

## Tracked fixture files

The accepted machine-readable fixture data and expected artifacts are:

```text
tests/fixtures/corrected_semantics/
  README.md
  input/
    classes.csv
    identity_evidence.csv
    observations.csv
  expected/
    project_identity_map.csv
    selected_observations.csv
    radio_class_summary.csv
    classroom_class_summary.csv
    manifest.json

tests/golden/corrected_semantics_v1.json
```

Small focused test modules should read the human-readable tables directly.
The corrected golden compares stable Parquet/JSON/CSV semantic content, not
PNG bytes or Parquet file bytes.

## Implementation history and current boundary

PR-0024's physical choices are closed. The project owner accepted the metric
and schema IDs, run-scoped paths, `class_date` selected-observation
partitioning, Parquet string-list tie representation, integer microsecond
durations/TSF, UTC nanosecond timestamps, nullable raw CU, unrounded float64
ratios, source precedence, and concise manifest logging.

[`corrected_beacon_selection.py`](../corrected_beacon_selection.py) implements
the accepted calculation as an isolated, side-effect-free component, and
[`test_corrected_beacon_selection.py`](../tests/test_corrected_beacon_selection.py)
connects it to these inputs and expected results. The PR-0039 writer and its
physical list types are checked by
[`test_project_identity_output.py`](../tests/test_project_identity_output.py).
The selected-observation logical builder and atomic Parquet dataset writer are
implemented in
[`selected_observation_output.py`](../selected_observation_output.py). Its
focused tests compare all accepted oracle columns, retain nullable QBSS values,
and verify the UTC `class_date=YYYY-MM-DD` partition layout and exact Arrow
schema.
The accepted JSON manifest now has a validated builder, duplicate-key-safe
loader, project/input/code provenance, capture-plan provenance, and atomic
non-overwriting writer in [`run_manifest.py`](../run_manifest.py). The
create-only project/run staging, logging, cleanup, and whole-directory
publication boundary is implemented in
[`run_lifecycle.py`](../run_lifecycle.py). The minimal integration in
[`corrected_run.py`](../corrected_run.py) stages the identity map and selected
observations together, requires every selected BSSID to exist in the identity
map, records their provenance, and refuses final publication until the
radio/class and classroom/class summary tables are also present. The accepted
ADR-0004 calculator and Parquet writers in
[`corrected_summary_output.py`](../corrected_summary_output.py) now produce
those tables from the complete expected-bin grid, identity map, and selected
observations, including the one-value-per-radio/bin CU count vector; the
runner invokes them directly rather than registering arbitrary files.
Capture conversion now
retains its required current-Aruba timing/lineage
fields, and `aruba_beacon_adapter.py` normalizes them in memory and feeds this
component. Configured-hostname AP/classroom scope, stable current-Aruba radio
identity, durable identity-map writing, and durable selected-observation
writing are implemented. The two summary-table writers reproduce these
accepted logical oracles, including the additive UTC class boundaries, and are
connected to the publication gate. The first bounded corrected report package
is also derived inside that gate. Capture-to-run orchestration and the public
acceptance command are implemented; final owner report review remains separate.
The fixture is connected to the supported end-to-end workflow. No historical
golden pipeline is retained or needed.

The later Alpha 0.2 reporting/navigation changes are documented in
[reporting and publication](reporting_and_publication.md). They do not change
this authoritative calculation fixture or require an Original replay.
