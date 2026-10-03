# ADR-0004: Missing CU, no-beacon, coverage, and streak semantics

- Status: Accepted
- Date proposed and decided: 2026-09-02
- Owners/reviewers: Project owner, also serving as analysis-correctness
  reviewer
- Roadmap items:
  [PR-0048 through PR-0052](../post_refactor_roadmap.md)
- Related decisions:
  [ADR-0003](0003-ap-and-physical-radio-identity-contract.md),
  [ADR-0005](0005-class-interval-boundary-semantics.md),
  [ADR-0006](0006-output-and-metric-versioning.md), and
  [ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md)

> Current implementation status (2026-10-02): these summaries and reports are
> produced by `beacon_analysis.py run` and pass through the run publication
> gate. Historical comparison language below explains metric intent and does
> not require the removed workflow.

## Plain-language purpose

This decision keeps “the radio reported low utilization” separate from “the
utilization value is unknown.” Missing CU never means zero.

It defines a small set of per-radio/class metrics for valid CU, missing CU,
no-beacon time, coverage, and two streak interpretations:

- a possible connected streak that can connect high bins across unknown bins
  and corresponds to one useful legacy comparison; and
- an actual observed streak that ends whenever CU is not observed.

## Scope

This ADR decides:

- the state of each radio/class/bin;
- how member BSSIDs supply one radio-level CU observation;
- CU averages and null results;
- valid-CU coverage and missing-duration metrics;
- possible-connected and actual-observed high-CU streaks; and
- how the classroom longest streak is derived from radio results.

It does not decide station-count or admission-capacity missingness. It does not
impute CU, synthesize missing beacons, infer packet loss, or add a minimum-
coverage suppression threshold.

## Bin states

After ADR-0005 class-start binning, ADR-0009 duplicate handling, and ADR-0003
radio grouping, each radio/bin has exactly one of these states:

| State | Meaning | CU meaning |
|---|---|---|
| `observed_high` | At least one selected member-BSSID beacon supplies valid CU above the selected threshold | Observed high |
| `observed_non_high` | At least one selected member-BSSID beacon supplies valid CU at or below the threshold, including measured zero | Observed non-high |
| `beacon_missing_cu` | One or more member-BSSID beacons were selected, but none supplies valid CU | Unknown CU; beacon reception was observed |
| `no_beacon` | No member-BSSID beacon was selected in the bin | Unknown CU; beacon reception was not observed |

The first two states are threshold-specific labels over the same valid CU
observation. Coverage and missingness are independent of the selected high-CU
threshold.

The first corrected CU metric version evaluates two named thresholds
independently:

- **high CU:** channel utilization greater than 50%, equivalent to raw QBSS CU
  values `128` through `255`; and
- **very high CU:** channel utilization greater than 75%, equivalent to raw
  QBSS CU values `192` through `255`.

A value at or below a threshold is non-high for that threshold. One observation
can therefore be high without being very high. Duration and both streak metrics
are calculated separately for each threshold.

Missing, malformed, corrupt, or rejected CU is never converted to zero. A
measured CU value of zero remains a valid `observed_non_high` observation.

## Selecting one CU value per radio/bin

CU is assumed to match across BSSIDs assigned to the same radio. The processor
does not compare those values or require an agreement check.

A **member BSSID** is simply a BSSID that the project identity map assigns to
that `radio_group_id`. A radio can have several member BSSIDs because it can
advertise several SSIDs/BSSs. Membership describes radio topology; it does not
mean that every member supplies the radio's CU value in every bin.

For cadence consistency:

1. ADR-0009 first produces at most one selected observation per BSSID/bin.
2. The first member BSSID that supplies valid CU in class order becomes the
   preferred CU source for that radio/class session.
3. Use the preferred BSSID's valid CU whenever it is available in a bin.
4. If that BSSID is absent or its CU is invalid/missing, use the earliest valid
   CU from another member BSSID in the same bin.
5. Apply accepted source precedence and deterministic lineage ordering before
   choosing among otherwise equivalent observations.

The preferred BSSID remains stable for the session; a temporary substitute
does not become the new preferred source. This minimizes adjacent samples from
different beacon phases while allowing another same-radio BSSID to fill a
missing observation.

If selected member-BSSID beacons exist but none has valid CU, the radio/bin is
`beacon_missing_cu`. If no selected member-BSSID beacon exists, it is
`no_beacon`.

## Station-count separation

The preferred/fallback BSSID rule above applies only to repeated radio-level
values such as CU and admission capacity. It must not discard station-count values. ADR-0009 retains
at most one selected beacon for every BSSID/bin, and station-count processing
uses every such BSSID row that carries a valid station count.

The accepted station-count grains are:

- one selected value per BSSID/bin;
- a radio/bin total summed across all member BSSIDs;
- an SSID/classroom/bin total summed across in-scope BSSIDs advertising that
  SSID; and
- a band/classroom/bin total summed across in-scope BSSIDs whose radios are in
  that band.

For example, if a radio's preferred CU BSSID reports 10 stations and another
member BSSID reports 5 in the same bin, radio CU still comes from one source,
but the radio station count is 15. `preferred_bssid` is therefore recorded in
the radio/class CU result, not as static project identity-map membership.

This clarification fixes entity and rollup grain only. Missing station-count
values never mean zero, but the policy for partial totals when some member
BSSIDs are missing or invalid remains PR-0053 work.

## Duration and coverage metrics

Use each bin's actual clipped duration from ADR-0005, including the terminal
partial bin.

For every radio/class session, record:

| Metric | Definition |
|---|---|
| `class_duration` | Sum of all expected class-bin durations; equal to scheduled class duration |
| `valid_cu_duration` | Sum of bin durations with a valid selected CU value |
| `missing_cu_duration` | Sum of `beacon_missing_cu` bin durations |
| `no_beacon_duration` | Sum of `no_beacon` bin durations |
| `cu_coverage` | `valid_cu_duration / class_duration` |

These durations partition the class:

```text
valid_cu_duration + missing_cu_duration + no_beacon_duration
  = class_duration
```

No-beacon and missing-CU durations are reported separately so low coverage has
an explicit cause.

## CU averages

Calculate average CU using only bins with valid selected CU. Unknown bins are
excluded from both the numerator and the average's observation denominator.

Use bin duration as the weight so a terminal partial bin contributes only its
actual duration:

```text
average_cu = sum(CU_bin * bin_duration) / valid_cu_duration
```

If `valid_cu_duration` is zero, `average_cu` is null. It is never reported as
zero merely because no valid observations exist. Report `cu_coverage` beside
the average.

## High-CU duration and streak metrics

For each configured high-CU threshold, calculate the following independently
for each radio/class session.

### Total observed high duration

`observed_high_duration` is the sum of actual durations of all
`observed_high` bins. Unknown bins never add high duration.

### Possible connected streak

`possible_connected_high_streak_duration` uses the specifically requested
connected comparison logic:

- `observed_high` adds the bin's actual duration and continues the streak;
- `observed_non_high` ends and resets the streak; and
- `beacon_missing_cu` or `no_beacon` neither adds duration nor ends the
  streak.

High bins on both sides of unknown bins can therefore belong to one possible
connected streak. The reported duration contains only observed high-bin
duration; the unknown gap duration is reported separately by the
missing-duration metrics above. This is not proof of continuous observed high
CU and does not create a promise to reproduce the complete legacy pipeline.

### Actual observed streak

`observed_high_streak_duration` is the longest run of consecutive
`observed_high` bins:

- `observed_high` adds actual bin duration and continues the streak;
- `observed_non_high` ends the streak;
- `beacon_missing_cu` ends the streak; and
- `no_beacon` ends the streak.

No unknown interval is imputed as high. This is the primary corrected streak
metric.

## Classroom result

Calculate all CU durations, coverage, averages, and streaks independently per
radio first. Do not combine alternating high bins from different radios into
one streak.

For each class session and threshold:

- the classroom possible-connected longest streak is the maximum
  `possible_connected_high_streak_duration` from any in-classroom radio; and
- the classroom actual-observed longest streak is the maximum
  `observed_high_streak_duration` from any in-classroom radio.

Retain the responsible radio identity with each classroom maximum. How a tie
between radios is represented physically remains an output-schema decision;
the maximum metric value is unchanged. If no radio has valid CU, CU averages
and high-CU results are null; coverage and missing-duration metrics still
describe the class.

## Invariants

- Missing CU never means zero.
- No observation produces a null CU result, not a measured-zero result.
- Unknown bins never contribute to an average or observed high duration.
- Unknown bins end the actual observed streak.
- Unknown bins bridge, but do not add duration to, the possible-connected
  streak.
- Coverage uses actual time with valid CU divided by total scheduled class
  duration.
- Missing-CU and no-beacon durations remain separate.
- One radio contributes at most one CU value per bin.
- Same-radio BSSID CU equality is assumed and is not verified.
- A substitute BSSID fills only a missing preferred-BSSID bin and does not
  change the session's preferred source.
- CU-source preference never filters selected per-BSSID station-count values.
- Metrics are computed per radio before the classroom maximum is selected.
- No missing beacon or CU value is synthesized or imputed.

## Required fixtures and tests

Focused fixtures must cover:

- measured zero versus missing CU;
- observed high and observed non-high bins;
- a received beacon with missing, malformed, or corrupt CU;
- an expected bin with no beacon;
- all four states in one class and exact duration partitioning;
- full and terminal-partial bin duration weighting;
- average CU excluding unknown bins and null average with no valid CU;
- a high–unknown–high sequence that connects only in the possible-connected
  streak;
- missing-CU and no-beacon bins each ending the actual observed streak;
- stable preferred member-BSSID selection;
- another member BSSID supplying valid CU when the preferred BSSID is absent
  or invalid without becoming the new preferred source;
- per-radio streak calculation with alternating high bins on different radios;
  and
- classroom maximum retaining the responsible radio identity.

The possible-connected metric remains available because it is useful in its
own right. The complete legacy pipeline is retained only at commit
`4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb`; production does not need to run
or store it beside the corrected metrics. Activating corrected metrics still
requires ADR-0006/PR-0024 versioning, output placement, and reviewed goldens.

## Implementation record — 2026-09-24

`corrected_summary_output.py` implements the accepted CU portion of this ADR.
It consumes the complete expected class-bin grid, project identity map, and
selected per-BSSID/bin observations. For every in-classroom radio it chooses a
stable preferred BSSID, uses another member only when necessary, distinguishes
valid CU from beacon-without-CU and no-beacon time, and calculates the accepted
duration-weighted average, coverage, missing durations, high and very-high
durations, and both streak interpretations. Empty or entirely unknown radio
sessions retain null averages and threshold results rather than becoming zero.

The same component derives classroom maxima from complete per-radio results,
retains every tied responsible radio ID, and reports distinct radio counts by
band. It writes the accepted `radio_class_summary.parquet` and
`classroom_class_summary.parquet` schemas without overwriting existing files.
`corrected_run.py` invokes the writer inside lifecycle-owned staging, records
both artifacts, and keeps the existing all-artifact publication gate. Focused
tests reproduce the accepted human-readable oracle, exercise exact threshold
edges and no-observation null behavior, validate physical Arrow schemas, and
verify runner publication. Station-count rollups remain outside this ADR slice
as already documented under PR-0053. The radio summary's singular `channel`
field is populated only when the radio has one observed channel in the
available identity/selected evidence; a multi-channel session retains its
complete channel list in the project identity map and leaves this singular
summary field null rather than choosing an arbitrary channel.

## Consequences

### Benefits

- Unknown data cannot silently lower CU averages or appear as measured zero.
- The observed streak has a direct, conservative meaning.
- The possible-connected result remains available as a qualified estimate.
- Coverage and separate missing durations explain incomplete results.
- A stable same-radio BSSID source supports consistent sampling cadence while
  allowing simple gap filling.
- Classroom results cannot combine unrelated radio streaks.

### Trade-offs

- The actual observed streak can be shorter when capture coverage is poor.
- The possible-connected streak can bridge long unknown gaps, so it must remain
  labeled as a possible measure and shown with coverage.
- No minimum-coverage suppression rule is selected; consumers must interpret
  results alongside coverage.

## Complexity guardrail

This ADR deliberately does not add imputation, probabilistic bounds, dynamic
cadence estimation, CU agreement checks, or coverage-based suppression.
Introduce any such behavior only after the project owner confirms the
practical need at a high level.
