# ADR-0010: Station-count rollup and staleness semantics

- Status: Accepted
- Date proposed: 2026-10-03
- Date decided: 2026-10-03
- Owners/reviewers: Project owner, also serving as analysis-correctness reviewer
- Roadmap items: Alpha 0.2 station-count gate
- Supersedes: None
- Superseded by: None
- Related decisions:
  [ADR-0003](0003-ap-and-physical-radio-identity-contract.md),
  [ADR-0004](0004-missing-cu-and-missing-beacon-semantics.md),
  [ADR-0005](0005-class-interval-boundary-semantics.md), and
  [ADR-0006](0006-output-and-metric-versioning.md)

## Context and current behavior

The corrected selected-observation artifact retains at most one station-count
value per BSSID/class-session/bin. ADR-0004 already fixes the station-count
entity grains: BSSID/bin values may be summed at radio/bin,
SSID/classroom/bin, and band/classroom/bin grain without applying the CU
preferred-BSSID rule. It did not decide how to handle transiently absent
BSSIDs, incomplete totals, or class-level enrollment comparisons.

The Original workflow searched nearby rows and duplicated a calculated
`total_scount` value onto beacon rows. That output could be fast to consume,
but its row grain did not match the duplicated total and its missing/staleness
behavior was not explicit. The corrected workflow must retain the useful
station-count purpose without restoring that representation.

Short capture-delivery gaps occur in retained evidence, including multi-radio
AP-555 data. Station count normally changes more slowly than beacon delivery.
For this purpose, briefly extending the last known value is preferable to
publishing a known-low sum.

## Decision drivers

- Avoid known-low totals during short beacon-delivery gaps.
- Keep measured zero distinct from missing or unavailable station count.
- Preserve BSSID-level lineage and avoid aggregate values on misleading row
  grains.
- Include every sampled SSID, AP, radio, and band represented in the class
  data while making that scope clear to report readers.
- Keep processing bounded and small relative to capture extraction.
- Preserve the legacy peak-versus-enrollment question with explicit corrected
  semantics.

## Evidence and fixture limits

- Retained AP-555 evidence shows transient delivery gaps while station count is
  expected to change more slowly than beacon reception.
- Selected observations already retain nullable station count independently of
  CU source preference.
- The current corrected semantic fixture proves station values at BSSID/bin
  grain but does not yet exercise 30-second extension or complete-only
  classroom totals.
- No evidence establishes that a value older than 30 seconds remains accurate;
  the accepted limit is an owner-selected reporting rule, not a measured
  physical guarantee.

## Options considered

- **Fresh-only totals:** rejected because brief loss would create systematic
  undercounts or excessive unavailable bins.
- **Unlimited carry-forward:** rejected because old values could outlive a real
  station-count change.
- **Thirty-second forward and leading backward extension:** accepted as the
  practical balance for transient loss.
- **Publish partial sums:** rejected for the first corrected version because a
  known-low number is more misleading than an unavailable total.
- **Duplicate rollups onto every selected BSSID row:** rejected because it
  obscures grain and increases storage.
- **Add a sixth authoritative artifact immediately:** deferred until the
  bounded derived view is measured.

## Decision

### Scope and session-active membership

Station-count reports cover only the SSIDs, APs, BSSIDs, radios, and bands that
are both in configured project scope and represented in the analyzed data.
They are not an inventory of unsampled infrastructure or unobserved SSIDs.
Methodology text must state this boundary explicitly.

A BSSID is a **session-active member** when it has at least one selected beacon
observation in that physical class session. A project BSSID observed in other
sessions but never observed in the current session is treated as not beaconing
in that session and is excluded from that session's expected membership. It
does not make the session's station totals unavailable.

Once a BSSID is session-active, it is an expected member for station-count
rollups throughout that session. A selected beacon with a null or invalid
station count establishes activity but does not supply a valid station-count
value. In the durable selected schema, a valid value is a non-null validated
`qbss_station_count`; malformed upstream input is rejected or represented as
null before this calculation. A measured zero is valid and remains distinct
from missing.

### Thirty-second extension

Station-count supplementation is confined to the same BSSID and physical
class session:

1. Use the current bin's valid selected station count when present.
2. Otherwise, carry the previous valid value forward when its source bin start
   is no more than 30 seconds before the target bin start.
3. For leading missing bins before the BSSID's first valid station count only,
   carry that first later valid value backward when its source bin start is no
   more than 30 seconds after the target bin start.
4. Do not use a later value to fill an interior or trailing gap.
5. When no permitted value exists, the BSSID station count is unavailable.
6. Never extend a value across a physical class-session boundary.

The 30-second limit is exact and inclusive. A capture gap has no special
semantic category; it is missing data governed by the same rule.

Each derived BSSID/bin value needs only its nullable station count and whether
it is fresh, stale-assisted, or unavailable. The output does not need to store
the source bin or age. Those can be recovered from the selected observations
and deterministic extension rule if deeper review is required.

### Rollups and completeness

Every rollup sums supplemented BSSID/bin values directly. A rollup must never
sum another rollup, because the radio, SSID, band, and classroom populations
overlap.

The accepted rollups are:

- **radio/bin:** all session-active member BSSIDs assigned to the radio;
- **SSID/classroom/bin:** all session-active classroom BSSIDs advertising the
  SSID, across included APs, radios, and bands;
- **band/classroom/bin:** all session-active classroom BSSIDs whose radios are
  assigned to the band, across included APs and SSIDs; and
- **classroom/bin:** every session-active BSSID across all included APs,
  radios, bands, and SSIDs in the classroom.

The first corrected version does not publish partial numeric totals. A rollup
is numeric only when every expected BSSID in that rollup has a fresh or
permitted stale-assisted value. If any expected value remains unavailable,
the rollup value is null and the rollup is labeled unavailable. A missing
BSSID invalidates only rollups containing that BSSID; unrelated SSID or band
rollups can remain available.

An available rollup has a `stale_assisted` flag that is true when one or more
of its BSSID values were extended. No partial sum, stale age, or source-bin
column is required.

### Placement and bounded processing

The first implementation will use a bounded derived station-bin view rather
than duplicate aggregate totals onto selected BSSID rows or add a sixth
authoritative artifact. `selected_observations/` remains the authoritative
source.

The view is processed one physical session at a time and may be written
incrementally as a manifest-registered, run-local derived Parquet report. Its
logical grain is:

```text
(class_session_id, classroom, bin_number, rollup_type, rollup_id)
```

The minimal values are bin identity/time, nullable station count,
availability, and `stale_assisted`. `rollup_type` distinguishes radio, SSID,
band, and classroom rows. The implementation must measure runtime and storage
before this view is promoted to a durable logical artifact. It must not load a
complete project-wide BSSID/bin product into memory merely to generate the
report.

### Class-session and enrollment statistic

The class-session station statistic remains the maximum available
classroom/bin total. Fresh and stale-assisted bins both participate.

Enrollment is the combined enrollment of all sections sharing the physical
class session. When enrollment is zero or missing, the ratio is null and that
session is excluded from aggregate enrollment-ratio calculations. The
human-readable label is **peak client count per enrolled**. Reports retain the
underlying peak client count and combined enrollment.

If the same maximum occurs in both fresh-only and stale-assisted bins, a
fresh-only occurrence is preferred when labeling whether the selected peak
used stale assistance. Per-SSID and per-band station totals and peaks are
reported separately; the enrollment numerator is the all-BSSID
classroom/bin peak.

## Analysis-semantic impact

Intentional change.

The corrected calculation explicitly extends station counts for up to 30
seconds, rejects incomplete totals instead of publishing known-low partial
sums, uses session-active membership, and computes a classroom-wide peak from
all included BSSIDs. It is not numerically required to match the Original
nearby-row lookup or duplicated `total_scount` column.

## Data, provenance, output, and compatibility impact

- Selected BSSID/bin station counts and their source lineage remain unchanged.
- Derived rollups carry availability and stale-assistance state without
  rewriting selected observations.
- Reports must state that totals cover only sampled/configured entities that
  are represented in the analyzed session data.
- Existing five-artifact runs remain valid; station reports are additive
  derived outputs.
- Metric/schema identifiers must distinguish the implemented station-count
  semantics from legacy station outputs and from corrected CU metrics.

## Migration and rollback

Existing runs require no migration. A future station report can be regenerated
from selected observations plus the run's identity and class-session evidence.
Before Alpha 0.2 publication, rollback consists of omitting the derived station
package; it must not alter the already published five authoritative artifacts
or corrected CU reports.

## Required tests and acceptance evidence

- Fresh values, measured zero, null values, and invalid values remain distinct.
- Forward extension succeeds at exactly 30 seconds and fails beyond 30
  seconds.
- Backward extension applies only to leading bins and obeys the same boundary.
- Extension never crosses a physical class-session boundary.
- A BSSID absent for the entire session is excluded from session membership.
- A session-active BSSID missing beyond 30 seconds makes only its containing
  rollups unavailable.
- Radio, SSID, band, and classroom totals sum base BSSID values without double
  counting through other rollups.
- No partial numeric total is emitted.
- Stale-assisted bins can supply the class peak and enrollment comparison.
- Simultaneous-section enrollment is combined; zero/missing enrollment yields
  a null ratio and exclusion from aggregate ratio calculations.
- Processing remains bounded by physical session, and complete-run runtime and
  storage are measured on representative project data.

## Consequences and risks

- The 30-second extension intentionally trades temporal freshness for avoiding
  transient undercounts.
- A real station-count change can be delayed in the derived series for up to
  30 seconds during missing observations.
- A BSSID that beacons only briefly becomes an expected session member; gaps
  beyond the extension limit can therefore make affected rollups unavailable.
- Excluding a BSSID never observed in the session treats it as not beaconing,
  not as an unknown member with an assumed count.
- The maximum remains sensitive to genuine or erroneous peaks; that limitation
  is accepted and should be stated in methodology text.

## Unresolved follow-up questions

- Plot selections and broader aggregate station summaries remain Alpha 0.2
  report-design work.
- Station-count relationships with CU or admission capacity require separate
  population, pairing, and presentation review.

## Implementation addendum (2026-10-03)

The supported capture workflow implements this decision as
`reports/station_count/station_bin_rollups.parquet`, with
`station_class_report.csv`, `station_aggregate_report.csv`, and
`methodology.txt` in the same directory. The manifest registers these as
derived exports. Processing and temporary storage remain bounded to one
physical session; the manifest records total rollup rows and the largest
session part.

Owner review accepted the Original-compatible enrollment-weighted aggregate
definition. For the overall population and separately for each SSID and band,
the reported average is the sum of eligible class-session peak client counts
divided by the sum of their corresponding combined enrollment. A session is
eligible only when enrollment is positive and its population peak is
available; stale-assisted peaks remain eligible. The report retains both sums,
the eligible-session count, and the stale-assisted peak-session count. It does
not use the equal-weighted mean of per-session ratios.

No station plot, percentile, station/CU comparison, or admission-capacity
calculation was added.

## Later reporting labels and scope

The paragraph above describes that dated station-rollup implementation slice.
Station distributions, CU relationships and admission capacity were subsequently
accepted under ADR-0012/0013. The current web enrollment label is **Peak stations
per enrolled**; the original field/file names and the accepted enrollment-weighted
ratio of sums remain unchanged. See
[current reporting](../reporting_and_publication.md) for presentation changes.
