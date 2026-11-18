# ADR-0005: Class interval, binning, and observation-selection semantics

- Status: Accepted
- Decision: Accepted within the interval-geometry and per-BSSID observation-
  selection scope below
- Date proposed: 2026-08-12
- Date decided: 2026-08-26
- Owners/reviewers: Project owner, also serving as analysis-correctness reviewer
- Roadmap items:
  [PR-0021, PR-0022, and PR-0024](../post_refactor_roadmap.md)
- Related decisions:
  [ADR-0001](0001-canonical-internal-timestamp-representation.md),
  [ADR-0003](0003-ap-and-physical-radio-identity-contract.md),
  [ADR-0004](README.md#decision-register),
  [ADR-0006](0006-output-and-metric-versioning.md), and
  [ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md)
- Supersedes: None
- Superseded by:
  [ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md) in part for the
  corrected metric

> Current implementation status (2026-10-02): fixed class-start bins,
> terminal partial bins, TSF-derived placement when reliable, and first
> observed BSSID/bin selection are active in `beacon_analysis.py run`. Text
> below describing a future cutover or rollback is historical rationale.

## Scope and decision state

This ADR accepts the time geometry and observation-selection contract needed
by corrected class-interval metrics:

- class start/end membership;
- bin boundaries, anchors, width, and terminal partial intervals;
- the relationship among raw beacon timing, useful captured metadata timing,
  deduplication spacing, and analysis-bin width;
- deterministic selection of the first observation per BSSID in an
  analysis bin; and
- separation of observation selection from later coverage inference.

The accepted policy is implemented in the versioned corrected workflow. Its
fixture and golden assertions pin the active interval behavior.

This ADR deliberately does not decide:

- how a received beacon with missing CU or an expected interval with no beacon
  affects high/non-high state, duration, coverage, or streak bounds; ADR-0004
  owns that decision;
- physical-radio identity or how multiple BSSIDs collapse into one radio/bin
  state; ADR-0003 owns that decision;
- how selected BSSID observations are combined into a physical-radio,
  channel, or classroom high-CU state;
- station-count alignment or de-duplication;
- CU thresholds; or
- the final metric-version identifier, public output placement, compatibility
  duration, or default-mode transition; those remain ADR-0006 follow-up and
  PR-0024/PR-0055/PR-0078 work.

## Historical context before the accepted refactor

The current runtime is described in
[current_pipeline.md](../current_pipeline.md#timing-binning-and-selection).
The pre-refactor pipeline described in this context first filtered one AP to an
inclusive class interval and then thinned observations independently by
`wlan.ta`, which the current Aruba path treats as BSSID.

For each BSSID, the former `beacon_filtering.filter_by_time` sorted by timestamp, retained
the first observation, and then greedily retains the first later observation
whose timestamp is at least `0.95 * BEACON_RATE` after the last retained
observation. With `BEACON_RATE=1.024 s`, the eligibility gate is:

```text
0.95 * 1.024 s = 0.9728 s
```

The gate opens `51.2 ms` before the nominal next captured metadata-bearing
beacon at `1.024 s`. That margin is one half of the standard `102.4 ms` raw
beacon interval. Because beacons are not typically expected early, the first
normally expected captured beacon can occur up to `51.2 ms` after the gate and
still be the first eligible selection. A beacon delayed further is also
eligible because the rule imposes no maximum late-arrival bound. This is not
expected-cadence slot matching.

This produces approximately one retained observation per ten raw beacon
intervals when all beacons are captured. It also retains consecutive
observations from sources that already capture approximately one in ten,
unless an observation arrives before the `0.9728 s` eligibility gate. Mixed
and duplicate observations can still occur because capture sources overlap.

The later class-session calculation places all retained location-wide rows
into class-start-anchored bins and marks a bin high when any retained row is
high. It does not currently select one representative row per BSSID/bin and
can combine APs, BSSIDs, and radios in one bin state.

Focused characterization tests pin the following legacy behavior:

| Concern | Current unversioned behavior |
|---|---|
| Class membership | Beacon rows at both `start_time` and `end_time` are included: `[start, end]`. |
| Deduplication scope | Within one AP/class slice, rows are grouped by `wlan.ta`; SSID, frequency, source capture, and physical-radio identity are not part of the key. |
| Deduplication selection | Keep the earliest row, then the earliest row at least `0.95 * BEACON_RATE` after the last retained row. This is a rolling gate, not a fixed bin. |
| Bin anchor | `pd.date_range(start, end, freq=BEACON_RATE)` anchors class-analysis bins at the class start. |
| Bin closure | Evaluated bins are left-closed and right-open: `[t_start, t_end)`. An observation exactly on an internal edge belongs to the later bin. |
| Per-bin selection | No representative BSSID row is selected; a bin is high when any retained location-wide row exceeds the threshold. |
| End-point mismatch | A row exactly at class end is included in non-binned class statistics but belongs to no evaluated bin. |
| Terminal partial interval | A remainder shorter than `BEACON_RATE` is omitted from duration/streak metrics, although its rows still affect other class statistics. |
| Session shorter than one bin | No duration/streak bin is evaluated. |
| Timing concepts | `BEACON_RATE` supplies the effective captured metadata interval and analysis-bin width; its `0.95` multiple supplies deduplication minimum spacing. |
| Analysis-bin edge tolerance | Exact timestamp membership determines the bin. No snapping or tolerance changes bin membership. |
| High duration | Each nonempty high bin adds one full `BEACON_RATE`, independent of observation count or placement inside the bin. |
| Empty interval | Legacy comparison adds no duration and does not reset its streak; ADR-0004 separately defines the actual observed streak and coverage behavior. |

Exact-timestamp duplicates with the same BSSID are expected ordinarily to be
duplicate observations of the same beacon, so their measured values normally
agree. The current implementation nevertheless lacks an explicit stable
tie-breaker, which matters for deterministic provenance and for the case where
apparently duplicate rows conflict.

## Decision drivers

1. Every in-scope instant must have deterministic membership in at most one
   class and one analysis bin.
2. Selection timing must be consistent even when overlapping capture sources
   produce mixed rates or duplicate BSSID observations.
3. The selected value in a bin must not depend on a later, possibly still
   unrefreshed duplicate merely because it is more extreme.
4. Terminal partial time must be visible rather than silently disappearing or
   being counted as a full interval.
5. Raw canonical instants must not be shifted or repaired to fit an expected
   cadence.
6. Raw beacon interval, useful captured metadata cadence, deduplication
   spacing, analysis-bin width, and any future coverage tolerance must have
   distinct meanings even when values are derived from one another.
7. Results must retain enough configuration, boundary, selection, and source
   evidence to be reproduced.
8. Current metrics must remain available for comparison until the corrected
   metric passes its versioned acceptance gates.

## Options considered

### Class membership

| Option | Meaning | Benefits | Risks |
|---|---|---|---|
| B-A — Closed legacy | `[start, end]` | Preserves current row selection | Exact-end membership disagrees with right-open bins; adjacent/overlapping sessions can share a boundary row |
| B-B — Half-open | `[start, end)` | Unique membership and the same convention as bin partitioning | Intentionally excludes exact-end rows; typical ten-minute class breaks make the practical effect small |
| B-C — Open or mode-specific | Exclude both endpoints or vary by report | Flexible | Harder to explain and reproduce; no evidence requires it |

**Decision:** B-B, half-open class intervals.

### Bin anchor and closure

| Option | Meaning | Benefits | Risks |
|---|---|---|---|
| A-A — Class-start anchor | Partition each class as `[start + kW, start + (k+1)W)` | Directly represents elapsed class time; deterministic; no leading partial bin | Bin phase differs between classes with different starts |
| A-B — Global wall-clock/epoch anchor | Use one shared absolute grid | Cross-session alignment | Produces partial bins at class boundaries and couples results to an arbitrary epoch |
| A-C — Observed-cadence anchor | Infer phase from received observations | May match one transmitter's cadence | Sparse/missing observations can change the anchor and different BSSIDs can acquire different grids |

Here `start` is the scheduled class start, `W` is the analysis-bin width, and
`k` is a non-negative integer bin number. All options use left-closed/right-
open bins so an instant on an internal edge belongs to the later bin.

**Decision:** A-A for class-duration metrics.

### Terminal partial interval

| Option | Meaning | Benefits | Risks |
|---|---|---|---|
| P-A — Drop legacy remainder | Do not represent time shorter than `W` at class end | Preserves current metrics | Silently loses class time and its coverage state |
| P-B — Count as a full bin | Apply the ordinary full-bin duration | Simple | Can report more high/observed time than the class contains |
| P-C — Represent actual width | Materialize `[last_edge, end)` with `actual_duration < W` and `width_status=partial` | Preserves all class time without overcounting | Requires versioned output/metric changes and explicit missing-data rules |

**Decision:** P-C. A full-width interval uses `width_status=full`; the terminal
short interval uses `width_status=partial`. Width status describes time
geometry only and must remain separate from whether the interval contains
observations or has sufficient coverage.

### Timing concepts and observation selection

| Option | Meaning | Benefits | Risks |
|---|---|---|---|
| C-A — One overloaded value and any-row bin state | Continue using one implicit rate meaning and let any retained row determine a high bin | Preserves current behavior | Hides selection meaning; mixed/duplicate rates can allow a later row to control the result |
| C-B — Fixed cadence-slot matching | Match observations to ideal expected slots with a tolerance | Can support coverage estimates | Introduces a model of refresh phase that current evidence cannot prove; is unnecessary for selecting the earliest useful row |
| C-C — Explicit timing meanings plus first-observation selection | Record timing concepts separately and select the earliest observation per BSSID/bin without a cross-bin rolling gate | Deterministic; previous-bin state cannot suppress a candidate | Requires tie rules, new metric versioning, and later physical-radio aggregation |

**Decision:** C-C.

For the current supported Aruba ERM mode, the accepted initial values and
relationships are:

| Concept | Accepted value or rule |
|---|---|
| Nominal raw beacon interval | `0.1024 s` |
| Useful captured metadata cadence | `1.024 s`, corresponding to ten nominal raw beacon intervals |
| Legacy deduplication eligibility gate/minimum spacing | `0.9728 s`, derived as `0.95 * 1.024 s`; characterized for the historical workflow but not applied to corrected bin selection |
| Analysis-bin width | `1.024 s` |
| Analysis-bin anchor | Scheduled class start |
| Analysis-bin edge tolerance | None; exact half-open timestamp membership |
| Representative observation | Earliest observation by canonical timestamp for each BSSID in each analysis bin after duplicate-view reconciliation |

These concepts must be named and recorded separately, but they do not all
need to be independent user-facing settings. A validated mode may derive one
from another, provided the effective values and derivation are captured.

The corrected metric does not apply the legacy rolling gate. Fixed bins already
limit the durable selected-observation result to one row per BSSID/bin. A
cross-bin gate can reject the first observation in a bin merely because the
previous bin's selected observation was late, making selection depend on prior
bin state. After duplicate-view reconciliation, select the earliest observation
within each BSSID/bin independently.

For equal canonical timestamps, the canonical path must use a stable source-
record ordering key when available. If tied rows have identical relevant
payload and legacy data lacks source identity, deterministic input row order
may be used while reporting that provenance as unavailable. If tied rows
conflict and no stable source evidence resolves them, the corrected path must
report an ambiguity rather than silently select a value.

Expected-cadence slot matching is not an observation-selection rule. A future
coverage contract may use separately justified timing tolerance to decide
whether an expected observation was received, but it must not move canonical
timestamps, change class/bin membership, or replace the first-observation
selection rule. ADR-0004 and DES-004 own that coverage decision.

## Accepted decision

The project owner, also serving as analysis-correctness reviewer, accepts this
contract for a new corrected metric version:

1. Class intervals are half-open `[start, end)`.
2. Class-analysis bins are left-closed/right-open, class-start anchored, and
   contiguous.
3. Every class interval is fully represented. A terminal partial interval is
   recorded with its actual duration and `width_status=partial` rather than
   counted as a full bin or silently omitted.
4. Canonical timestamps are immutable. There is no snapping at class or bin
   boundaries.
5. Raw beacon interval, useful captured metadata cadence, analysis-bin width,
   the historical deduplication spacing, and any future coverage tolerance are
   distinct concepts. The accepted initial Aruba values are `0.1024 s`,
   `1.024 s`, and `1.024 s` for the first three respectively. The historical
   gate is `0.9728 s` but is not applied to corrected bin selection; no
   analysis-bin edge tolerance is used.
6. Do not apply a rolling minimum-spacing gate in the corrected metric. Select
   the earliest observation per BSSID in each analysis bin after duplicate-
   view reconciliation. Resolve exact ties
   deterministically; report unresolved conflicting ties as ambiguous.
   Expected-slot tolerance, if later accepted for coverage, cannot select a
   different value or change interval membership.
7. The current closed-boundary, remainder-dropping, any-row behavior remains
   an explicitly identified legacy comparison mode during migration.

Selection of missing-bin state and streak bounds remains blocked on ADR-0004;
aggregation from BSSID to radio-group/classroom results also requires Phase 5
implementation of accepted ADR-0003.

## Analysis-semantic impact

intentional change

Accepting this ADR changes documentation and architectural authority only; it
does not change runtime behavior. Implementing it will intentionally change
exact-end row inclusion, terminal-partial representation, representative-row
selection, configuration meaning, and potentially interval-derived metrics.
It therefore requires a new metric version and must not overwrite the legacy
golden.

## Data, provenance, output, and compatibility impact

This decision changes no current Parquet, CSV, plot, fixture, or schema. A
future implementation must make the following available to every derived
interval result directly or through a referenced run/metric manifest:

- class start/end instants and half-open closure;
- bin anchor, width, left/right boundaries, actual duration, and width status;
- nominal raw interval, useful captured metadata cadence, analysis-bin width,
  and any separately accepted coverage tolerance;
- BSSID selection key, selected source-row reference, and tie outcome;
- selected metric version and missing-data assumption; and
- physical-radio/source identity sufficient to reproduce the result when
  those contracts exist.

Current unversioned output remains the legacy baseline. A corrected result
must use a separate namespaced metric-version identifier under ADR-0006's
independent-version rule. The exact identifier, artifact placement, filename,
and coexistence duration remain PR-0024/PR-0055/PR-0078 decisions.

## Migration and rollback

Implementation must initially support a dual run:

1. reproduce the current unversioned legacy interval behavior and existing T3
   fingerprint; and
2. produce corrected, separately versioned interval results from the same
   canonical instants and reviewed oracle fixtures.

Rollback selects legacy mode and removes no legacy fixture or golden. A
corrected metric cannot become the default until its oracle cases, integrated
golden, output placement, and review record are accepted. A changed decision
requires a superseding ADR and another metric version.

## Required tests and acceptance evidence

### Decision/characterization evidence

- Exact class start/end and just-outside observations.
- Observation immediately before versus exactly on an internal bin edge.
- Complete bins followed by a terminal partial interval.
- Session shorter than one full bin.
- Existing high/empty/high and high/non-high/high behavior.
- Existing location-wide observations from alternating radios.
- Existing exact and just-under `0.95 * BEACON_RATE` deduplication boundary.
- T2 ordinary suite. The unchanged T3 legacy fingerprint remains acceptance
  evidence but need not be rerun for this documentation-only decision update.

### Implementation gate

- Half-open adjacent classes proving unique exact-boundary membership.
- Non-integral class duration proving full time coverage and exact partial
  width, with width status independent of observation coverage.
- Exact edge, plus/minus one nanosecond, and timezone-equivalent instant cases.
- Dense nominal `0.1024 s`, already-thinned `1.024 s`, jittered, mixed-rate,
  and duplicate-source BSSID sequences.
- Cross-bin cases proving that a late observation in one bin cannot suppress
  the first observation in the next bin.
- Zero, one, and multiple rows per BSSID/bin, proving the earliest is
  selected without discarding observations from other BSSIDs.
- Equal-timestamp identical duplicates with deterministic provenance and
  equal-timestamp conflicting rows producing an explicit ambiguity when no
  stable evidence resolves them.
- Property tests proving bins are ordered, non-overlapping, gap-free across the
  class interval, and sum exactly to class duration.
- Proof that coverage-slot logic, if later implemented, cannot alter selected
  rows, timestamps, or interval membership.
- ADR-0004 truth tables for observed high, observed non-high, received/missing
  CU, and expected/no beacon.
- Phase 5 implementation cases under accepted ADR-0003 before any corrected
  per-radio streak is published.
- A separately reviewed corrected-metric golden; the legacy fingerprint must
  remain unchanged.

## Consequences and risks

### Benefits

- Adjacent or overlapping sessions have unambiguous exact-boundary ownership,
  while the usual ten-minute class break makes the practical row-count impact
  very small.
- Every instant and every portion of class time has an explicit interval
  representation.
- Partial duration cannot silently disappear or be over-counted as a full bin.
- The earliest BSSID observation supplies a consistent representative
  instead of allowing a later duplicate or extreme value to control the bin.
- Raw interval, captured cadence, historical deduplication gate, bin width, and coverage
  tolerance cannot be conflated under one undocumented meaning.
- Missing-data and identity decisions can operate over a stable interval model
  without being smuggled into timestamp normalization or observation
  selection.

### Costs and risks

- Exact-end observations, terminal partial intervals, and first-observation
  selection can change counts and duration-derived metrics relative to the
  legacy baseline.
- Dual-mode migration and versioned outputs add temporary complexity.
- Legacy rows lack durable capture/frame identity, limiting tie provenance.
- First-observation selection is deterministic but cannot prove when vendor
  metadata actually refreshed.
- The legacy `0.9728 s` gate remains documented only to explain historical
  behavior and must not be reused as a corrected coverage tolerance.
- Per-radio corrected metrics remain blocked until the accepted identity
  contract is implemented and missing-data semantics are accepted.

## Deferred follow-up work

1. Implement accepted ADR-0003 project-scoped AP/BSSID/radio grouping and
   project-static membership through the Phase 5 gates.
2. Define missing-CU, missing-beacon, coverage, and streak-bound semantics in
   ADR-0004 without changing this ADR's selected observation.
3. Define the corrected metric identifier, dual-run artifacts, and default/
   rollback policy in PR-0024 and the broader ADR-0006 follow-up.
4. Implement only after those dependencies and the required oracle fixtures
   make PR-0021/PR-0022 Ready.

## Addendum — accepted identity scope and simplified TSF evidence

On 2026-08-26,
[ADR-0003](0003-ap-and-physical-radio-identity-contract.md) was Accepted with
project-scoped configured AP names, BSSID-primary stream grouping, whole-project
operational radio grouping, project-static BSSID ownership, and the accepted
BSSID-to-radio-to-AP-to-classroom hierarchy. The earlier time-scoped wording in
deferred item 1 is no longer ADR-0003's scope. The selected BSSID/bin record
from this ADR feeds that hierarchy; ADR-0003 does not change its timing rule.

On 2026-09-02, the owner simplified
[ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md). On 2026-09-09, the
owner refined it to use a narrow per-BSSID/session `min(O-F)` anchor and
TSF-derived time for bin placement when TSF is reliable. This ADR remains
authoritative for the corrected metric's half-open class and bin geometry,
class-start anchor, terminal partial interval, actual clipped duration, and
separation of interval width from coverage state. ADR-0009 determines whether
reliable TSF-derived time or fallback observation time places a BSSID's rows
onto that fixed grid.

Observation time still admits candidates to a scheduled class. Missing,
frozen, reversing, insufficient, or grossly inconsistent TSF causes consistent
observation-time fallback for that complete BSSID/session. Compatible-BSSID
timing pools, clock profiles, drift analysis, segmentation, and class-level
fallback remain superseded. Runtime behavior remains unchanged until the
separate implementation, metric-version, migration, golden, and rollback gates
pass.

## Addendum — fixed-bin selection replaces the rolling gate

On 2026-09-09, the owner clarified that the original `0.9728 s` gate existed
to keep selections near the useful ten-beacon cadence before fixed
representative bins were defined. For the corrected metric, class-start bins
now provide that cadence. The owner accepted removing the rolling gate because
combining both rules can reject a bin's first observation based on the prior
bin's selection.

The accepted corrected rule is therefore one selected observation per
BSSID/bin: reconcile duplicate views, then keep the earliest observation in
that bin with deterministic source/lineage tie-breaking. The `0.9728 s` rule
remains only as historical-current behavior and legacy characterization. This
documentation decision changes no runtime code or output.
