# ADR-0009: Per-BSSID TSF-derived beacon time and bin semantics

- Status: Accepted; revised by owner
- Date proposed: 2026-08-27
- Initial acceptance: 2026-08-27
- Simplified decision: 2026-09-02
- TSF-derived binning revision: 2026-09-09
- Owners/reviewers: Project owner, also serving as analysis-correctness
  reviewer
- Roadmap items:
  [PR-0021 and PR-0022](../post_refactor_roadmap.md)
- Related decisions:
  [ADR-0003](0003-ap-and-physical-radio-identity-contract.md),
  [ADR-0005](0005-class-interval-boundary-semantics.md), and
  [ADR-0008](0008-aruba-capture-timestamp-authority-and-fallback.md)
- Evidence:
  [retained-capture characterization](../adr0009_retained_capture_characterization.md)

> Current implementation status (2026-10-02): reliable per-BSSID/session TSF
> anchoring, ordering, duplicate recognition, and bin placement are active in
> `beacon_analysis.py run`, with whole-stream observation-time fallback.

## Plain-language purpose

Captured beacons can arrive late, out of order, more than once, or not at all.
The beacon Timestamp/TSF value in `wlan.fixed.timestamp` is stamped at the
transmitter and can therefore provide more consistent ordering and bin
placement than a later capture timestamp when it progresses normally.

The preferred absolute observation timestamp is still necessary: TSF has no
calendar date. Observation time decides whether a beacon is a candidate for a
scheduled class and provides the absolute anchor for that BSSID's TSF. Once
anchored, reliable TSF-derived time places and orders the BSSID's beacons in
the class bins. If TSF is unusable, the complete BSSID/session falls back to
observation time.

## Terms

| Term | Meaning |
|---|---|
| `O` | Preferred absolute observation timestamp selected by ADR-0008: valid Aruba ERM time, otherwise valid frame time. |
| `F` | Beacon Timestamp/TSF from `wlan.fixed.timestamp`, converted from microseconds to the same time unit as `O`. It is relative to the transmitting BSS and is not a calendar timestamp. |
| `B` | Best-effort absolute TSF origin for one BSSID/session: the earliest valid `O-F` candidate in the highest-precedence available timestamp-source cohort after class admission and duplicate reconciliation. |
| `T` | TSF-derived absolute beacon time for usable TSF: `B+F`. |
| `I` | Advertised beacon interval from `wlan.fixed.beacon`, in TU. |
| `R` | Advertised interval in time: `I * 1,024 microseconds`. |
| `W` | Class-analysis bin width. The current accepted corrected value is `1.024 s`. |

`B` is a lower-envelope, best-effort timing origin. It must not be described as
a verified AP boot time. `T` represents TSF-derived transmitter time; it does
not claim to reconstruct an ideal scheduled TBTT when channel access delayed
the actual beacon transmission.

## Decision

### 1. Observation time admits candidates to the class

Class membership starts with the half-open observation-time test:

```text
class_start <= O < class_end
```

A row whose `O` falls outside the class is excluded for simplicity, even if a
later TSF mapping might have placed it inside. A row without a valid accepted
`O` is also excluded with an explicit diagnostic. This prevents a timestamp
with an obviously wrong day or hour from silently entering the class or
anchoring its TSF grid.

Class-start bins remain the ADR-0005 intervals:

```text
[class_start + kW, min(class_start + (k+1)W, class_end))
```

where `k` is a nonnegative bin number. The final bin may be partial.

### 2. Evaluate one complete BSSID/session together

TSF reliability, anchoring, ordering, duplicate handling, and fallback are
decided for the complete set of admitted observations from one BSSID in one
class session. Do not make independent per-row clock choices.

Do not pool TSF origins or progression across BSSIDs, radios, APs, capture
endpoints, or classes. Similar TSF values do not establish radio or AP
identity.

### 3. Reconcile duplicate views before deriving the origin

For duplicate views of one `(BSSID, TSF)` beacon:

1. apply accepted capture-source precedence;
2. within that precedence, keep the earliest valid `O`; and
3. resolve a remaining tie with durable capture/packet lineage.

A payload conflict produces a diagnostic while preserving deterministic
selection. It does not create another beacon.

Timestamp-source precedence also applies to origin candidates across the
BSSID/session. Use valid Aruba-time candidates when that source is available
for the usable stream; use frame-time candidates only when Aruba time is
unavailable. Do not let a lower-precedence capture clock win `B` merely because
its timestamp is numerically earlier.

### 4. Use a lower-envelope anchor when TSF is reliable

Within the highest-precedence available timestamp-source cohort for the usable
BSSID/session, calculate:

```text
origin_candidate_i = O_i - F_i
B = min(origin_candidate_i)
T_i = B + F_i
```

Using all valid BSSID/session candidates avoids making a delayed first beacon
the permanent offset for the class. Because received beacons are normally
late rather than early relative to transmitter time, the earliest valid
origin candidate is the least-delayed available estimate.

When TSF is usable, `T` controls:

- bin placement, even when `O` would place the beacon in an adjacent bin;
- sequence order among distinct beacons; and
- first-beacon selection within the BSSID/bin after duplicate reconciliation.

Both `O` and `T`, the selected timing method, `B`, and source lineage remain
available for audit. If an admitted row's reliable `T` falls outside the
half-open class, exclude that row with a diagnostic rather than clamping it to
an edge bin.

### 5. TSF reliability and skipped intervals

TSF is usable only when the complete admitted BSSID/session has distinct,
normally progressing values and its observation-time/TSF relationship is not
grossly inconsistent. The sanity check is intentionally broad: a wrong day or
hour, frozen value, reset, or otherwise obvious mismatch cannot anchor bins.
This decision does not introduce drift measurement or a fine-grained numeric
clock profile.

When usable:

- equal BSSID and TSF identifies duplicate views of one beacon;
- increasing TSF restores sequence order despite delayed or out-of-order
  capture arrival;
- a larger-than-normal positive TSF difference can identify skipped beacon
  intervals; and
- no missing beacon is synthesized.

The advertised interval may help interpret positive TSF gaps. Exact unusual-
gap classification thresholds remain unnecessary unless a practical need is
confirmed with the owner.

### 6. Whole-BSSID/session observation-time fallback

TSF is unusable when it is missing, nonnumeric, frozen, clearly resets or
reverses, lacks enough distinct progression to establish reliability, or is
grossly incompatible with observation time. Exact retained-capture sentinel
values `0` and `56` are known frozen examples, not the only invalid values.

When TSF is unusable:

- keep otherwise valid admitted beacons;
- use `O` for bin placement and ordering for the entire BSSID/session;
- reconcile exact source duplicates using source precedence and lineage; and
- select the earliest observed record per BSSID/bin.

Do not mix TSF-derived and observation-time bins within the same
BSSID/session, split it into clock segments, repair or rate-fit TSF, reject the
entire class, or change the fixed bin boundaries.

### 7. Complexity exclusions

This decision explicitly excludes:

- TSF-origin pooling across BSSIDs, radios, or APs;
- clock-domain or clock-segment identities;
- observation-clock drift/discontinuity analysis;
- change-screen blocks or persistence windows;
- local linear detrending or other clock fitting;
- diagnostic dual-clock corroboration;
- offset/rate correction beyond the single constant per-BSSID/session origin
  `B`; and
- whole-class fallback caused by TSF quality.

## Required behavior examples

| Situation | Result |
|---|---|
| Observation timestamp is outside the class | Exclude before TSF evaluation, even if TSF might map inside. |
| Same BSSID and TSF appears through two capture sources | Treat as one beacon; apply source precedence, earliest `O`, then lineage before calculating `B`. |
| Reliable TSF and `O` indicate adjacent bins | Use `T=B+F` for the bin and retain `O` as provenance. |
| First admitted beacon is delayed | Do not anchor only to it; use the earliest valid `O-F` from the complete BSSID/session. |
| TSF gap spans several plausible beacon intervals | Record skipped intervals; do not create rows. |
| TSF is frozen at `56` | Use observation time for the complete BSSID/session. |
| TSF clearly resets, reverses, or conflicts grossly with time | Use observation time for the complete BSSID/session; do not reject the class. |
| Two BSSIDs on the same radio have similar TSF behavior | Derive and evaluate each BSSID independently; radio aggregation remains ADR-0003 work. |

## Validation obligations

Focused fixtures and tests must cover:

- Aruba-before-frame observation-time precedence and frame fallback;
- half-open class admission by `O`, including a wrong-day/hour exclusion;
- multiple `O-F` candidates proving the earliest valid candidate anchors `B`;
- a delayed first beacon not becoming the session anchor;
- reliable TSF placing a beacon in a different bin than `O`;
- a reliable mapped time outside the class being excluded without clamping;
- same-BSSID/same-TSF duplicate views across capture sources;
- deterministic source precedence and lineage ties;
- out-of-order distinct TSF values;
- plausible skipped intervals without synthesized rows;
- missing, nonnumeric, frozen, reversing, resetting, and grossly incompatible
  TSF producing whole-BSSID/session fallback;
- identical TSF values on different BSSIDs remaining independent; and
- payload disagreement between duplicate views producing a diagnostic.

## Consequences

### Benefits

- Reliable transmitter-side timing, rather than capture arrival jitter,
  determines the BSSID's bin placement.
- The complete-session lower envelope avoids anchoring every bin to a delayed
  first beacon.
- Duplicate, ordering, and skipped-interval behavior remains deterministic.
- Frozen or otherwise unusable TSF completes analysis normally through one
  consistent BSSID/session fallback.
- The calculation needs only one constant origin per BSSID/session and no
  drift, segmentation, or cross-BSSID clock model.

### Trade-offs

- This reintroduces a small absolute mapping from relative TSF to class time.
- Observation-time class admission can omit a beacon captured just outside a
  boundary even if transmitter time would map it inside. The owner accepts
  this simplification because exact class boundaries are not analytically
  important.
- A mapped edge beacon may be excluded when `T` falls outside the class; it is
  never silently clamped.
- The origin is the least-delayed observed estimate, not a proven literal AP
  boot time or ideal TBTT phase.

## Relationship to earlier decisions

ADR-0005 remains authoritative for half-open class/bin geometry, class-start
anchors, terminal partial intervals, width status, and fixed-bin selection
without the historical rolling gate. This ADR determines whether a BSSID uses
`T` or fallback `O` as the coordinate placed into those fixed bins.

ADR-0008 remains authoritative for absolute observation-time precedence and
class admission. ADR-0003 remains authoritative for BSSID-to-radio-to-AP-to-
classroom identity and radio-level aggregation.

## Revision history

The initial proposal contained mapped TSF/TBTT windows, compatible-BSSID
timing pools, and extensive numeric clock/change profiles. The 2026-09-02
simplification removed all origin mapping and retained TSF only for order and
duplicates. On 2026-09-09, the owner confirmed that reliable TSF should also
control bins and accepted the narrow per-BSSID/session `min(O-F)` anchor in
this revision.

The shared timing pools, drift/change analysis, segmentation, repair, and
whole-class fallback remain superseded. Do not add further timing modes or
threshold machinery without first confirming the practical need with the
project owner.
