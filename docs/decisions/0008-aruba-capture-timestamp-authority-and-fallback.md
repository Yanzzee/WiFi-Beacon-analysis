# ADR-0008: Observation-timestamp authority and fallback

> The TSF-anchor relationship below is superseded by
> [ADR-0019](0019-observation-time-and-native-tsf-diagnostics.md). Observation-time
> precedence remains; observation time now also controls ordering, bins and
> capture slots, while native TSF is diagnostic and duplicate evidence only.

- Status: Accepted; simplified by owner revision
- Date proposed: 2026-09-01
- Initial acceptance: 2026-09-02
- Simplified decision: 2026-09-02
- TSF-derived binning revision: 2026-09-09
- Owners/reviewers: Project owner, also serving as analysis-correctness
  reviewer
- Roadmap items:
  [PR-0021, PR-0022, and PR-0024](../post_refactor_roadmap.md)
- Related decisions:
  [ADR-0001](0001-canonical-internal-timestamp-representation.md),
  [ADR-0005](0005-class-interval-boundary-semantics.md), and
  [ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md)
- Evidence:
  [Aruba timestamp source contract](../aruba_capture_timestamp_source_contract.md)
  and
  [retained-capture characterization](../adr0009_retained_capture_characterization.md)

> Current implementation status (2026-10-02): this precedence and fallback
> contract is active in `beacon_analysis.py run`. Migration-era statements
> below are retained only as decision rationale.

## Plain-language purpose

This decision says which absolute timestamp admits a beacon into a scheduled
class and anchors reliable per-BSSID TSF values to that class's absolute time.

For supported Aruba ERM captures, use the valid raw `@aruba_erm.time` value
because it is stamped closest to the capture source. If it is unavailable or
unusable, use valid `frame.time_epoch`. For captures without Aruba ERM time,
use valid `frame.time_epoch`.

The preferred observation timestamp determines whether a beacon is a candidate
for the class. When a BSSID/session has reliable TSF progression, ADR-0009
derives one best-effort absolute TSF origin from the earliest valid
`observation_time - TSF` candidate and uses TSF-derived time for bin placement.
Otherwise, observation time also places the beacon in its bin.

The timestamps are assumed accurate enough for their stated purpose within a
project. The processor does not analyze clock drift, compare clock domains,
fit rate corrections, or reject an entire class because of clock behavior.

## Decision

### Observation-time precedence

For each beacon row, select immutable absolute observation time `O` in this
order:

1. valid normalized raw `@aruba_erm.time`; otherwise
2. valid normalized `frame.time_epoch`.

The accepted source parser and normalization rules still apply. A row with no
valid accepted absolute timestamp cannot be placed in a scheduled class and
must produce an explicit source-timestamp diagnostic.

`O` controls:

- half-open scheduled-class membership;
- eligibility for the per-BSSID/session TSF-origin calculation;
- class-start bin membership and deterministic ordering when TSF is unusable;
  and
- the observation-time provenance and sanity comparison retained when
  reliable TSF-derived time controls bin placement.

The selected source name and raw/normalized values remain provenance. Adding
another vendor-specific timestamp ahead of frame time is a new source-policy
feature and requires high-level owner confirmation before a detailed design is
introduced.

### Explicit non-goals

This decision does not introduce:

- an observation-clock-domain identity;
- clock-discontinuity or drift detection;
- one-minute change screens or persistence thresholds;
- cross-clock normalization or rate fitting;
- diagnostic-clock corroboration;
- whole-class fallback caused solely by clock behavior; or
- automatic pooling of clocks or timestamps across BSSIDs.

`frame.time_epoch` is a normal fallback timestamp, not a tool for repairing
Aruba time. No hidden clock correction is performed.

### Source precedence and ties

When overlapping capture sources expose the same beacon, apply the accepted
source precedence before comparing observation time. Within the chosen source
precedence:

1. choose the earliest valid `O`; then
2. use durable capture/packet lineage as the deterministic tie-breaker.

Exact duplicate views with conflicting decoded payloads produce a diagnostic;
they do not cause an observation-clock model to be inferred.

## Relationship to TSF

`wlan.fixed.timestamp` is not a calendar timestamp, so it cannot admit a row
to a scheduled class by itself. Revised
[ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md) evaluates it within
one BSSID/class session. When it is reliable, the earliest valid `O-F`
candidate supplies one best-effort origin, TSF-derived time controls bin
placement and sequence order, and equal TSF identifies duplicate views.

Origin candidates honor the same timestamp-source precedence: valid Aruba
time candidates form the origin pool when available for the usable stream;
frame-time candidates are used only when Aruba time is unavailable.

This is a per-BSSID/session offset calculation only. It does not pool BSSIDs,
infer a shared AP boot time, fit drift or rate, split clocks into segments, or
repair the source timestamp. When TSF is unusable, the whole BSSID/session uses
`O` for bin placement.

## Validation obligations

Required focused tests are intentionally small:

- valid Aruba time is selected ahead of frame time;
- valid frame time is used when Aruba time is missing or invalid;
- a row with neither valid source receives an explicit diagnostic and is not
  silently assigned to a class;
- equivalent normalized instants produce the same class/bin result;
- source precedence is applied before timestamp comparison for overlapping
  duplicate views;
- deterministic lineage resolves remaining ties;
- an observation timestamp outside the half-open class is excluded before any
  TSF-origin calculation;
- reliable per-BSSID/session TSF can place an admitted beacon in a different
  class-start bin than `O`;
- the earliest valid `O-F` candidate anchors the BSSID/session;
- a gross timestamp/TSF mismatch, including an obviously wrong hour or day,
  cannot silently anchor TSF-derived bins;
- unusable TSF causes whole-BSSID/session observation-time fallback; and
- clock offset or drift does not create a new segment, correction, or class-
  level rejection.

Existing legacy behavior and outputs remain unchanged until the separately
versioned interval implementation is approved and activated under ADR-0006 and
PR-0024.

## Consequences

### Benefits

- The timestamp rule matches the actual precision requirement.
- Aruba time remains preferred while non-Aruba captures can use frame time.
- Analysis remains unattended when TSF is absent or unusable.
- Clock-model state, thresholds, transformations, and failure modes are
  eliminated.
- The result is easier to implement, test, explain, and audit.

### Trade-offs

- The processor does not attempt to correct capture-clock errors.
- Capture-source latency can affect an observation-time fallback near a bin
  boundary. Reliable TSF-derived time reduces that effect without changing
  scheduled class admission.
- The origin is a best-effort lower-envelope estimate, not a claim that the AP
  literally booted at that instant.

## Superseded proposal history

The initially accepted ADR-0008 proposal included evidence-derived clock
domains, discontinuity segmentation, optional normalization, and whole-class
fallback. The owner subsequently removed that machinery. On 2026-09-09, the
owner accepted the much smaller per-BSSID/session lower-envelope offset needed
to let reliable TSF improve bin placement. Clock domains, drift/rate fitting,
change screens, cross-BSSID timing pools, and whole-class fallback remain
superseded. The retained replay remains useful capture evidence, not a pending
numeric clock-profile gate.

## Complexity guardrail

Keep timing behavior as simple as the stated precision and accuracy
requirements permit. Before drafting or implementing additional clock models,
cross-BSSID timing features, correction algorithms, or numeric profiles:

1. describe the practical problem and proposed benefit at a high level;
2. confirm with the project owner that the additional behavior is needed; and
3. only then prepare detailed decision or implementation material.
