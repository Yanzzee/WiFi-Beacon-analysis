# Architecture Decision Records

This directory holds architecture decision records (ADRs) for the corrected
pipeline. An ADR records one consequential decision, the
evidence available when it was made, alternatives considered, consequences,
and validation obligations. It does not replace
[`current_pipeline.md`](../current_pipeline.md) as the current-behavior source
of truth or authorize unrelated analysis changes.

ADR-0002 is Accepted within its conceptual canonical-data scope. ADR-0006 is
Accepted within the versioning and durable-output scope. ADR-0001 is
Accepted within its timestamp-representation scope. ADR-0005 is Accepted and
implemented for class interval geometry and per-BSSID first-observation
selection. ADR-0003 is
Accepted for project-scoped AP identity, the BSSID-to-radio-to-AP-to-classroom
hierarchy, operational radio grouping, static BSSID ownership, unattended
fallback, and value grain. ADR-0004 is Accepted for missing-CU/no-beacon,
coverage, average, and streak semantics. ADR-0007 remains an uncreated
reservation. Revised ADR-0008 is Accepted for observation-time precedence and
class admission: valid Aruba ERM time, otherwise valid frame time. Revised
ADR-0009 is Accepted for reliable per-BSSID/session `min(O-F)` anchoring and
TSF-derived bin placement, ordering, duplicate recognition, and skipped-
interval diagnostics, with whole-BSSID/session observation-time fallback.
ADR-0005 remains authoritative for class-start bin geometry. These decisions
are implemented by the supported `beacon_analysis.py run` workflow.

The 2026-09-03 owner direction for ADR-0006 rejects a permanent parallel
legacy calculation/output path. Commit
`4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb` retains the historical refactored
workflow; future comparisons should normally rerun the new metric definition
on older captures. The current and planned artifact inventory is maintained in
[`output_inventory.md`](../output_inventory.md).

On 2026-09-04 the owner approved PR-0024's five durable logical artifacts:
selected BSSID/bin observations, radio/class summaries, classroom/class
summaries, a generated project identity map, and a small run manifest. CSVs and
plots are derived exports. On 2026-09-09 the owner accepted the physical paths
and partitioning, metric/schema identifiers and placement, data types, tied-
radio representation, source precedence, concise logging, and corrected
golden contract. The corrected runner now implements the contract, including
the three compact CSV review exports as non-authoritative derivatives;
PR-0024 remains `Documented` because it records the accepted architecture.

## When an ADR is required

Create an ADR before work that selects or changes any of the following:

- analysis semantics, metric definitions, thresholds, interval boundaries, or
  missing-data interpretation;
- canonical schemas, timestamps, identity, provenance, or output versions;
- an ambiguity policy that could include, exclude, repair, group, or impute
  data differently;
- public runner, CLI, interface, persistence, compatibility, or support
  contracts;
- a technology choice that materially constrains later implementation.

A local implementation detail that stays within an accepted contract normally
does not need its own ADR. If a proposed implementation exposes an undecided
contract, stop and create or amend the relevant proposed ADR before coding.

Before drafting a lengthy ADR or introducing additional mechanisms,
thresholds, modes, or features, first present the practical need and a simple
high-level recommendation to the project owner. Proceed with detailed design
only after the owner confirms that the added behavior is needed. Prefer the
smallest logical process that meets the accepted precision and accuracy
requirements.

## Identifier, filename, and status rules

- Allocate identifiers monotonically as `ADR-0001`, `ADR-0002`, and so on.
- Use filenames such as
  `0001-canonical-internal-timestamp-representation.md`.
- Never reuse or renumber an identifier.
- Use one of these ADR statuses: `Proposed`, `Accepted`, `Rejected`,
  `Superseded`, or `Deprecated`.
- A `Proposed` ADR must label the decision as unresolved. Draft preferences,
  examples, and recommendations are not accepted decisions.
- An accepted ADR is normally immutable except for typo/link corrections and
  non-decision addenda. Before implementation, the project owner may simplify
  an accepted decision in place when the revision is explicit, dated, and
  identifies the superseded scope. Otherwise a changed decision gets a new
  ADR that supersedes the old one.
- Link each ADR to the current roadmap and to related ADRs where applicable.

Roadmap work uses a separate delivery-status vocabulary defined in
[`post_refactor_roadmap.md`](../post_refactor_roadmap.md). ADR status must not
be used to imply that implementation or validation is complete.

## Lightweight workflow

1. Copy the template below into the next numbered file with status `Proposed`.
2. State the current behavior and evidence, including fixtures and tests that
   do and do not exist.
3. State the decision drivers and viable options without presenting an
   unresolved preference as fact.
4. Record analysis-semantic, schema/output, provenance, compatibility,
   migration, fixture, and test consequences.
5. Review with the people responsible for analysis correctness and affected
   interfaces.
6. Mark the ADR `Accepted` or `Rejected` and record the review date. Do not
   begin dependent implementation while the required decision remains
   unresolved.
7. Update roadmap/design references and acceptance criteria. Do not rewrite
   `current_pipeline.md` until runtime behavior has actually changed and been
   validated.
8. If evidence later invalidates the decision, create a superseding ADR and
   preserve the history.

## ADR template

```markdown
# ADR-XXXX: Decision title

- Status: Proposed
- Date proposed: YYYY-MM-DD
- Date decided: Not decided
- Owners/reviewers:
- Roadmap items:
- Supersedes: None
- Superseded by: None

## Context and current behavior

## Decision drivers

## Evidence and fixture limits

## Options considered

## Decision

Unresolved while status is Proposed.

## Analysis-semantic impact

Use exactly one of: none, behavior-preserving, intentional change.

## Data, provenance, output, and compatibility impact

## Migration and rollback

## Required tests and acceptance evidence

## Consequences and risks

## Unresolved follow-up questions

```

## Decision register

ADR-0001 through ADR-0006, ADR-0008, and ADR-0009 are linked records. The
other filenames remain reservations only:

| ID | Proposed filename | Decision scope | Current state |
|---|---|---|---|
| ADR-0001 | [`0001-canonical-internal-timestamp-representation.md`](0001-canonical-internal-timestamp-representation.md) | Logical/storage timestamp representation; naive, offset, DST, and display-timezone policy | Accepted 2026-08-03 |
| ADR-0002 | [`0002-canonical-beacon-schema.md`](0002-canonical-beacon-schema.md) | Conceptual canonical row grain, authority layers, value/transformation semantics, hybrid provenance direction, and implemented corrected-output boundary | Accepted 2026-07-30; implemented |
| ADR-0003 | [`0003-ap-and-physical-radio-identity-contract.md`](0003-ap-and-physical-radio-identity-contract.md) | Project-scoped AP identity, BSSID hierarchy, operational radio grouping, static BSSID ownership, automatic precedence/fallback, and metric grain | Accepted 2026-08-26 |
| ADR-0004 | [`0004-missing-cu-and-missing-beacon-semantics.md`](0004-missing-cu-and-missing-beacon-semantics.md) | Four-state radio/bin model, missing-CU/no-beacon duration, valid-CU coverage and averages, possible-connected and observed streaks, and classroom maximum | Accepted 2026-09-02; implemented |
| ADR-0005 | [`0005-class-interval-boundary-semantics.md`](0005-class-interval-boundary-semantics.md) | Class start/end inclusion, class-start bin anchoring, fixed-bin per-BSSID first-observation selection without a rolling gate, and trailing partial intervals | Accepted 2026-08-26; implemented |
| ADR-0006 | [`0006-output-and-metric-versioning.md`](0006-output-and-metric-versioning.md) | Schema/metric version separation, manifest identity, validators, and five durable corrected artifacts | Accepted 2026-07-31; implemented |
| ADR-0007 | `0007-interface-technology-choice.md` | Local/hosted and web/desktop/other interface choice, deployment, data boundary, and operational constraints | Not created; unresolved |
| ADR-0008 | [`0008-aruba-capture-timestamp-authority-and-fallback.md`](0008-aruba-capture-timestamp-authority-and-fallback.md) | Observation-time precedence and class admission; observation time also anchors reliable TSF and is the whole-BSSID/session fallback | Accepted 2026-09-02; TSF-anchor relationship revised 2026-09-09 |
| ADR-0009 | [`0009-tbtt-tsf-beacon-slot-and-bin-semantics.md`](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md) | Per-BSSID/session `min(O-F)` anchoring, reliable TSF-derived bin placement/order/duplicates/gaps, and observation-time fallback | Accepted 2026-08-27; TSF-derived binning revised 2026-09-09 |

ADR-0001 through ADR-0006, ADR-0008, and ADR-0009 are implemented for the
supported workflow. ADR-0003 requires no hardware registry or device matrix.
ADR-0004 keeps missing values distinct from
zero, valid-CU time coverage, separate missing-CU/no-beacon durations, a
possible-connected comparison streak, an actual observed streak that ends on
unknown bins, and per-radio calculation before the classroom maximum. Revised
ADR-0008 uses valid Aruba time, otherwise valid frame time, for class admission
and as the fallback timing coordinate. Revised ADR-0009 uses the earliest
valid `O-F` candidate within each reliable BSSID/session to derive TSF time for
bin placement, sequence order, duplicate recognition, and skipped-interval
diagnostics. It retains no cross-BSSID pool, drift model, clock segmentation,
or whole-class fallback. The
[2026-09-01/02 retained-capture characterization](../adr0009_retained_capture_characterization.md)
supports this simplified contract; no numeric clock profile remains for
review. The current remaining work is maintained in
[`post_refactor_roadmap.md`](../post_refactor_roadmap.md).
ADR-0007 should wait until the unified runner and interface boundary are
sufficiently defined.
