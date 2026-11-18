# ADR-0013: Radio admission-capacity reporting

- Status: Accepted
- Date decided: 2026-10-08
- Owners/reviewers: Project owner
- Roadmap items: Alpha 0.2 admission-capacity gate
- Related decisions: [ADR-0011](0011-bounded-radio-bin-reporting-source.md),
  [ADR-0012](0012-class-session-and-cu-station-reporting.md)

## Decision

The owner accepted the admission-capacity recommendation and clarified that
CU describes channel conditions, whereas admission capacity describes the
radio's medium-capacity allowance for QoS admission. CU is still sampled at
physical radio/bin grain: separate radios on the same channel are not merged.
Admission capacity must not be summed into channel or classroom capacity.

Select one advertised admission-capacity value per expected physical radio/bin.
Use the unchanged CU-selected beacon when its capacity is valid. Otherwise use
a stable admission source: the first valid BSSID of the configured CU primary
SSID in the session, or the first valid member BSSID if none exists. When that
source is unavailable in a bin, use the earliest valid same-radio member.
Admission availability is independent of CU availability.

Retain raw values; display `raw / 31250 * 100` as **Advertised available
admission capacity (%)**. The field's units are 32 microseconds per second,
not a station limit or throughput estimate. It is not inferred as `100 - CU`.
Values 0 through 31250 are usable, including zero. Larger advertised uint16
values remain in selected observations but are flagged and excluded from
capacity percentages. Distinguish `valid`, `missing_capacity`,
`invalid_capacity`, and `no_beacon`. Do not clamp or stale-fill capacity.

Diagnose distinct valid values among member BSSIDs in the bin. Keep the chosen
value with a disagreement flag rather than summing, averaging, or invalidating
the bin. Differences can reflect different beacon times; persistent differences
may warrant checking SSID/vendor policy. Constant-zero populations receive a
warning without being reclassified as missing.

## Reporting and placement

Extend the bounded radio/bin derived Parquet with capacity/state, separate
source lineage, valid/invalid candidate counts, disagreement, and CU-source
match. The existing CU columns and all five durable artifacts keep their
semantics. Add an admission metric identifier and a report-schema identifier
to distinguish the additive report contract from older derived views.
No new capture decode, authoritative artifact, or project-wide observation
table is introduced. Existing immutable runs are not rewritten.

Report overall and band-separated histograms, exact empirical CDFs, capacity
versus CU and capacity versus available radio-station-count hexbins, and
descriptive Pearson/tie-aware Spearman coefficients. Each eligible radio/bin
contributes once. Unknown values are excluded; permitted stale-assisted station
totals participate and are counted. Report coverage, zero/constant warnings,
source mismatch, invalid-candidate and disagreement diagnostics. No capacity
sums, congestion thresholds, client-admission guarantees, inferential regression,
p-values, or independence claims are introduced. Constant inputs yield null
correlation, not zero. Detailed radio/session plots remain unchanged and opt-in.

## Required verification

- Deterministic CU-source preference and admission fallback, without changing CU.
- Capacity available without CU; CU available without capacity.
- Zero, missing, invalid, and no-beacon remain distinct; no stale extension.
- Multiple BSSIDs never duplicate capacity, and disagreement retains one value.
- Source lineage resolves to the selected BSSID/session/bin row.
- Exact weighted correlations, ties, constants, and stale-assisted pair counts.
- Manifest registration, cleanup, doubled plots, and unchanged CU/station results.
- Measure cached full-project reporting runtime/storage separately from capture
  runtime; review representative graphics before publication.

## Sources

[Cisco's field definition](https://www.cisco.com/en/US/docs/voice_ip_comm/cuipph/7925g/7_0/english/administration/guide/7925cfgu.html)
defines admission capacity in units of 32 microseconds per second.
The accepted usable range and display conversion describe advertised medium
time, not assurance that the AP will grant an admission request.
