# Alpha 0.2 admission-capacity reporting review

- Evidence generated: 2026-10-08 UTC; record completed: 2026-10-09
- Decision: [ADR-0013](../decisions/0013-radio-admission-capacity-reporting.md)
- Source: completed `2025-full` run `20261004T202139962640Z-3152b7cd`
- Scope: cached selection/report regeneration, not a fresh capture run

## Preservation and semantic evidence

The bounded review reconstructed radio/bin selection from 6,511,028 published
selected observations across 373 physical sessions. Every existing CU, source,
identity, bin, station-count, availability, and stale-assistance column matched
the published 2,847,339-row radio/bin view. Existing CU/station correlation and
coverage CSV values also matched. No historical Original code was executed,
and no published source artifact or configuration file was changed.

Capacity is valid in 2,198,050 radio/bins (77.1966% of the expected grid).
The other 649,289 rows have no selected member beacon. This project contains
no missing/invalid/zero capacity advertisements. Valid-member disagreement
occurs in 112,029 radio/bins; values remain selected rather than averaged.
All chosen capacities use the unchanged CU source in this particular project.

Capacity/CU has 2,198,050 valid pairs. Capacity/station has 2,198,024 pairs,
including 102,156 stale-assisted station pairs. Overall and band panels are
radio/bin populations, not sums of channel/classroom capacity. CU describes
channel conditions; capacity describes the radio's advertised QoS allowance.

## Timing and storage

| Measurement | Result |
|---|---:|
| Initial cached read/re-derivation/validation plus reporting | 357.24 s |
| Read/re-derivation/validation portion | 332.01 s |
| Initial reporting portion, excluding re-derivation | 25.23 s |
| Final cached view-read/report regeneration, after display refinement | 27.44 s |
| Published older radio/bin view | 44,772,558 bytes / 42.70 MiB |
| Extended radio/bin view | 66,292,858 bytes / 63.22 MiB |
| Added view storage | 21,520,300 bytes / 20.52 MiB |
| Final admission package: 2 CSVs, methodology, 8 PNGs | 2,006,893 bytes / 1.914 MiB |

These are single warm-cache measurements, not an SLA or an isolated estimate of
incremental full-run cost. The re-derivation portion includes existing CU
calculations and validation, so it is not the added admission-selection cost.
The prior reporting review's 12.58-second radio package is historical context,
not a controlled baseline for subtracting a full-run delta. A complete capture
run with the new family has not yet been timed. No peak-memory claim is made.

Statistics retain marginal frequencies, fixed 40x30 hexbin grids and participating
radio/session IDs, not all project rows or joint-value pairs. A narrow second
read of the derived view supplies centered Pearson and exact tie-aware rank
correlations. No extra capture decoding or authoritative artifact is introduced.

## Presentation and tests

Final review output is local/ignored under:

```text
output/previews/alpha_0_2_review_20261008/admission_final_20261008T230011Z/
```

Histograms use one-percentage-point bins centered on integer percentages. This
avoids false alternating gaps caused by values such as 98.9984% landing beside
98% at ordinary integer bin edges. Raw values are not rounded, and exact CDFs
retain every advertised value. Relationship grids remain 40x30. Figure sizes
match the doubled reporting convention; radio/session plots are unchanged.

The 60 affected admission, CU-semantic, radio, station, class-session, capture,
run-lifecycle and CLI tests passed. Tests cover zero, missing, invalid, no-beacon,
source fallback, disagreements, independent CU availability, source joins,
ties/constants, exact split-part correlations, fixed hexbin equivalence,
histogram edges, manifest registration and create-only failure cleanup.
All 10 final PNGs (8 admission plus 2 existing CU/station) passed integrity
checks. Representative histogram, CDF and relationship graphics were visually
inspected. Agent review does not replace owner output acceptance.

## Remaining checkpoint

Owner review of the admission outputs and complete-run timing remain release
evidence checkpoints. Beacon-rate semantics, richer 5/6 GHz comparisons and
remaining original-inventory scope decisions stay separate; implementing this
gate does not close Alpha 0.2 automatically.
