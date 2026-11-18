# Offline run overview validation

Date: 2026-10-16. Scope: ADR-0017 navigation, compact-source display, offline
link integrity, lifecycle integration, rendering and cached generation cost.

Historical checkpoint: the sizes, test counts and script-free presentation
below describe the October implementation. Current navigation includes a small
inline active-section script, and current page structure/refresh behavior are
documented in [reporting and publication](../reporting_and_publication.md).

## Full-project preview

Source: immutable `2025-full` run `20261014T160532618192Z-3152b7cd`, copied into
a separate ignored preview directory. Only compact report assets and CSV
exports were copied; observation/bin Parquets were neither copied nor read.
Previously validated affected-station reports and band plot refinements from
the same source run were included for a representative current presentation.
The copied manifest describes the source run, not a newly published analysis.

The page groups 373 physical sessions across 18 classrooms, displays existing
overall and infrastructure-cohort metrics, and links the seven report families.
Generation plus local-link validation took 0.122 seconds; the final HTML is
81,788 bytes (about 80 KiB), excluding linked assets. Asset copying is not part
of that timing. This is page-generation evidence, not incremental full-run cost.

## Presentation and compatibility

Desktop, a true 390-pixel embedded viewport, and the classroom section anchor
were visually checked using installed Chrome's headless renderer. The browser
automation runtimes were unavailable. Native disclosure interactions are covered
structurally and await owner usability review rather than an automated click test.

Cards stack at narrow widths, navigation wraps, wide session tables scroll
within their container, and optional radio plots are not embedded wholesale.
Images reserve intrinsic dimensions before lazy loading to stabilize section
jumps. All generated href/src targets and section anchors are validated;
there are no scripts, remote fonts, CDN assets or server requirements.

Tests cover zero versus unavailable/nonfinite values, exact source display,
coverage units, HTML escaping, encoded filenames, optional radio index links,
missing families, create-only writing, local/run-boundary validation, stable
image layout, manifest registration, CLI discoverability and atomic failure
cleanup. Existing logical metric goldens are unchanged. HTML/PNG bytes are not
regression oracles. Compilation and whitespace checks pass.
Final regression: 377 passed, 16 external-tool/short-capture tests deselected,
in 102.55 seconds. The ten focused overview checks also pass independently.

## Remaining owner review

Open `reports/index.html` on a future run, or the cached preview, and review the
path from headline metrics to band comparison, classroom/session and evidence.
CSV links may open or download according to browser settings. Keep the run
directory together. No existing report files were moved or old runs rewritten.
Broader formatting/folder changes and interactive/cross-project UI remain
separate follow-up decisions; ADR-0007's framework choice remains unresolved.

## Owner refinement follow-up

ADR-0018 implements the accepted broad-to-detail hierarchy, observation-window
and scope context, scheduled class-hours, existing enrollment/radio-station
tables, visible Pearson/Spearman coefficients and coverage, full-width explained
figures, and class-session AP/radio counts with valid-radio-time pooled CU.
Selected-beacon dates are not labelled complete raw-file bounds. Scheduled
class-hours count simultaneous rooms separately. No regression/R² is introduced.

Fresh cached preview generation and link validation took 0.167 seconds; the
page is 108,760 bytes (about 106 KiB). Source compact reports were copied into
a new preview; canonical tables/old runs were unchanged. The metadata gives
18 classrooms, 373 scheduled/observed class sessions, 448.50 scheduled class-hours
and 6,511,028 selected BSSID/bin observations. Counts/components retain source
definitions; this is not another full capture run or full-file time inventory.

The overview, a 390-pixel viewport, the correlation table and an expanded
classroom/session table were visually checked through local QA frames in
headless Chrome. An expanded QA copy opens the first native room disclosure;
the deliverable still starts collapsed. Click-through radio paths retain
focused structural/link tests rather than an interactive browser automation test.

Regression: 381 passed, 16 external-tool/short-capture tests deselected in
102.11 seconds. The 14 overview tests include date-specific DST, pending-manifest
metadata, overlapping class-hour sums, unequal-duration radio CU pooling,
incomplete components, zero-valid radios, enrollment/correlation source display
and section hierarchy. Existing logical metric/report goldens are unchanged.
