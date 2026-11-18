# ADR-0017: Offline run report overview

- Status: Accepted
- Date decided: 2026-10-16
- Owners/reviewers: Project owner
- Roadmap items: Alpha 0.2 reporting organization and navigation
- Related decisions: ADR-0006, ADR-0011 through ADR-0016

## Decision

Add one self-contained `reports/index.html` as the human entry point for each
new run. Use inline CSS, native HTML navigation/disclosures and only the small
inline navigation script accepted below, with no remote assets, server or
application framework. That refinement supersedes the original no-scripts
restriction. Existing filenames, directories,
metric definitions, optional-plot defaults and authoritative artifacts remain
unchanged. This is a derived report, not ADR-0007's interactive UI choice.

Present headline CU, station means, actual 6 GHz use and infrastructure context
from existing compact report tables. Show availability/coverage and units next
to values; unavailable never becomes zero. Retain the source-table/methodology
links and scope/weighting distinctions. Do not reconstruct metrics or read
large observation/bin Parquets for this page.

Organize the current page into Overview, CU, Stations & enrollment, CU/station,
Admission capacity, Band comparison, Capture quality, Class sessions and Evidence.
Station-time exposure belongs under CU after maximum streaks; classroom
peak-exposure tables/CDF are omitted. Group compact session rows by classroom with local times
and a clearly named timezone. Link optional radio plots through the existing
plot index and session disclosures, without embedding the entire collection.
Curate key figures; keep the complete report-file inventory collapsed.

Use run-relative links, validate linked assets, escape input text, and reject
paths outside the run. Explicitly identify unavailable report families. Link
the run summary and manifest; the manifest is written afterward inside the
same atomic publication gate. Register the overview as a derived HTML export
and log its generation stage. A generation/link failure prevents publication.
Never rewrite an older published run to add an overview. QA previews use copies
in a separate preview directory.

## Verification and remaining work

Test full/minimal/empty report families, exact display values, missing versus
zero, safe HTML/path handling, optional radio links, local-link integrity,
create-only writing, failure cleanup and manifest registration. Preserve all
existing logical metric goldens. Inspect desktop and narrow layouts, measure
cached generation cost, and obtain owner review before broader reorganization.
Interactive filtering, automated cross-project browsing and GUI technology
selection remain future design work.

## Owner-requested navigation refinement, 2026-11-13

Use a small, static inline script to highlight the report section currently in
view and set its navigation link's `aria-current="location"`. Native section
links and disclosures continue to work when scripting is disabled. Keep the
menu compact on desktop, with shorter labels and width fitted to its content;
allow links to wrap on narrow screens. Match anchor offsets to the menu height so linked
headings remain visible. This adds no remote dependencies, data loading,
filtering, server or framework, and changes no analysis or report metrics.

Presentation/context follow-up is recorded in ADR-0018. It adds project framing,
full-width explanations, existing enrollment/correlation displays and the
accepted pooled radio-time class-session mean without changing source metrics.
