# ADR-0018: Web report context and explanations

- Status: Accepted
- Date decided: 2026-10-16
- Owners/reviewers: Project owner
- Related decisions: ADR-0010, ADR-0012, ADR-0015, ADR-0017
- Roadmap items: Alpha 0.2 report usability refinements

## Decision

Refine the offline overview from project-wide findings to individual classroom
sessions. Introduce station counts before CU/station relationships, then
admission capacity and band comparisons; expose existing station/enrollment
statistics and correlation coefficients, and explain section/figure meanings
briefly for network engineers. Use full-width clickable originals, reserving
image dimensions and lazy loading; do not duplicate every plot as a thumbnail
or reduce the existing full-resolution outputs.

Project context comes from the manifest/effective run metadata and compact
reports. Label first/last selected beacon timestamps as the analyzed observation
window, not the complete capture-file extent. Use the schedule's named timezone
and date-specific offsets. Distinguish scheduled class sessions from sessions
with measurements. Total scheduled class time sums physical session durations
once per classroom/session; simultaneous rooms contribute separate class-hours,
not distinct elapsed wall time. Incomplete context stays unavailable.

The enrollment headline is **Peak stations per enrolled**:
sum(eligible classroom/session peaks) / sum(corresponding enrollment). Do not
relabel it as mean concurrent devices/person. Display existing SSID/band
breakdowns and radio station means/coverage from their compact source reports.

Classroom headings include observed AP/radio counts and seating capacity when
known. Class-session rows include mean radio CU pooled
from existing radio/session raw-CU-duration and valid-duration components. This
uses the ADR-0015 entity/session pooling definition, not an equal average of
radio means, CU sum, or merged classroom/channel measurement. Preserve source
links; it does not change canonical summaries or authorize deferred classroom
channel aggregation. Missing components produce unavailable means, not partial
pooled values. A radio with zero valid duration contributes zero weight.

Expose existing Pearson r and Spearman rho and pair counts/coverage beside the
CU/station graphs. They are count-weighted valid radio/bin associations, not
duration-weighted project means or regression results. No R-squared, fit,
p-value, independent-sample claim or causal interpretation is added. Regression
semantics remain deferred. Brief explanations and family methodology links
provide the meaning and limitations without hiding the results in disclaimers.

## Verification

Test section hierarchy, full-width figures/explanations, metadata during atomic
publication, UTC/local/DST display, exact scheduled durations/counts, missing
context, enrollment source values, unequal-duration session CU pooling,
zero-valid radios, existing correlation display and absence of regression
claims. Preserve metric/report goldens, safe HTML/path handling and immutable
published runs; refresh only a separate cached preview for owner review.

## Owner-reviewed presentation reconciliation, 2026-11-13

The current order is Overview, Channel utilization, Stations & enrollment,
CU/station, Admission capacity, Band comparison, Capture quality, Class sessions
and Evidence. Station-time with high CU is a subsection after maximum CU streaks.
The classroom peak-exposure table/CDF are omitted from the page and inventories;
ADR-0016's underlying derived files remain generated. This changes presentation,
not exposure components, enrollment peaks or metric definitions.

Band comparison displays distinct AP/radio/classroom/class-session cohort counts,
an expandable AP-model table stratified by sampled 5/6 GHz radio configuration,
and the equal-comparison mean of saved matched 6-minus-5 GHz CU differences.
Each classroom has a pooled band table above its individual session rows.
Radio plot links are listed one per line within the classroom disclosure.

CU and admission-capacity means in these comparison tables both use valid
duration. The standalone admission-capacity mean uses equal weight per valid
radio/bin reading; this difference is explained in prose, not long column
headers. Admission capacity is secondary; it has no separate cadence analysis.
The capture-quality description reads expected intervals and analysis-bin widths
from the saved configuration, without assuming a default for missing metadata.
No inferred beacon loss or missing CU is introduced.

The Overview wording remains owner-frozen. Refreshing with
`beacon_analysis.py report --from-run <completed-run>` creates a separate report
bundle and records source/renderer provenance. It reuses saved data and supported
plot builders; it does not rewrite the initial full-run report. Public export
and GitHub Pages deployment are planned, not implemented; see
[reporting and publication](../reporting_and_publication.md).
