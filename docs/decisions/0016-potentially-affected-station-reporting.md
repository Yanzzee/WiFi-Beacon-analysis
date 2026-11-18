# ADR-0016: Potentially affected station reporting

- Status: Accepted
- Date decided: 2026-10-16
- Owners/reviewers: Project owner
- Roadmap items: Alpha 0.2 stations associated with high CU
- Related decisions: ADR-0004, ADR-0010, ADR-0011, ADR-0015

## Decision

Update 2026-11-16: [ADR-0020](0020-conservative-cu-missing-bin-bounds.md) adds
high/very-high station-time bounds to the entity/population CSVs, chart and CU
subsection. Observed percentages and peak results below retain their original
definitions. Bounds use available station-time, including accepted stale counts,
and cannot bound periods whose station counts are unknown.

Report potential exposure, not proven impairment, unique devices or people.
Use the existing radio/bin CU and complete radio station totals. High means
raw CU >=128; very high means >=192. High includes very high; also publish
disjoint below-high, high-only and very-high station-time categories.

For each radio/class session, AP/class session and classroom/class session,
publish paired station-time components and percentages, valid-pair coverage,
missing-CU/no-beacon and station availability, stale-assisted contributions,
and peak simultaneous potentially affected associations. Include all-band
and separate sampled-band views; radio rows already identify their band.
Infrastructure cohorts reuse ADR-0015's AP and classroom enablement definitions.

Exposure is sum(stations * bin duration) in a CU category divided by the same
sum across all valid CU/available-station pairs. Include zero stations and
accepted stale-assisted station values. Never fill CU. A zero station-time
denominator gives an unavailable percentage, not zero exposure. This is pooled
association-time, not an average of session percentages.

Peak counts sum qualifying stations within one bin before taking the session
maximum. Use only bins with valid CU and available station counts for every
radio in the entity/session/band view. Never publish a partial numeric peak;
publish complete-bin duration/coverage and unavailable duration separately.
With complete pairs but no threshold crossing, the peak is zero. With no
complete bin it is null. Preserve the first peak bin/time and whether that bin
uses stale assistance. Independent radio/AP/session peaks are never added.

Expected membership is the existing radio/bin session membership, not a new
inventory of unobserved radios. Empty sessions retain unavailable classroom
rows. Coverage describes this sampled scope, not unsampled infrastructure.
Peak coverage and exposure pair coverage are different denominators: complete
entity time versus pooled radio time. Export both explicitly.

Whole-project overall, band and infrastructure-cohort exposure pools radio
components exactly once. AP and classroom summaries are alternative views,
never added to radio rows. Project peak distributions use classroom/session
peaks (available counts, min, median, p90 and max), not a sum of session peaks
or a claim of a synchronized project-wide peak. These descriptive quantiles
give each available physical session equal weight.

## Outputs and implementation

Add a manifest-registered derived `reports/affected_stations/` package with
separate metric/schema v1 identifiers, session tables, population summaries,
classroom peak distributions, two bounded graphics and methodology. Add
concise overall exposure/coverage lines to the run summary. Stream the existing
one-session detail parts; retain compact summary rows only, with no extra
capture decode or authoritative artifact. Source keys link back through
radio/session/bin to the radio/bin view and selected observations.

## Verification

Test exact station-time and terminal-bin weighting, thresholds, missing CU,
zero/missing/stale station counts, concurrent versus independent peaks,
complete-only totals, multi-band/mixed-cohort views, empty sessions, integer
overflow safety, deterministic logical outputs, create-only writing, cleanup
and manifest registration. Preserve existing corrected/report goldens. Review
cached representative output and runtime/storage separately from full-run cost.

## Consequences

Association counts do not identify unique devices or active transmissions.
Channel CU can reflect other BSSs. Missing observations can bias even paired
results, and complete-only peaks can miss a true peak; coverage accompanies
every interpretation. This report does not establish causal performance loss.

## Web presentation follow-up, 2026-11-13

The owner removed the classroom peak-exposure table and CDF from the web report
and its inventories because absolute peaks can be misleading across class sizes.
The page instead presents station-time shares, association-hours and pair
coverage under Channel utilization, after maximum streaks. The CSV/PNG files
specified above are still generated and retained; their calculation has not
been removed or redefined. See [ADR-0018](0018-web-report-context-and-explanations.md).
