# Current reports and public publication plan

Reconciled: 2026-11-13. Analysis definitions are unchanged. Final owner report
review and Alpha 0.2 tag/release authorization remain outstanding.

## Current web report

The full run writes `reports/index.html` inside
`output/runs/<project>/<run-id>/`. The page order is:

1. Overview: capture dates, scheduled sample, counts and headline results.
2. Channel utilization: histogram/CDF, maximum per-radio classroom streaks,
   then station-time with high CU, association-hours and paired-data coverage.
3. Stations & enrollment: radio means and enrollment-weighted class peaks.
4. CU/station: overall/per-band descriptive Pearson/Spearman relationships.
5. Admission capacity: advertised remaining radio QoS allowance and relationships.
6. Band comparison: sample sizes, overall/AP-cohort results, model summaries,
   then CU, admission-capacity and station/6 GHz-use figure groups.
7. Capture quality: availability, nominal/effective cadence expectations and
   exploratory previous-window associations.
8. Class sessions: classroom disclosures, room band summaries, individual
   scheduled class/event rows and direct optional radio-plot links.
9. Evidence: provenance and supporting files.

Evidence counts come from recorded input and stage metadata, without opening
the original inputs. Files stored outside the application folder are included.
The first full-run HTML receives these counts before the manifest is written;
report refresh reads them from the saved manifest.

The menu fits its content on desktop, wraps on narrow screens and highlights
the section in view using a small inline script. Native links/disclosures work
without it. The page uses no server, external fonts, CDN or runtime data fetch.
Figures are full-width clickable originals with reserved dimensions and lazy
loading; this affects display/loading, not their stored image bytes.

Band cohort counts are distinct observed APs, radios, classrooms and class
sessions within each row. Different band/cohort rows can share those entities;
their counts are not additive. The expandable model table uses saved `band`
population rows, stratified by model and sampled 5/6 GHz radio configuration,
without duplicating AP- and classroom-cohort records. Room band tables use
`classroom_band_summary.csv` across that room's analyzed sessions.

Classroom headings show observed AP/radio counts and known seating capacity.
Counts that vary across sessions remain identified. Session tables do not
repeat constant room counts as columns; radio plot links appear one per line.
The term class session includes a scheduled event, and classroom identifies the
configured room or monitored space. Missing/zero enrollment remains governed
by the existing input and eligibility rules; attendance is not inferred.

The Overview wording is owner-frozen except for explicitly requested edits.
Absolute classroom high-CU association peak tables and their CDF are omitted
from the web page and its file inventories. The original ADR-0016 derived CSV,
PNG and peak fields are still generated and retained. This is a presentation
decision, not removal or redefinition of their calculations.

Update 2026-11-16: conservative missing-bin bounds are implemented under
[ADR-0020](decisions/0020-conservative-cu-missing-bin-bounds.md). Stage 4 shows
observed/minimum/maximum high-CU station-time percentages and association-hours
in the existing CU subsection. Observed percentages use paired CU/station time;
bounds use all available station-time, including accepted stale counts, with
missing CU allowed to range from 0% to 100%. Unknown station counts remain
outside the bounds. The chart retains observed bars and adds min/max whiskers.
Stage 5 also integrates Overview cards, text summaries and supported historical
backfill. Missing denominators leave bounds unavailable. See the
[bounds review](benchmarks/alpha_0_2_cu_bounds_review_2026-11-16.md); earlier
dated publication/readiness evidence does not include this later feature.

Owner presentation review 2026-11-16: Overview cards show separate overall and
sampled-band observed CU values, with “Between … and … possible” descriptions;
high and very-high CU station-time have separate cards. Detailed metrics and
tables separate observed numbers from possible ranges. Cohort CU CDFs use one
panel per cohort, with a further row only for mixed-classroom comparisons.
Matched-radio and mean-CU-versus-coverage scatter plots show observed points
only; their numeric bounds remain in the tables. Mean/difference histograms
retain independent per-bucket count bounds. Those bucket maxima need not be
jointly attainable, and the shading is a range of total counts, not an amount
to add above the observed counts. Calculations and source values are unchanged.

## Weighting, names and cadence

- Web labels use **Peak stations per enrolled** and mean stations per radio.
  Stored files/fields retaining `client` or `average` have not been renamed.
- Peak stations per enrolled is sum of eligible classroom/class-session peaks
  divided by sum of corresponding combined enrollment. It is not a mean
  concurrent count or the percentage of attendees with a device. The denominator
  does not add an instructor; roles are not inferred from station counts.
- CU and admission-capacity means in the band/model/classroom summary tables
  weight their valid measurements by duration. Mean stations/radio uses duration
  with complete counts, including accepted 30-second assistance.
- The standalone admission-capacity package's mean gives each valid radio/bin
  reading equal weight. Weighting is explained in prose rather than column
  headers. Admission capacity is secondary and has no separate cadence analysis.
- Matched 5/6 GHz CU uses common-valid bins within each radio-pair/class-session.
  Its displayed mean signed difference gives each usable comparison equal weight;
  multiple radio pairs from one AP are not independent AP-level samples.
- Capture-quality prose reads the recorded nominal interval, ERM slot multiple
  and analysis-bin widths. At 100 TU, native expectation is about 9.77 beacon
  opportunities/s/BSSID; the ten-interval ERM baseline is about 0.977/s/BSSID.
  The reviewed run uses 1.024-second CU bins with at most one selected radio
  measurement per bin. Other saved settings produce corresponding descriptions;
  missing settings are not silently replaced with defaults.
- CU coverage measures usable radio analysis time. Cadence availability measures
  occupied expected BSSID slots before representative selection. Missing captured
  beacons do not prove RF loss or establish unobserved CU. Calibration is deferred.

## Refresh without repeating analysis

```powershell
.\.venv\Scripts\python.exe beacon_analysis.py report --from-run "output/runs/<project>/<run-id>"
```

`--from-run` names a completed analysis run, not a previous refresh bundle.
The command creates
`output/reports/<project>/<source-run-id>/<report-id>/reports/index.html`.
Its `--output-root` controls this refresh root; it does not change the full-run
output convention. Each refresh is create-only and preserves the original run
and first report. Captures, TShark, sample files and `config.env` are unnecessary.

The bundle independently copies saved reports, available CSV exports and the
source manifest. It rebuilds HTML and supported figures from compact CSVs and
narrow, bounded reads of saved reporting Parquets. Other graphics, including
individual radio plots, are reused. It does not recompute selection or radio/bin
metrics. `report_refresh.json` records source hashes and renderer provenance;
the copied manifest continues to describe the original analysis.
The beacon capture-slot gap CDF is redrawn from its saved gap-count CSV.

CU mean/CDF and station-time bounds are added to the copied tables when their
saved evidence supports them. Exact sums and durations are preferred. Core
and older cadence means can use unrounded persisted values, retaining normal
floating-point representation limits without inventing integer sums. A narrow
saved-detail pass supplies older zero-station denominators and matched sums;
unavailable components leave bounds blank. The refresh updates band/exposure
paragraphs in the copied text summary, and its metadata records added fields,
sources and hashes. All original CSV cells and Parquet fields remain unchanged.

All linked files must remain together. The bundle includes reporting Parquets,
CSV downloads and optional plot collections; it is much larger than the HTML.
It is not a curated public export. Deleting those files without updating the
HTML and inventories leaves broken links.

## Measured storage and three-week planning

The following counts include every unique local `href`/`src` target in the
reviewed report plus the HTML itself, not merely the embedded pictures. Sizes
are uncompressed MiB (2^20 bytes), not raw capture storage or browser transfer.

Source analysis: `2025-full/20261029T213233082897Z-3152b7cd`.
Reviewed refresh: `20261113T164626141987Z-354718bc`.

| Linked content | Files | MiB |
|---|---:|---:|
| Radio/class-session PNGs | 691 | 252.34 |
| Summary PNGs | 31 | 4.22 |
| CSVs, including review exports | 30 | 6.80 |
| Three reporting Parquets | 3 | 101.20 |
| HTML, JSON and text documents | 13 | 1.00 |
| **Total linked report** | **768** | **365.56** |

The whole refresh bundle is about 365.65 MiB; it additionally retains the
refresh record and the two deliberately unlinked peak-exposure files. This
differs from the original full analysis's 477.04-MiB footprint because canonical
root Parquets and other analysis artifacts are not copied into the refresh.
Linked radio/bin detail, station rollups and cadence windows account for the
101.20 MiB of reporting Parquets.

A retained one-week `2024-full` run, `20261005T060004004196Z-1f855431`, covers
2024-11-11 through 2024-11-15 and contains **1,811 radio/class-session PNGs
totaling 617.85 MiB**. Both its images and the 2025 reference have 3360x1400
canvases. The older 2024 run predates the current full report package, so this
is measured plot evidence, not a finished new-format 2024 report-size claim.

Measured plot means are about 0.34-0.37 MiB each; use 0.35-0.40 MiB times the
radio/class-session plot count as a preliminary planning rule. Image complexity,
session duration, sample membership and compression affect the result. Capture
bytes alone do not predict the plot count or published report footprint.

For **one 2024-like week plus two 2025-like weeks**, existing images alone sum
to about **1,122.5 MiB (1.10 GiB)**. Budget approximately **1.5-1.8 GiB** for
all currently linked files across three equivalent new-format reports; allow
about **2 GiB** of hosting space for one revision of each. This is a projection,
not a measurement of the owner's pending runs. Additional large weeks or retained
published revisions increase it. Removing linked reporting Parquets would save
space but requires a public export that repairs/removes their links; even a
viewing-focused collection with all PNGs is likely around 1.1-1.3 GiB.

After each new run, record actual radio-plot count/bytes, summary graphics,
linked CSV/JSON/text, linked Parquets, total unique referenced bytes and largest
file. Preserve the new run's own manifest rather than inheriting an older estimate.
Current audit helpers/results are ignored local previews under
`output/previews/publication_review_20261113/`.

## Public hosting: local publication candidate prepared

Update 2026-11-18: the owner completed and selected `2024-classroom-captures`,
`2025-classroom-captures`, `2026-classroom-captures` and `2026-hbll`. The
[publication closeout record](benchmarks/alpha_0_2_publication_closeout_2026-11-18.md)
records their measured runtime/storage and the prepared **68.78-MiB** static
package. It includes repaired WebP links, deliberate data-omission notices,
clearly labelled watermarked examples per sampled band, and actual shared HBLL
plots after exact measurement equality checks. Public plot indexes retain
source paths plus separate publication-path/type columns. The two unlinked
classroom peak-exposure files are omitted. No statistical definition changes.

The reports-only local `example-reports` branch in
`Yanzzee/WiFi-Beacon-analysis` contains a catalog and per-project directories;
each report keeps its `reports/index.html`. A link is prepared in that repo's
main README. The original analysis runs and owner's copied `Output/runs/`
folders were not modified. Push, owner browser review, Pages configuration and
the deployed-site smoke check remain pending. This is a one-time prepared
publication candidate, not a new application export command.

The older storage estimates above describe complete linked report collections.
The much smaller current package omits most radio/session images and all
Parquet data; its size does not establish that a complete collection would fit.
The following hosting alternatives remain relevant for larger future exports.

As checked 2026-11-13, [GitHub Pages limits](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits)
cap the published site at 1 GB. That is a site-wide limit, not a per-report
allowance. Three full reports with all current-resolution radio plots should
not be planned as one unmodified Pages site.

Possible delivery choices are:

1. Pages landing/summary reports, with the complete browsable report/plot
   collection on static hosting with sufficient capacity.
2. Pages landing/summary reports plus complete offline report ZIPs as GitHub
   release downloads. ZIPs preserve relative links after extraction, but do not
   provide direct online browsing of individual plots inside the archive.
3. Separate project sites/repositories, provided each project's actual footprint
   fits the Pages limit. The larger 2024 result needs measurement first.
4. Measure image optimization or explicitly approved web-sized images,
   retaining full-resolution originals separately. The current curated package
   uses the owner's WebP copies and freshly rendered watermarked chart examples;
   its savings cannot be extrapolated to a complete plot collection.

[GitHub Releases](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)
allow files under 2 GiB each and up to 1,000 assets per release; an archive per
approved project is more practical than thousands of separate PNG assets.

A future public exporter should allowlist approved projects/assets/fields,
regenerate links and inventories, and write public provenance without local
paths. AP/BSSID/SSID identifiers, room/course labels, dates and enrollment occur
in HTML, CSV and image titles and must be reviewed as actual content. The original
manifest/refresh record is not automatically the public metadata document.
Preserve analysis/run IDs, versions and useful hashes without publishing local
filesystem inventories by default.

Use a dedicated tracked public-report source folder and a catalog of immutable
report revisions. The existing `output/` is ignored; do not force-add complete
local run folders. A future manually triggered Pages workflow should deploy only
the reviewed site directory, validate exact-case links under the repository URL
prefix, and avoid running TShark/capture analysis. The current one-time prepared
branch does not add an application export command, change repository settings,
deploy a site or publish a release tag.

The development origin names `Yanzzee/Beacon-analysis`; the owner selected
`Yanzzee/WiFi-Beacon-analysis` for this publication. Public hosting does not resolve the separate richer UI
choice, and final owner scope/output and release approval remain outstanding.
