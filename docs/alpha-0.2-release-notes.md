# Alpha 0.2 — Corrected Reporting Expansion

Release type: Release-candidate notes; not tagged or published. Documentation
reconciled through 2026-11-18; final owner report review continues.

Four fresh v2 project runs and a compact WebP example-report package are now
audited. A local reports-only publication branch is prepared, with explicit
omissions, band-specific watermarked examples and verified shared HBLL plots.
See the [publication closeout](benchmarks/alpha_0_2_publication_closeout_2026-11-18.md).
These are local preparation steps, not a release tag or Pages deployment.

Alpha 0.2 expands the corrected analysis into a navigable reporting package
for network engineers. It preserves the five authoritative artifacts and the
accepted identity, timing, CU coverage and missing-data rules. New reports
describe sampled associations and channel conditions, not unique people,
device capability, measured throughput or causal deployment effects.

## Highlights

Implemented addition, 2026-11-16: ordinary and conditional mean exports now include
conservative missing-bin lower/upper bounds under
[ADR-0020](decisions/0020-conservative-cu-missing-bin-bounds.md). Observed
calculations and canonical schemas are unchanged. Equal-session, zero-station,
paired-window and cadence-window ranges are implemented. Stage 3 adds numeric
CDF bounds, shaded CU CDFs and mean-figure intervals. Stage 4 adds high/very-high
station-time bounds in exposure exports/chart and the CU subsection, conditional
on available station counts. Stage 5 integrates HTML/text displays and supported
saved-run backfill, retaining original observations and source runs. Missing
supporting components leave bounds unavailable. See the
[implementation inventory](cu_missing_bin_bounds_implementation_plan.md) and
[representative review](benchmarks/alpha_0_2_cu_bounds_review_2026-11-16.md).

- Observation timestamps now control all measurement bins and cadence slots.
  Native TSF is diagnostic, with conservative duplicate recognition and
  explicit clock-anomaly lineage/warnings. Core metric and capture-quality
  versions advance to v2; affected older analyses require a fresh run.
- `Sample_classrooms.csv` defines project location scope. Broader schedule and
  AP lists are filtered to those locations before analysis, allowing a
  one-location run without editing the master schedule.
- Offline `reports/index.html` entry point, printed by the CLI, with project
  context, numeric results, full-width explained figures, methodology/source
  links and classroom/session navigation. No server or internet is required.
- Complete station-count totals by radio, SSID, band and classroom, with
  accepted forward/leading-backward stale assistance limited to 30 seconds.
  Unavailable totals stay null rather than becoming partial sums or zero.
- Enrollment-weighted **Peak stations per enrolled**, using
  combined simultaneous-section enrollment. Zero/missing enrollment is excluded.
- Bounded CU/station/admission-capacity radio/bin detail with optional
  radio/class-session time series. Detailed plots remain **off by default**;
  enable `--radio-class-plots` or `RADIO_CLASS_PLOTS=true` when needed.
- Overall and 5/6 GHz infrastructure comparisons, including mixed-classroom
  identification, matched same-AP/session radio pairs, and actual 6 GHz
  station association-time shares. Sampled 2.4 GHz participates in the tables.
- Cohort AP/radio/classroom/class-session sample counts, expandable AP-model
  summaries by sampled radio configuration, pooled classroom band tables and
  the equal-comparison mean signed matched 6-minus-5 GHz CU difference.
- Optional `6GHz_Enabled` AP input, defaulting to observed project-wide 6 GHz
  evidence; explicit contradictions are rejected and enabled-but-unobserved
  infrastructure remains visible.
- Radio QoS admission-capacity distributions and descriptive relationships,
  with independent availability/source diagnostics and no cross-radio sums.
- Exploratory native/ERM beacon-cadence and capture-quality diagnostics. The
  default transmitted expectation is 100 TU; ERM uses one effective observation
  per ten intervals. Empty capture slots are not proven RF loss.
- Station-time with high/very-high CU and paired-data coverage, displayed under
  Channel utilization. Absolute classroom peak-exposure tables/CDF are omitted
  from the web report; their original derived files remain generated. Missing
  CU is never filled.
- Compact wrapping navigation with offline current-section highlighting, and
  `report --from-run` to build a separate refreshed report bundle from a saved
  completed run without decoding captures or changing the original report.
- Existing Pearson r and Spearman rho are visible alongside CU/station plots.
  No regression R-squared or independent-sample claim is introduced.
- Supporting improvements include consistent SSID text/hex decoding, minimal
  Saturday/Sunday schedule support, lowercase `data`/`output`/`sample` roots,
  and updated acquisition utilities under `capture_scripts/`.
- Explicit nullable station-report types remove the pandas concatenation
  FutureWarning while preserving logical values and missingness. CSV integer
  fields may omit unnecessary `.0` formatting.

## Reviewing a run

Open `reports/index.html` and keep the run folder together so relative links
work. The page order is Overview, Channel utilization (including station-time
exposure), Stations & enrollment, CU/station, Admission capacity, Band comparison,
Capture quality, Class sessions and Evidence. Classroom headings include
observed AP/radio counts and known seating capacity; class rows retain local
times, enrollment, mean radio CU, station peaks, coverage and optional plot links
listed one per line. Room band-summary tables appear above those session rows.

Observation dates describe first/last selected in-scope beacons, not the full
capture-file bounds. Scheduled class-hours sum each classroom/session once;
simultaneous rooms contribute separately. Mean radio CU pools valid radio time
and is not a merged classroom/channel measurement. Full metric definitions and
source tables are linked within each report family.

The authoritative artifacts remain `project_identity_map.parquet`,
`selected_observations/`, `radio_class_summary.parquet`,
`classroom_class_summary.parquet` and `manifest.json`. HTML, CSV, other reporting
Parquets and graphics are derivatives. Existing published runs are immutable;
they are not retrofitted with new reports.

## Validation and measured full-run cost

The dedicated corrected semantic oracle and existing logical report goldens
remain unchanged. The 2026-10-31 checkpoint passed 392 non-external tests and 16
real-capture/TShark tests; these are historical counts, not the current total.
The latest 2026-11-13 focused report/refresh/CLI suite passed 58 tests. Final
candidate-wide validation is still to be refreshed after owner edits.
Historical NUL-only SSID text is adapted to the
accepted preserved-hex representation in test expectations only; source
references and production output are unchanged. The final gate is recorded in the
[release-readiness evidence](benchmarks/alpha_0_2_release_readiness_2026-10-29.md),
including the owner-provided short-capture reference location.

The complete 80-capture `2025-full` run processed 49.37 GiB of input, 373 class
sessions and 6,511,028 selected observations using eight workers. It generated
all 691 optional radio/session plots and completed in **5h 02m 26s**, including
CLI preflight/input hashing. Output occupied **477.04 MiB**, of which
**252.34 MiB** was optional radio/session PNGs. The HTML overview took 1.90 s
to generate. This is one observed run, not a controlled performance guarantee.

The full input hash and logical identity/radio/classroom summary fingerprints
match the previous same-input corrected run, excluding run IDs. Selected-row
counts match, overview links and 83 manifest references pass validation, and
all indexed radio plots exist. The later warning fix was verified separately
from cached rollups: all 2,689 class-report rows and eight aggregate rows across
373 sessions match exactly, including nulls, with FutureWarnings treated as
errors. No second five-hour run was needed for that compatibility fix.

## Supported environment and limits

The verified analysis environment remains Windows, 64-bit Python 3.13 and
TShark 4.6.6, installed from a source checkout with `requirements.txt`.
Reviewed vendor adapters remain Aruba and Extreme; Linux acquisition scripts
do not establish analysis-platform or additional-vendor support.

Coverage and capture-scope limits accompany results. Accepted stale station
counts participate, but CU and admission capacity are not stale-filled.
Associations are not unique devices, attendees or proof of impairment.

Per owner direction, R-squared is not required, beacon-loss calibration is
future work, and cross-project/year comparisons remain manual. Automated
cross-year joins, inferred missing CU, safe reliability thresholds,
AP-model loss corrections and causal/inferential models are not included.
Other unaccepted classroom-channel/legacy aggregate statistics are not claimed.
Installers, a GUI framework, resume and hosted deployment remain outside this
source-checkout release. The Original-versus-corrected comparison is a one-time
[informational migration guide](original_to_corrected_migration.md), not a
recurring Original replay requirement.

Final owner output/scope sign-off and explicit tagging/publication approval
remain separate from this candidate preparation.

Selected reports on GitHub Pages are planned separately. A curated public
export/deployment workflow is not implemented. Full-resolution radio plots
and linked reporting Parquets can exceed a single site's storage budget across
three week-long runs; see [reporting and publication](reporting_and_publication.md).
