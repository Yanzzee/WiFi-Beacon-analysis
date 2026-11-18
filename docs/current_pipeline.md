# Current Beacon Analysis Pipeline

`beacon_analysis.py run` is the supported end-to-end workflow. It reads raw
captures and the three user CSVs, validates the complete plan, performs the
corrected analysis, and atomically publishes one run-scoped result.

## Execution flow

1. Resolve flags, caller environment, and `config.env`.
2. Validate TShark, capture containers, CSV schemas, dates, timezone, and
   expanded physical class sessions.
3. Hash every capture and source CSV for the run manifest.
4. Detect each capture as native 802.11 or Aruba ERM type 0, 3, or 6.
5. Extract the union of required capture and supported vendor fields once.
6. Normalize Aruba or Extreme AP-name evidence into the canonical observation
   shape and filter to configured AP hostnames.
7. Retain compact project-wide identity evidence and stage only observations
   falling within their classroom class windows.
8. Resolve BSSID to radio to AP to classroom identity using AP name plus
   band/channel. Radios on different APs remain distinct even on the same
   band/channel.
9. Process one complete class-session partition at a time: diagnose native TSF,
   conservatively reconcile duplicate native views, and use observation time
   for bin placement and first-observed selection.
10. Calculate corrected radio/class and classroom/class CU, coverage,
    missingness, and streak summaries.
11. Build bounded per-session station-count rollups with the accepted
    30-second stale-assistance and complete-total rules.
12. Write the five durable artifacts, compact CSV exports, CU report package,
    and derived reporting families inside lifecycle-owned staging. Generate
    `reports/index.html` from compact reports, validate its local asset links,
    and register it as a derived export before writing the manifest.
13. Validate the manifest and atomically publish the completed run directory.

No processed, grouped, sampled, or legacy analysis working directories are
part of this flow.

Start human review at `reports/index.html` (ADR-0017), printed by the CLI after
publication. Its offline overview links headline/coverage results, infrastructure
comparisons, classroom/session detail, optional radio plots and supporting
tables/methodology. It changes no metric definitions or existing paths and
requires no server or internet. A small inline script highlights the current
section; ordinary links/disclosures work without scripting. Missing packages stay explicit. A link
or generation failure prevents publication; old runs are not retrofitted.
ADR-0018 adds manifest/run context before publication, selected-observation
date bounds, scheduled class-hours, existing peak/enrollment and correlation
displays, full-width figure explanations, and pooled mean radio CU with
observed AP/radio/seat context in classroom headings. Band/model/cohort tables
and each classroom's band summary precede detailed class-session rows; optional
radio plots are linked one per line. This is compact-source
presentation and accepted entity/session pooling, not a new decode pass,
regression model or merged classroom-channel measurement.

The current page order is Overview, Channel utilization, Stations & enrollment,
CU/station, Admission capacity, Band comparison, Capture quality, Class sessions
and Evidence. Station-time with high CU is a CU subsection after classroom
maximum streaks. Classroom high-CU association peak tables/CDF are omitted from
the web page and its file inventories; their saved derived outputs remain.
See [reporting and publication](reporting_and_publication.md) for current display
weighting, saved-run refresh, and the planned public export boundary.

`beacon_analysis.py report --from-run <completed-run>` creates a separate
`output/reports/<project>/<source-run-id>/<report-id>/` bundle. It rebuilds HTML
and supported figures from saved compact reports/narrow rollups without source
captures, TShark, sample files or `config.env`. It does not change the original
run or the initial full-run report location. The copied manifest describes the
source analysis; `report_refresh.json` records the newer renderer and sources.

## Command and configuration

Typical command:

```powershell
.\.venv\Scripts\python.exe beacon_analysis.py run --project "2025-full"
```

Resolution precedence is command-line flag, caller environment, selected env
file, then interactive prompt. `--yes` skips the final plan confirmation;
`--non-interactive` disables all prompts.

Current `config.env` settings:

- `CAPTURES_FOLDER`
- `OUTPUT_FOLDER` or optional `CORRECTED_OUTPUT_ROOT`
- `SAMPLE_FOLDER`
- `START_DATE` and `END_DATE` (inclusive schedule expansion range)
- `TIMEZONE`
- `TSHARK_PATH`
- `ERM_UDP_PORT`
- `CAPTURE_WORKERS`
- `CU_PRIMARY_SSID`, `RADIO_CLASS_PLOTS`
- `BEACON_INTERVAL_TU` (capture-quality expectation only)

Individual CSVs and all major settings can be overridden by command-line
flags. One capture worker is the built-in fallback; four is the current
measured recommendation. The accepted range is 1-16.

`OUTPUT_FOLDER` names the base application-output folder and the CLI appends
`runs`. `CORRECTED_OUTPUT_ROOT` and `--output-root` name the exact directory
that contains project folders. The canonical explicit override is therefore
`--output-root output/runs`, not `--output-root output`.

## User inputs

### Captures

`.pcapng` is preferred; true `.pcap` is also accepted. Container type is
validated by magic bytes rather than filename extension. Capture-profile
detection is performed independently per file.

Reviewed profiles:

| Profile | Timing | Radio evidence | Notes |
|---|---|---|---|
| Aruba ERM type 0 | Aruba source time plus frame time | Payload/channel evidence varies | Supported |
| Aruba ERM type 3 | Aruba source time plus frame time | Most useful reviewed ERM radio metadata | Preferred Aruba CU capture |
| Aruba ERM type 6 | Frame time when Aruba time is absent | Radiotap plus decoded beacon evidence | Supported |
| Native 802.11/radiotap | Frame epoch time | Radiotap/beacon fields | Supported for reviewed Aruba/Extreme AP-name IEs |

Aruba ERM types 1, 2, 4, and 5 are not claimed because no representative
project captures have been reviewed.

### CSVs

- `Sample_APs.csv`: `Hostname`, classroom mapping (`Building-Room` or its
  source columns), and optional `Model`.
- `Sample_classrooms.csv`: the explicit project location scope and capacity metadata.
- `Sample_schedule.csv`: location, course, class period, meeting days,
  enrollment, capacity, and schedule-date applicability.

The CSVs are loaded directly into memory. Source schedule sections with the
same classroom/start/end become one physical class session; enrollment is the
sum of the source sections. The normalization is shown in terminal preflight
and retained in the log, manifest, and run summary.

The `Location` values in `Sample_classrooms.csv` select the project locations.
Schedule `Location` and AP-mapping `Building-Room` values are cleaned for
whitespace/non-breaking spaces and matched exactly against that list. Rows
for other locations are excluded before validating schedule values, expanding
sessions, applying AP-name capture filters, or calculating deployment counts.
A full multi-location schedule and AP file can therefore be reused with a
one-location classroom file. The selected locations and scope policy are
retained under `effective_configuration.input_adapter.location_scope` in the
manifest; the complete original CSVs are still hashed as source inputs.

An empty/blank location scope, an absent AP mapping for a selected location,
or a schedule with no rows in that scope fails preflight with an input error.
Selected locations with mapped but unobserved APs remain in scope and retain
the unavailable-data behavior described below, provided the run has matching
project observations; the existing error for no matching configured AP
observations at all is unchanged. The programmatic normalized
adapter, which does not take a classroom file, continues to use the supplied
AP mappings and class sessions directly.

A class session is identified by classroom plus normalized UTC start and end,
so simultaneous sessions in different rooms remain distinct.

## Extraction and normalization

The extraction plan requests required common fields plus all registered AP-name
fields in one TShark pass. Important canonical evidence includes:

- `aruba_erm.time` and `frame.time_epoch`;
- `wlan.fixed.timestamp` and `wlan.fixed.beacon`;
- `wlan.bssid`/`wlan.ta`, SSID, QBSS CU, station count, and admission capacity;
- Aruba and Extreme AP-name IEs;
- radiotap/radio frequency and DS/HE/radio channel fields; and
- source capture, frame number, capture rank, profile, and ERM type.

The production display filter includes configured AP hostnames across every
supported AP-name field. Rows missing a supported AP name or outside configured
project scope are discarded without a second counting pass.

SSID normalization handles readable names and hexadecimal encodings in the
same input. Each distinct hex token that decodes to printable UTF-8 is mapped
to its readable name; decoding no longer depends on the first 100 rows, the
fraction of hidden SSIDs, or the capture-file boundary. Missing markers remain
missing evidence, and invalid UTF-8 or control-byte encodings remain unchanged
rather than being collapsed into replacement characters. Decoding is cached
per distinct token within each capture. A literal readable name that itself
looks like the hex encoding of printable UTF-8 is inherently ambiguous without
an explicit representation marker; this normalization treats it as encoded.

## Identity

Configured hostname determines AP and classroom membership. Within one AP,
band plus current channel separates radios; a channel number is never used
without band because 2.4, 5, and 6 GHz channel numbers overlap. Different APs
on the same band/channel remain distinct. BSSID MAC-pattern grouping is not
used when AP name is available.

After extraction and identity resolution, the runner warns about configured
APs without matching supported beacon observations and scheduled classrooms
without resolved radios. These warnings appear before class-session analysis
and are retained in the run log and manifest. A zero-radio classroom remains
in the scheduled reports with unavailable measurements and an explanatory
notice; its missing measurements are not converted to zero. Inconsistencies
between summaries for classrooms with resolved radios still fail validation.

Channel changes are retained as observed evidence. A BSSID maps to one radio
group, a radio group to one AP, and an AP to one classroom for a project run.

## Timing, binning, and selection

Class admission first uses preferred observation time: valid Aruba ERM time,
otherwise frame epoch time. Observations outside the scheduled class interval
by that time are excluded.

Observation time also controls sequence order, first-beacon selection and bin
placement. TSF never retimes an admitted beacon or excludes it for a mapped
timestamp outside the class. Native TSF is
checked independently per BSSID/class session, without ERM values, by comparing
successive TSF and observation-time increments and their cumulative offset
range. The trust tolerance is one advertised beacon interval, with the default
100 TU when unavailable. This is a conservative diagnostic/deduplication
bound, not a clock correction or an inferred beacon-loss threshold.

Only trusted native streams can collapse repeated TSF values, and only when
retained beacon fields match, the views come from distinct capture files, and
their observation timestamps are within 102.4 ms. Ambiguous or conflicting
views remain. No origins, drift fitting or clock segments are constructed.
The legacy `tsf_origin` and `tsf_derived_time` output columns are null in new
runs; `timing_method` is `observation_time`. Raw TSF and source lineage remain.
See [ADR-0019](decisions/0019-observation-time-and-native-tsf-diagnostics.md).

Class and bin intervals are half-open. Bins start at class start, use the
configured width (currently 1.024 seconds), and preserve the exact duration of
a terminal partial bin. Selection keeps the earliest observed record per
BSSID/bin after source precedence and deterministic tie-breaking. There is no
rolling minimum-spacing gate.

## Corrected summaries

Missing CU never means zero. Each radio/bin is one of:

- observed valid CU;
- beacon received with missing/invalid CU; or
- no beacon.

Valid-CU coverage is valid-CU bin duration divided by complete class duration.
CU averages exclude missing-CU and no-beacon bins. High CU begins at raw value
128 and very high at 192.

Both streak interpretations are retained:

- actual observed: missing CU or no beacon ends the streak;
- possible connected: a high bin on both sides of one empty bin may bridge it
  for comparison.

Streaks are calculated per radio. The classroom result is the maximum from any
in-classroom radio and records every tied responsible radio ID.

The same selection pass writes a bounded derived radio/bin view. A configured
primary SSID, `eduroam` by default, supplies CU when valid; otherwise the
earliest valid same-radio member BSSID supplies one fallback value. Exactly one
CU value is emitted per physical radio/bin. Explicit `missing_cu` and
`no_beacon` rows preserve unknown time instead of implying zero.

Radio/class plots convert canonical UTC bin instants to the configured named
schedule timezone only for display. Titles show AP, radio, local
date, and local class interval without repeating the class-session identity.
The x-axis shows local 24-hour time. Measured CU uses a blue line with
light-blue area fill; unknown intervals remain blank and any dashed blue bridge
remains visual only. Station count is orange and uses a consistent 0-100 axis
unless an observed value requires a higher next-multiple-of-10 limit.
An orange dashed visual bridge connects available station counts across
unavailable gaps, including stale-assisted endpoints; it does not fill nulls
or affect metrics. Side padding is 2.5% of the plotted time span. The upper-left
box shows building-room, subject, seat capacity, combined enrollment,
configured distinct room AP count, AP model, and band. Missing details show
Unknown, and conflicting seat capacities are also shown as Unknown.

Schedule wall times are interpreted with an explicit named IANA timezone and
then normalized to UTC before class admission or binning. `America/Denver`,
for example, automatically applies MST or MDT according to each schedule date;
the executing computer's timezone and the date when analysis runs are never
consulted. Fixed-offset zones cannot provide that DST resilience.

Station count remains available per selected BSSID observation. A BSSID seen
at least once in a physical class session is a station-rollup member for that
session. Missing values use the same BSSID's prior valid value for at most 30
seconds; leading missing bins may use the first later value for the same
limit. Interior gaps are never filled from the future. Radio, SSID, band, and
classroom totals are numeric only when every member is available, and each
available total records whether stale assistance was used.

The class-session station statistic is the maximum available classroom/bin
total. It includes stale-assisted bins and is divided by combined section
enrollment only when enrollment is positive. The web label is `Peak stations
per enrolled`; stored legacy-named fields remain unchanged. Run-wide overall, per-SSID, and per-band values use the
enrollment-weighted ratio of sums: eligible class-session peaks are summed and
divided by the corresponding combined-enrollment sum. Zero/missing enrollment
and unavailable peaks are excluded. These rollups are derived reports;
selected observations remain authoritative.

Project-level station distributions use complete classroom/bin totals, with
separate band/classroom/bin panels. Unavailable totals are excluded and stale-
assisted values are identified. CU/station relationships use only same-radio,
same-bin valid pairs from the bounded radio/bin view. Overall and per-band
hexbins accompany pair coverage and descriptive Pearson and tie-aware Spearman
coefficients. No p-values or regression are reported because adjacent bins
from one radio/session are repeated observations.

The class-session report joins accepted classroom/radio summaries and station
peaks with schedule context. Simultaneous sections retain deterministic course
labels and source-section count, and enrollment is combined. Room capacity is
descriptive only: missing/conflicting values remain null, conflicts are
flagged, and no room-capacity or enrollment-to-QBSS-capacity ratio is derived.

ADR-0013 adds `reports/admission_capacity/`, backed by capacity fields in the
existing bounded radio/bin view. One valid advertised radio admission allowance
is selected per bin, preferably from the unchanged CU source, with independent
preferred-SSID/member fallback. Raw 0..31250 is displayed as a percentage of
medium time (`raw / 31250 * 100`); zero, missing, invalid and no-beacon remain
distinct. No stale capacity is invented. Separate lineage and valid-member
disagreement/invalid-candidate diagnostics are retained.

CU describes channel conditions, still sampled separately per physical radio;
admission capacity describes the radio's advertised QoS admission allowance.
It is not summed into channel/classroom capacity, inferred as 100-CU, or treated
as a client limit. Overall/per-band histograms, exact CDFs, capacity/CU and
capacity/station hexbins, and descriptive Pearson/Spearman correlations use one
eligible radio/bin contribution. Stale-assisted station pairs participate.
Fixed display grids and marginal frequencies plus one narrow derived-Parquet
read keep reporting bounded; no new capture decoding or durable artifact is added.

### Exploratory beacon cadence and capture quality

ADR-0014 adds `reports/beacon_quality/` from pre-selection session observations,
built one physical session at a time. No additional TShark decode is needed.
Native and ERM families are kept separate, with files pooled within each family
and source references retained. ERM TSF is not used in these diagnostics;
native slots use observation time after conservative native-only duplicate
recognition. Native diagnostics are reused from selection, including in mixed
runs; adding ERM cannot alter native TSF trust or native slot placement.
Forward/backward jumps and offset inconsistencies are flagged in the run log,
manifest warning counts, HTML capture-quality section and cadence CSV. That
CSV retains jump counts, maximum step error, offset range and the capture,
frame, observation timestamp and TSF value of an anomalous observation.

New core outputs use metric `beacon.metric/corrected-cu/v2` with the unchanged
analysis-results v1 table shape. Capture-quality metric/schema identifiers
are v2. Historical runs are not rewritten. Repairing earlier retimed or
excluded observations requires a new analysis, not `report --from-run`.

The default diagnostic expectation is 100 TU (102400 microseconds), with an
explicit `BEACON_INTERVAL_TU`/`--beacon-interval-tu` override if deliberately
changed. Advertised intervals have mismatch/missing diagnostics. ERM slots are
ten nominal intervals; extras in the same BSSID/slot do not increase its
effective cadence and are not removed from canonical processing. Windows
contain 100 nominal intervals (10.24 seconds by default), with exact terminal
counts and separate member/any-beacon availability.

Expected slots span the class window for session-observed family BSSIDs.
Listening exposure and ERM own/neighbour role are unknown, so empty slots are
not confirmed RF losses and class-window rates are not active listening rates.
The package retains edge/interior gaps and timing/duplicate uncertainty.

Descriptive associations use current member-empty-slot fraction and the
previous window's available duration-weighted pooled radio/bin CU/station
context. Missing CU stays missing; accepted station assistance participates
with stale-duration/pair diagnostics. Pearson is null for constants, and pair
coverage remains visible. CSV cohorts separate family/model/band and sampled
AP 5/6-GHz radio-group counts. These are not complete hardware inventories.
No causal claim, safe-CU threshold, missing-CU estimate, or periodic-loss/model
correction is introduced. Simultaneous ERM/native validation remains deferred.

### Potentially affected station reporting

ADR-0016 accumulates `reports/affected_stations/` while the existing complete
per-session radio/bin parts stream. It retains only compact radio/AP/classroom
session records and reuses ADR-0015 infrastructure classifications. Project,
band and cohort exposure pools radio station-time components exactly once.
Accepted stale station counts participate; CU is never filled. High/very-high
raw thresholds remain 128/192, with very high included in high.

Potentially affected association peaks require complete CU/station pairs for
all session-active radios in that entity/band bin, sum within the bin, then
take the session maximum. Incomplete totals are unavailable, not partial peaks.
Peak source bin/time, coverage and staleness remain explicit in the saved data.
The current web report intentionally omits classroom peak distributions because
absolute peaks can be misleading across different class sizes; it presents
station-time exposure and pair coverage under Channel utilization instead.
The manifest registers the three CSVs, two bounded plots, context and
methodology; the run summary adds concise overall exposure and pair coverage.
This describes potential association-time exposure, not unique devices or
proven impairment. Existing authoritative artifacts and older runs are unchanged.

### Detailed band comparison and longitudinal exports

ADR-0015 adds `reports/band_comparison/`. The optional `6GHz_Enabled` AP input
column is validated during loading. Blank/auto uses project-wide observed
6 GHz radio evidence; explicit declarations are retained, enabled-but-unobserved
APs are flagged, and false declarations contradicting captured 6 GHz fail before
publication. This classification is reporting context, not a new radio-band
resolver or a change to authoritative CU/station artifacts.

Comparison accumulation shares the existing per-session radio-detail and
station-rollup streams without another decode/read pass. Compact radio/session
and matched AP-pair rows, exact CU marginals, and fixed display grids survive;
whole-project bin rows do not. Overall/band/AP-cohort/classroom-cohort summaries
retain valid-duration CU means, coverage, high/very-high shares, authoritative
streak maxima, station availability/staleness, and independent capacity means.
Equal AP/session and classroom/session CU views expose weighting differences.
Mixed classrooms and model/sampled-radio-topology strata remain identifiable.

Same-AP/session radio pairs use common valid CU bins only. CU relative to
associated station count uses same-radio/bin positive-station pairs and a
ratio of duration-weighted sums. Station-use shares use simultaneous complete
band totals: 6/(5+6) and 6/(2.4+5+6), with existing stale assistance. Overall use
shares accumulate classroom contributions only, avoiding overlap with AP views.
Expected-but-unavailable bands are unknown, not measured zero. 2.4 GHz is included
when present, although primary charts emphasize 5/6 GHz.

Stable CSVs, infrastructure enablement exports, component sums and context
support manual cross-project comparison. Captures do not establish unique
device counts, 6 GHz capability, throughput/latency, or causation. No automatic
cross-year AP/radio identity or unobserved channel-width/power inference is added.
The run summary includes concise overall deployment, CU and actual-use metrics.

## Output lifecycle

Repository storage roots are lowercase: `data/`, `output/`, and `sample/`.
New manifests and export paths use `output/runs/`. Published historical run
files retain their original recorded path spelling and provenance; the folder
case migration does not rewrite or rehash those records. Nested project names
and input filenames are unchanged.

Successful runs publish below:

```text
output/runs/<project-slug>/<UTC-timestamp>-<input-hash-prefix>/
```

The five durable logical artifacts are:

1. `project_identity_map.parquet`
2. `selected_observations/`
3. `radio_class_summary.parquet`
4. `classroom_class_summary.parquet`
5. `manifest.json`

Compact human-readable copies of identity and summary tables are under
`exports/csv/`. `reports/run_summary.txt` is the run-wide first-read report.
`reports/cu_coverage_streak/` contains a comprehensive `methodology.txt`, three
review CSVs, and eight bounded plots.
`reports/radio_class_detail/` contains the bounded combined CU/station
radio/bin Parquet view, relationship table/plots, and methodology. Its indexed
radio/class plot collection is produced only when `--radio-class-plots` or
`RADIO_CLASS_PLOTS=true` is set.
`reports/station_count/` contains a bounded rollup Parquet table, a compact
class-session CSV with per-radio, per-SSID, per-band, and classroom peaks, an
enrollment-weighted aggregate CSV, classroom/bin and band distribution plots,
and station-specific methodology. `reports/class_sessions/` contains the
joined physical-session review table and accepted enrollment/peak plots. All
packages are registered as derived exports in the manifest. Aggregate station
and CU/station results are also presented concisely in
`reports/run_summary.txt`.

All generated report graphics use an expanded canvas at twice the former
width and height while retaining the existing text and line sizing. Histogram
resolution follows the underlying data: raw-CU and time-streak histograms stay
at their natural one-raw-value and one-analysis-bin limits; average-CU uses
two-raw-value buckets, class-share distributions use 2.5-percentage-point
buckets, and station-count distributions use one-client buckets.

Each run has a unique ID. Existing runs are never replaced or resumed. All
files are built in an owned hidden sibling staging directory; the manifest is
written last and the directory is renamed atomically. Handled failure or Ctrl+C
records the reason and removes staging. The next run removes an abandoned owned
staging directory only when its recorded process no longer exists.

## Progress and performance

The CLI reports capture number/total, filename, detected profile, completion
time, supported/configured/class-window row counts, and session staged/selected
rows. Exact within-capture percent is unavailable without another decode pass.

Capture workers independently extract, normalize, scope-filter, and stage
session parts. Identity resolution, session selection, summaries, and
publication remain deterministic and serialized. Station products are built
one physical session at a time; the manifest records total and maximum
per-session rollup rows, and the station-package write is a timed stage.
Full-project runtime varies with storage and system conditions; no runtime SLA
is currently claimed.

## Verification boundary

Conservative CU bounds are being added in stages under
[ADR-0020](decisions/0020-conservative-cu-missing-bin-bounds.md). Stage 1 adds
ordinary mean lower/upper fields to radio review CSVs and band/cohort/model/
classroom comparison CSVs, using the exact existing expected/valid durations.
The five canonical schemas and all observed calculations stay unchanged.
`cu_bounds_metric_id` identifies the additive calculation. Missing bins are
assigned 0%/100% for the two scenarios; these are not confidence intervals.
Stage 2 adds equal-session, available-zero-station and explicitly named
paired-window ranges to comparison CSVs, plus current/previous-window ranges
and exact components to the derived cadence-window Parquet. Observed means,
eligibility and correlations remain unchanged. Stage 3 adds observed/minimum/
maximum numeric CDF summaries, shaded CU CDFs and intervals in affected mean
figures. Saved-plot refresh uses exact compact counts/durations when present;
it also rebuilds the overall CU CDF. Stage 4 adds high/very-high station-time
quantity/percentage bounds to entity/population exports, the exposure chart
and the existing CU subsection. Bounds condition on available radio station
counts; they cannot bound periods with unknown counts. Observed percentages
retain paired station-time as their denominator. Stage 5 integrates every
HTML/text CU mean display, CDF metrics, station-time cards and supported
historical backfill into separate report bundles, preserving original fields
and labeling unsupported bounds unavailable. See the
[implementation inventory](cu_missing_bin_bounds_implementation_plan.md).

- Dedicated corrected semantic input and expected tables:
  `tests/fixtures/corrected_semantics/`
- Corrected golden contract: `tests/golden/corrected_semantics_v1.json`
- Additive ordinary mean-bound golden:
  `tests/golden/band_comparison_mean_bounds_v1.json`; original observed columns
  also remain checked against `tests/golden/band_comparison_v1.json`.
- Frozen explanatory comparison with the former calculation:
  `tests/golden/legacy_corrected_comparison_v1.json`
- Optional real-capture tests: marked `requires_external_tools`

The former workflow is not executed or maintained. Historical reproduction is
available from the public
[Original release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original).
