# Beacon Analysis

Beacon Analysis processes 802.11 beacon captures during scheduled class
sessions. It selects one representative beacon per BSSID and analysis bin,
resolves BSSIDs to radios/APs/classrooms, and reports channel utilization,
coverage, missingness, and high-utilization streaks.

Alpha 0.1 remains the published release. The current checkout contains the
[Alpha 0.2 reporting candidate](docs/alpha-0.2-release-notes.md); candidate
preparation does not mean it has been tagged or published.

See the [documentation guide](docs/README.md) for current references, accepted
decisions and dated validation records. The [reporting and publication guide](docs/reporting_and_publication.md)
describes the current web report, saved-run refresh and public-hosting plan.

## Supported workflow

`beacon_analysis.py run` is the supported entry point. One command reads the
captures and three source CSVs, completes the analysis, and atomically
publishes a unique run directory:

```powershell
.\.venv\Scripts\python.exe beacon_analysis.py run --project "Fall 2026"
```

The command prints the resolved plan and asks for confirmation. `--yes` skips
that confirmation. `--non-interactive` prohibits prompts and fails if required
configuration is missing.

Configuration is resolved in this order:

1. command-line flags;
2. caller environment;
3. `config.env` (or `--env-file`);
4. an interactive prompt for unresolved required values.

Important settings are `PROJECT_NAME`, `CAPTURES_FOLDER`, `OUTPUT_FOLDER` or
`CORRECTED_OUTPUT_ROOT`, `SAMPLE_FOLDER`, `START_DATE`, `END_DATE`, `TIMEZONE`,
`TSHARK_PATH`, and `CAPTURE_WORKERS`. Run `beacon_analysis.py run --help` for
the corresponding flags.

`TIMEZONE` is the named IANA location timezone used to interpret schedule wall
times and display local report times. Use a DST-aware name such as
`America/Denver`, not a fixed offset such as `Etc/GMT+7`. Once the project
location is configured, date-specific standard/daylight offsets are applied
automatically and do not depend on the computer's timezone when the run occurs.

`CU_PRIMARY_SSID` defaults to `eduroam` and gives that SSID first preference
when selecting one CU value per physical radio/bin; another same-radio BSSID
may supply a fallback value without creating a duplicate reading.
`RADIO_CLASS_PLOTS` defaults to `false`. Set it to `true`, or pass
`--radio-class-plots`, to generate the opt-in CU/station time-series collection.

Exploratory capture-quality diagnostics assume a transmitted interval of
100 TU (0.1024 seconds). `BEACON_INTERVAL_TU` or `--beacon-interval-tu` overrides
only that diagnostic expectation, not CU, station, or accepted TSF calculations.
ERM is assessed at one effective observation per ten nominal intervals;
native capture slots use capture timestamps. TSF is diagnostic and never
moves measurements or removes beacons for falling outside mapped class time.
Empty capture slots are not
confirmed over-the-air losses.

`OUTPUT_FOLDER` is the base application-output folder, so
`OUTPUT_FOLDER=output` publishes projects beneath `output/runs`.
`CORRECTED_OUTPUT_ROOT` and `--output-root` are exact roots for project run
folders; use `output/runs` when overriding the default layout.

### Refresh a report from a completed run

To apply current HTML wording, layout and metric displays to saved results:

```powershell
.\.venv\Scripts\python.exe beacon_analysis.py report --from-run "output/runs/2025-full/20261029T213233082897Z-3152b7cd"
```

The command prints the new `reports/index.html` path. Each invocation creates
a separate bundle under
`output/reports/<project>/<source-run-id>/<report-id>/`; `report --output-root`
changes that root. The bundle copies the saved tables, graphics and source
manifest, then rebuilds the HTML overview. It works without captures, sample
files, TShark or changes to `config.env`. The source run is preserved.

This refresh applies HTML/report-text changes and displays metrics already
available in the saved tables. It redraws the radio/class-session mean CU
histogram, CU histograms/CDFs by band, CU coverage histograms, and the cohort,
matched-AP, station-adoption and station-time exposure figures from saved CSV
tables. It also redraws
the classroom/bin station histograms and admission-capacity histograms by band
from saved rollups, applying current labels and shared vertical scales.
Parquet reads use bounded batches and only the required columns. The command
reuses the remaining graphics and does not repeat capture analysis or recompute
radio/bin metrics. The HTML can summarize compact saved results, such as the
equally weighted mean signed CU difference across matched radio-pair/class-session
comparisons. Keep the new bundle together for
relative links to work. `report_refresh.json` records the source run, manifest
hash, renderer versions, refreshed figures and generation time; the copied `manifest.json`
continues to describe the original analysis.

## Inputs

The top-level storage folders are `data/`, `output/`, and `sample/`.
Only those folder names are lowercase; existing nested project names and CSV
filenames such as `Sample_APs.csv` are unchanged. Capture utilities live in
`capture_scripts/`.

- `.pcapng` or `.pcap` captures under `CAPTURES_FOLDER`. New captures should
  use pcapng.
- `Sample_APs.csv`: unique AP hostname, classroom mapping, and optional model.
- `Sample_classrooms.csv`: the locations included in the project, plus classroom metadata.
- `Sample_schedule.csv`: class location, schedule, course, enrollment, and
  capacity.

`Sample_APs.csv` may include `6GHz_Enabled` (`true`/`false`, `yes`/`no`, `1`/`0`,
or blank/`auto`). When unset, an AP is considered 6 GHz enabled if 6 GHz was
observed anywhere in the project. Explicit true without captured 6 GHz remains
enabled-but-unobserved; explicit false contradicting observed 6 GHz is rejected.
This AP flag does not override capture-derived radio band identity.

For example, add the optional column without changing the other inputs:

```csv
Hostname,Building-Room,Model,6GHz_Enabled
ROOM-101-AP1,ROOM-101,AP-635,true
ROOM-101-AP2,ROOM-101,AP-515,auto
```

The three CSVs default to `SAMPLE_FOLDER`. They are read directly; no manual
CSV-to-Parquet preparation step is required. Schedule rows sharing the same
classroom/start/end are combined into one physical session and their enrolled
counts are summed.

`Sample_classrooms.csv` defines the location scope. Only schedule rows whose
`Location` is listed there and AP mappings whose `Building-Room` is listed
there participate in the run. Both files may contain additional locations;
those rows are excluded before schedule validation and expansion. Location
matching is exact after trimming whitespace and cleaning non-breaking spaces.
Each listed location must have an AP mapping; missing mappings fail preflight.
A mapped AP without captured beacons produces a warning and unavailable
measurements when other project APs have observations. A run with no matching
configured AP observations at all still fails with an input error.
For an HBLL-only project, list only `HBLL-3710` in
`Sample_classrooms.csv`; you can keep the full schedule and AP list.

The reviewed capture boundary supports:

- Aruba ERM types 0, 3, and 6;
- native 802.11/radiotap captures;
- Aruba and Extreme AP-name information elements;
- 2.4, 5, and 6 GHz band/channel classification when the required evidence is
  present; and
- Wireshark/TShark 4.6.6 on the verified Windows workflow.

For Linux over-the-air acquisition, use
[`capture_scripts/beacon_capture_direct.sh`](capture_scripts/beacon_capture_direct.sh); it accepts
channel numbers or primary-channel center frequencies and makes completed
captures readable by all users. See [capture-script usage and review](docs/capture_scripts.md).

Beacons without a configured AP hostname are outside project scope and are
discarded early. AP-name support for additional vendors remains future work.

## Outputs

Each successful execution creates:

```text
output/runs/<project>/<run-id>/
```

The five durable logical artifacts are:

- `project_identity_map.parquet`;
- `selected_observations/`;
- `radio_class_summary.parquet`;
- `classroom_class_summary.parquet`; and
- `manifest.json`.

Compact CSV exports are under `exports/csv/`. The bounded
`reports/cu_coverage_streak/` package contains a methodology/file
guide, review CSVs, and project-level plots. The bounded
`reports/radio_class_detail/radio_bin_detail.parquet` view combines one CU
state/value and the accepted station total per physical radio/bin. The same
package includes overall/per-band CU-station density plots and descriptive
correlations. `reports/station_count/` adds complete classroom/bin and per-band
station distributions, while `reports/class_sessions/` joins accepted CU,
station, enrollment, and schedule context. The large radio/class plot
collection remains disabled by default. Parquet remains authoritative.
`reports/admission_capacity/` describes radio QoS admission allowance.
`reports/beacon_quality/` adds bounded, source-family-separated cadence tables,
capture-slot gaps, and exploratory associations with previous-window CU and
station count. It does not estimate missing CU or correct AP-model loss.
Start review at `reports/index.html`, the offline run overview printed by the
CLI after publication. It links headline values and coverage, band comparisons,
classroom/session detail, optional radio plots, capture quality, numeric tables
and definitions. Open it directly in a browser; no server or internet is needed.
Keep the run folder together so its relative links work. The plain-text summary
remains `reports/run_summary.txt`. Older published runs are not rewritten.
The overview includes analyzed observation dates, scheduled class-hours and
selected-row counts; enrollment-weighted peak clients/enrolled; radio station
means; and existing Pearson/Spearman CU-station relationships with coverage.
It progresses through Overview, Channel utilization (including station-time
with high CU), Stations & enrollment, CU/station, Admission capacity, Band
comparison, Capture quality, Class sessions and Evidence. A small inline script
highlights the section in view; native links work without it. The menu fits its
content on desktop and wraps on small screens. Full-width figures include brief
explanations and remain clickable; no regression R² is implied.

Band comparison includes distinct AP/radio/classroom/class-session cohort
counts, a compact expandable AP-model/radio-configuration table, and matched
5/6 GHz CU differences. Each classroom has a pooled band-summary table above
its class-session rows; AP/radio/seat context belongs to the classroom heading,
and optional radio plot links are listed one per line. The web report labels
the enrollment-weighted result **Peak stations per enrolled**. Stored field and
file names retaining `client` or `average` are unchanged.

Classroom high-CU association peak tables and their CDF are deliberately absent
from the web report and its inventories. The underlying ADR-0016 CSV/PNG outputs
remain generated; station-time exposure and pair coverage are displayed under
Channel utilization. CU and admission-capacity means in band/model/classroom
tables use valid duration; the standalone admission-capacity mean gives each
valid radio/bin reading equal weight. Cadence descriptions use the run's saved
nominal interval, ERM expectation and analysis-bin widths.

Enrollment-based ratios use the combined student enrollment supplied in the
schedule; they do not add an instructor to the denominator. Station counts can
include instructor, staff and other associations on the sampled networks. The
analysis does not identify those stations' roles.

`reports/band_comparison/` is the detailed 5/6 GHz comparison package. It records
overall and per-band/cohort CU, coverage, station load, QoS capacity, infrastructure
enablement, and actual 6 GHz station-use shares. AP and classroom cohorts remain
separate, mixed rooms are flagged, and 2.4 GHz participates when sampled.
Matched AP/session comparisons use common valid CU bins. Stable CSVs and context
support manual year-over-year review; they do not measure device capability or
establish causal performance effects. See [ADR-0015](docs/decisions/0015-band-comparison-and-longitudinal-reporting.md).

Artifacts are built in an owned hidden staging directory and published only
after the complete run succeeds. A failed or interrupted run does not publish
partial results. Ctrl+C cancels the run; rerunning starts a new run rather than
resuming or overwriting.

## Analysis semantics

- Class intervals and bins are half-open; terminal partial bins retain their
  actual duration.
- Preferred observation time is Aruba ERM time, otherwise frame epoch time.
- Observation time controls ordering and bin placement. Native TSF is checked
  independently of ERM and supports diagnostics and conservative duplicate
  recognition only. Clock anomalies retain capture/frame references in the
  cadence table and produce run-log and report warnings.
- Selection keeps the first observed beacon per BSSID/bin after source
  precedence and deterministic ties.
- Missing CU never means zero. Valid CU, beacon-with-missing-CU, and no-beacon
  time remain separate.
- High CU begins at raw QBSS CU 128; very high begins at 192.
- Streaks are calculated per stable radio before classroom maximums are taken.

The accepted decisions are under [`docs/decisions`](docs/decisions), and the
current implementation is described in
[`docs/current_pipeline.md`](docs/current_pipeline.md).

New analyses use `beacon.metric/corrected-cu/v2` and capture-quality v2 to
distinguish observation-time timing from earlier TSF-derived results. Saved
v1 runs are unchanged; a report-only refresh cannot repair their selections.
Affected projects need a fresh analysis after this timing change. See
[the timing decision](docs/decisions/0019-observation-time-and-native-tsf-diagnostics.md).

## Performance

Capture extraction is bounded and concurrent. One worker is the conservative
fallback; four is the current measured recommendation. A full 80-capture,
49.37-GiB project has shown substantial run-to-run timing variation, so no
formal runtime guarantee is currently claimed.

## Tests

The primary calculation oracle is the dedicated corrected fixture under
`tests/fixtures/corrected_semantics`, with its golden contract in
`tests/golden/corrected_semantics_v1.json`.

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Tests marked `requires_external_tools` need local captures and TShark. The
frozen comparison in `tests/golden/legacy_corrected_comparison_v1.json`
documents intentional differences from the former calculation using the same
small input, but legacy code is not executed in the supported test suite.

To select external/short-capture tests and override the saved reference folder:

```powershell
$env:TSHARK_PATH = 'C:/path/to/tshark.exe'
.\.venv\Scripts\python.exe -m pytest tests -q -m "requires_external_tools or pre_analysis_short" --short-processed-dir 'path/to/known-good-parquets'
```

The explicit `tests` path loads its custom fixture-folder options. Saved
known-good references are external test inputs, not regenerated by this command.

## Historical reproduction

The last refactored version supporting the original published workflow and
golden fingerprint is the public
[Original release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original).
Use that release separately if historical reproduction is required. The
current repository does not ship or maintain the former multi-script workflow.
