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
9. Process one complete class-session partition at a time: apply per-BSSID TSF
   handling, duplicate reconciliation, bin placement, and first-observed
   selection.
10. Calculate corrected radio/class and classroom/class CU, coverage,
    missingness, and streak summaries.
11. Write the five durable artifacts, compact CSV exports, and the bounded
    report package inside lifecycle-owned staging.
12. Validate the manifest and atomically publish the completed run directory.

No processed, grouped, sampled, or legacy analysis working directories are
part of this flow.

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

Individual CSVs and all major settings can be overridden by command-line
flags. One capture worker is the built-in fallback; four is the current
measured recommendation. The accepted range is 1-16.

`OUTPUT_FOLDER` names the base application-output folder and the CLI appends
`runs`. `CORRECTED_OUTPUT_ROOT` and `--output-root` name the exact directory
that contains project folders. The canonical explicit override is therefore
`--output-root Output/runs`, not `--output-root Output`.

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
- `Sample_classrooms.csv`: classroom/location and capacity metadata.
- `Sample_schedule.csv`: location, course, class period, meeting days,
  enrollment, capacity, and schedule-date applicability.

The CSVs are loaded directly into memory. Source schedule sections with the
same classroom/start/end become one physical class session; enrollment is the
sum of the source sections. The normalization is shown in terminal preflight
and retained in the log, manifest, and run summary.

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

## Identity

Configured hostname determines AP and classroom membership. Within one AP,
band plus current channel separates radios; a channel number is never used
without band because 2.4, 5, and 6 GHz channel numbers overlap. Different APs
on the same band/channel remain distinct. BSSID MAC-pattern grouping is not
used when AP name is available.

Channel changes are retained as observed evidence. A BSSID maps to one radio
group, a radio group to one AP, and an AP to one classroom for a project run.

## Timing, binning, and selection

Class admission first uses preferred observation time: valid Aruba ERM time,
otherwise frame epoch time. Observations outside the scheduled class interval
by that time are excluded.

For each BSSID/class session, reliable advancing TSF uses the earliest valid
`observation_time - TSF` origin candidate. TSF-derived time can then place a
frame in a different bin than its observation timestamp. TSF also orders
delayed frames, collapses same-BSSID/same-TSF duplicate views, and identifies
skipped intervals without synthesizing missing beacons.

Missing, frozen, reversing, or grossly time-incompatible TSF is ignored for
that BSSID/session, which then uses observation time throughout. TSF origins
are not pooled across BSSIDs, and clocks are not drift-fitted or repaired.

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

Station count remains available per selected BSSID observation. Reviewed
radio/SSID/classroom/band station rollups are planned but not yet part of the
summary contract.

## Output lifecycle

Successful runs publish below:

```text
Output/runs/<project-slug>/<UTC-timestamp>-<input-hash-prefix>/
```

The five durable logical artifacts are:

1. `project_identity_map.parquet`
2. `selected_observations/`
3. `radio_class_summary.parquet`
4. `classroom_class_summary.parquet`
5. `manifest.json`

Compact human-readable copies of identity and summary tables are under
`exports/csv/`. `reports/cu_coverage_streak/` contains `run_summary.txt`, a
comprehensive `methodology.txt`, three review CSVs, and eight bounded plots.

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
publication remain deterministic and serialized. Full-project runtime varies
with storage and system conditions; no runtime SLA is currently claimed.

## Verification boundary

- Dedicated corrected semantic input and expected tables:
  `tests/fixtures/corrected_semantics/`
- Corrected golden contract: `tests/golden/corrected_semantics_v1.json`
- Frozen explanatory comparison with the former calculation:
  `tests/golden/legacy_corrected_comparison_v1.json`
- Optional real-capture tests: marked `requires_external_tools`

The former workflow is not executed or maintained. Historical reproduction is
available by checking out commit
`4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb` separately.
