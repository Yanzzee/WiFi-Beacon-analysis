# Beacon Analysis

Beacon Analysis processes 802.11 beacon captures during scheduled class
sessions. It selects one representative beacon per BSSID and analysis bin,
resolves BSSIDs to radios/APs/classrooms, and reports channel utilization,
coverage, missingness, and high-utilization streaks.

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

`OUTPUT_FOLDER` is the base application-output folder, so
`OUTPUT_FOLDER=Output` publishes projects beneath `Output/runs`.
`CORRECTED_OUTPUT_ROOT` and `--output-root` are exact roots for project run
folders; use `Output/runs` when overriding the default layout.

## Inputs

- `.pcapng` or `.pcap` captures under `CAPTURES_FOLDER`. New captures should
  use pcapng.
- `Sample_APs.csv`: unique AP hostname, classroom mapping, and optional model.
- `Sample_classrooms.csv`: classroom metadata.
- `Sample_schedule.csv`: class location, schedule, course, enrollment, and
  capacity.

The three CSVs default to `SAMPLE_FOLDER`. They are read directly; no manual
CSV-to-Parquet preparation step is required. Schedule rows sharing the same
classroom/start/end are combined into one physical session and their enrolled
counts are summed.

The reviewed capture boundary supports:

- Aruba ERM types 0, 3, and 6;
- native 802.11/radiotap captures;
- Aruba and Extreme AP-name information elements;
- 2.4, 5, and 6 GHz band/channel classification when the required evidence is
  present; and
- Wireshark/TShark 4.6.6 on the verified Windows workflow.

Beacons without a configured AP hostname are outside project scope and are
discarded early. AP-name support for additional vendors remains future work.

## Outputs

Each successful execution creates:

```text
Output/runs/<project>/<run-id>/
```

The five durable logical artifacts are:

- `project_identity_map.parquet`;
- `selected_observations/`;
- `radio_class_summary.parquet`;
- `classroom_class_summary.parquet`; and
- `manifest.json`.

Compact CSV exports are under `exports/csv/`. The bounded
`reports/cu_coverage_streak/` package contains a run summary, methodology/file
guide, review CSVs, and project-level plots. Parquet remains authoritative.

Artifacts are built in an owned hidden staging directory and published only
after the complete run succeeds. A failed or interrupted run does not publish
partial results. Ctrl+C cancels the run; rerunning starts a new run rather than
resuming or overwriting.

## Analysis semantics

- Class intervals and bins are half-open; terminal partial bins retain their
  actual duration.
- Preferred observation time is Aruba ERM time, otherwise frame epoch time.
- Reliable per-BSSID/session TSF assists ordering, duplicate reconciliation,
  and bin placement. Missing, frozen, reversing, or incompatible TSF falls
  back to observation time.
- Selection keeps the first observed beacon per BSSID/bin after source
  precedence and deterministic ties.
- Missing CU never means zero. Valid CU, beacon-with-missing-CU, and no-beacon
  time remain separate.
- High CU begins at raw QBSS CU 128; very high begins at 192.
- Streaks are calculated per stable radio before classroom maximums are taken.

The accepted decisions are under [`docs/decisions`](docs/decisions), and the
current implementation is described in
[`docs/current_pipeline.md`](docs/current_pipeline.md).

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

## Historical reproduction

The last refactored revision supporting the original published workflow and
golden fingerprint is Git commit
`4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb`. Check out that commit separately
if historical reproduction is required. The current repository does not ship
or maintain the former multi-script workflow.
