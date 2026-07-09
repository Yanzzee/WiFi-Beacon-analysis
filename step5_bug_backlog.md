# Step 5 Bug Backlog

This backlog is based on `docs/current_pipeline.md` plus a spot check of the current scripts, notebooks, README, config, and analysis-forward test harness. It is intentionally scoped to bugs and fragilities worth considering before mechanical notebook conversion and module extraction.

No source behavior changes are proposed here as broad refactors. Items marked unsafe before conversion should be treated as design decisions or guarded with explicit tests before changing analysis behavior.

## Confirmed Bugs

### 1. Hardcoded sampled output path ignores `SAMPLED_FOLDER`

- Status: fixed
- Category: bug
- Evidence: Original bug was in `analyze_data.ipynb`, code cell 0, where `append_to_parquet()` built `ap_beacons_file = f"Data/sampled/{AP_name}_class_beacons.parquet"` even though the notebook reads `SAMPLED_FOLDER` from `config.env` and `combine_all_class_beacons()` reads from `SAMPLED_FOLDER`.
- Why it matters: A non-default `SAMPLED_FOLDER` writes AP sampled files to one folder and then combines from another. That can produce missing outputs, stale outputs, or a `pd.concat` failure.
- Risk level: high
- Suggested fix scope: Replace only the hardcoded sampled write path with the configured `SAMPLED_FOLDER`; do not change filtering, metrics, schemas, or plot logic.
- Suggested validation/test: In an analysis fixture copy, set `SAMPLED_FOLDER` to a non-default folder, run the `analyze_data.ipynb` stage, and assert AP sampled files and `Data/all_class_beacons.parquet` are produced from the configured folder.
- Safe to fix before notebook conversion: Yes. This is a narrow config-path bug.
- Recommended order: 1
- Files changed: `analyze_data.ipynb`, `tests/test_notebook_config_paths.py`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_notebook_config_paths.py` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_analysis_forward.py --analysis-timeout 1800` passed.
- Remaining caveats: The first golden regression attempt with the default `--analysis-timeout 600` timed out in `analyze_all_class_beacons.ipynb`; rerunning the same regression with a larger per-notebook timeout passed. Pytest emitted a cache warning because it could not write `.pytest_cache` entries.

### 2. Rerunning `analyze_data.ipynb` appends duplicate sampled rows

- Status: fixed
- Category: bug
- Evidence: Original bug was in `analyze_data.ipynb`, code cell 0, where `append_to_parquet()` read an existing AP sampled parquet file and wrote `existing_table` plus the new table back to the same path. There was no stage-start cleanup or stale-output guard for `SAMPLED_FOLDER`, `Data/all_class_beacons.parquet`, or `Data/radio_summary.parquet`.
- Why it matters: Running the same stage twice in the same workspace can silently inflate row counts and downstream statistics. The current pytest harness uses a fresh temp workspace, so it will not catch this by default.
- Risk level: high
- Suggested fix scope: Add a small, explicit stage output policy: either fail fast when sampled/output files already exist, or clear only this stage's generated files at the start of the stage. Avoid changing analysis math.
- Suggested validation/test: Run the analysis-forward pipeline twice against the same prepared workspace and assert the second run either fails with a clear stale-output message or produces identical row counts and fingerprints.
- Safe to fix before notebook conversion: Yes, if implemented as an explicit output hygiene fix.
- Recommended order: 2
- Files changed: `analyze_data.ipynb`, `tests/test_notebook_config_paths.py`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_notebook_config_paths.py` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_analysis_forward.py --analysis-timeout 1800` passed.
- Remaining caveats: The fix uses fail-fast output hygiene rather than deleting generated files. It intentionally leaves normal within-run appends unchanged and does not address the empty sampled folder behavior tracked separately in item 3. Pytest emitted a cache warning because it could not write `.pytest_cache` entries.

### 3. Empty sampled folder crashes `combine_all_class_beacons()`

- Status: fixed
- Category: bug
- Evidence: Original bug was in `analyze_data.ipynb`, code cell 0, where `combine_all_class_beacons()` collected sampled parquet files and immediately called `pd.concat([...])`. Pandas raised `ValueError: No objects to concatenate` when no sampled files were present.
- Why it matters: If no AP/session data matches the sample, if grouped file names do not match AP hostnames, or if `SAMPLED_FOLDER` is misconfigured, the stage fails with an indirect pandas error rather than a pipeline-specific explanation.
- Risk level: medium
- Suggested fix scope: Add a clear precondition check that raises a helpful error when no sampled parquet files exist. Do not synthesize empty outputs unless that behavior is explicitly chosen.
- Suggested validation/test: Use a fixture with valid sample parquet files but no matching grouped AP files, or a deliberately empty configured sampled folder, and assert the stage raises the clear error.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 3
- Files changed: `analyze_data.ipynb`, `tests/test_notebook_config_paths.py`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_notebook_config_paths.py` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_analysis_forward.py --analysis-timeout 1800` passed.
- Remaining caveats: The fix raises a clear `FileNotFoundError` when no sampled parquet files exist; it does not synthesize empty outputs or change AP/session matching behavior. Pytest emitted a cache warning because it could not write `.pytest_cache` entries.

### 4. `calculate_total_scount()` only handles radios 1 and 2

- Status: fixed
- Category: bug
- Evidence: Original bug was in `analyze_data.ipynb`, code cell 0, where `calculate_total_scount()` built transmitter lists only for `radio_number == 1` and `radio_number == 2`. The same cell created radio summary rows only for hardcoded radio 1 and radio 2.
- Why it matters: Any AP/session slice inferred as more than two radios can fail, use stale state, or silently omit radio 3+ from `radio_summary.parquet`.
- Risk level: high
- Suggested fix scope: For Step 5, prefer a small explicit guard that fails clearly when inferred `radio_number > 2`. Generalizing all per-radio calculations can wait for module extraction unless a dedicated prompt asks for it.
- Suggested validation/test: Add a tiny synthetic dataframe or fixture slice with three inferred radio groups and assert the current stage fails with a clear unsupported-radio message.
- Safe to fix before notebook conversion: Partially. A guard is safe; changing the analysis to support arbitrary radios is broader.
- Recommended order: 4
- Files changed: `analyze_data.ipynb`, `tests/test_notebook_config_paths.py`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_notebook_config_paths.py` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_analysis_forward.py --analysis-timeout 1800` passed.
- Remaining caveats: The fix supports arbitrary observed `radio_number` groups within the existing MAC-gap grouping approach. It does not add band-based radio inference or change the existing nearest-transmitter search semantics beyond selecting transmitters from the current row's radio group. Pytest emitted the existing cache warning because it could not write `.pytest_cache` entries.

### 5. Radio-summary regression crashes when one radio-count group is empty

- Status: fixed
- Category: bug
- Evidence: Original bug was in `analyze_radio_summary.ipynb`, code cell 3, where regression ran unconditionally for `df_radio_1` and `df_radio_2`; the notebook comment noted this would error if either dataframe had no rows.
- Why it matters: A valid dataset containing only one-radio APs or only two-radio APs can fail during plotting/regression even after producing valid summary rows.
- Risk level: medium
- Suggested fix scope: Skip the missing group's regression and plot only available groups, while logging that the group was absent.
- Suggested validation/test: Build a minimal `Data/radio_summary.parquet` fixture where all rows have `radio_count == 1`, run `analyze_radio_summary.ipynb`, and assert it writes CSVs and plots without crashing.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 5
- Files changed: `analyze_radio_summary.ipynb`, `tests/test_radio_summary_notebook.py`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_radio_summary_notebook.py` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_analysis_forward.py --analysis-timeout 1800` passed.
- Remaining caveats: The fix skips and logs only missing radio-count regression groups. It does not change the upstream radio-count split, `ssid_count` division behavior, histograms, PDF/CDF plots, or any filtering thresholds. Pytest emitted the existing cache warning because it could not write `.pytest_cache` entries.

### 6. Radio-summary plots divide by `ssid_count` without guarding zero

- Status: fixed
- Category: bug
- Evidence: Original bug was in `analyze_radio_summary.ipynb`, code cells 1 through 6, where plotting repeatedly computed `beacons_per_second / ssid_count` without filtering missing or zero `ssid_count`.
- Why it matters: A zero or missing SSID count can create `inf`, `NaN`, misleading plots, or statsmodels failures.
- Risk level: medium
- Suggested fix scope: Add a pre-plot filter or explicit validation requiring `ssid_count > 0`, and log excluded rows.
- Suggested validation/test: Use a small `radio_summary.parquet` with one row where `ssid_count` is zero and assert the notebook excludes it or fails with a clear validation error.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 6
- Files changed: `analyze_radio_summary.ipynb`, `tests/test_radio_summary_notebook.py`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_radio_summary_notebook.py` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_analysis_forward.py --analysis-timeout 1800` passed.
- Remaining caveats: The fix excludes rows with missing, non-numeric, or non-positive `ssid_count` from `filtered_radio_summary_df` before downstream beacon-rate plots. It does not change the existing total-beacon threshold, class-duration filter, radio-count split, or plot formulas for retained rows. Pytest emitted the existing cache warning because it could not write `.pytest_cache` entries.

### 7. `samples.py` crashes on schedules with fewer than 11 rows

- Status: fixed
- Category: bug
- Evidence: `samples.py` previously had an unused debug access in the cleaning section, `first_record = sample_schedule_df.iloc[10]`, which could fail for schedules with fewer than 11 rows even though the value was not part of required processing.
- Why it matters: Small sample fixtures or reduced regression fixtures with fewer than 11 schedule rows could fail despite the row access being unrelated to the actual sample-generation workflow.
- Risk level: low
- Suggested fix scope: Remove the unused debug row access entirely rather than adding extra runtime branching for a value that is not used.
- Suggested validation/test: Run `samples.py` against a minimal schedule CSV with one valid class row and assert the three sample parquet files are created.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 7
- Files changed: `samples.py`, `tests/test_samples_script.py`, `step5_bug_backlog.md`.
- Validation run: `./.venv/Scripts/python.exe -m pytest tests/test_samples_script.py` passed.

### 8. `samples.py` casts dirty numeric fields before cell cleanup

- Status: fixed
- Category: bug
- Evidence: `samples.py` strips schedule column names, then immediately calls `sample_schedule_df.astype({... "Size": "int", "Enrl": "int" ...})`. The `clean_cell()` pass that removes non-breaking spaces and control characters runs only after the cast.
- Why it matters: The pipeline documentation notes dirty/non-breaking-space CSV inputs, but dirty numeric `Size` or `Enrl` values can fail before cleanup runs.
- Risk level: medium
- Suggested fix scope: Clean cells before strict dtype conversion, then cast numeric columns. Keep output column names and parquet schemas unchanged.
- Suggested validation/test: Add a focused sample CSV fixture where `Enrl` or `Size` contains whitespace/non-breaking-space formatting and assert `sample_classes.parquet` has integer `enrolled`.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 8
- Files changed: `samples.py`, `tests/test_samples_script.py`, `step5_bug_backlog.md`.
- Validation run: `./.venv/Scripts/python.exe -m pytest tests/test_samples_script.py` passed.

### 9. `process_pcapng.py` does not check `tshark` exit status

- Status: fixed
- Category: dependency issue
- Evidence: Original implementation used `subprocess.Popen(..., stdout=...)` and piped `proc.stdout` into `pd.read_csv()` without checking `proc.returncode` or `proc.stderr`. The file `process_pcapng.py` has been updated to:
	- resolve the `tshark` executable from `PATH` or the `TSHARK_PATH` env var,
	- invoke `tshark` with both `stdout` and `stderr` captured,
	- attempt to parse `tshark` CSV output into pandas and, on parse failure, kill the process and raise a `RuntimeError` including `tshark` stderr,
	- wait for the process to exit and raise a `RuntimeError` when `proc.returncode != 0` including the command and `stderr`.
- Why it matters: A missing `tshark`, invalid decode option, bad capture, or partial failure previously surfaced as confusing downstream pandas/parquet errors or produced incomplete converted data. The new checks make conversion failures explicit and easier to diagnose.
- Risk level: medium
- Suggested fix scope: The implemented change is focused and conservative: it adds explicit executable resolution (`TSHARK_PATH`), captures `stderr`, validates `tshark` exit codes, and raises descriptive errors. It does not alter downstream parquet schemas or analysis logic.
- Suggested validation/test: A new pytest `tests/test_process_pcapng_windows.py` was added that runs `process_pcapng()` against three known input captures in `Data/captures` and compares the produced parquet files to the existing, known-good files in `Data/processed_data`. The test passed locally (`1 passed`) when run on Windows with `tshark` available.
- Safe to fix before notebook conversion: Yes. This is an implementation-level robustness and error-reporting fix; it does not change analysis behavior.
- Files changed: `process_pcapng.py`, `tests/test_process_pcapng_windows.py`.
- Validation run: `py -3 -m pytest tests/test_process_pcapng_windows.py` — result: `1 passed` (warning: pytest cache write permission warning unrelated to conversion logic).
- Recommended order: 9

## Confirmed Documentation, Dependency, And Obsolete-File Issues

### 10. Capture scripts produce `.pcap`, but conversion only discovers `.pcapng`

- Status: fixed
- Category: documentation mismatch
- Evidence: Original behavior had `beacon_capture.sh`, `beacon_capture_direct.sh`, `beacon_capture_band.sh`, and `capture_direct.sh` writing `.pcap` names, while `process_pcapng.py`, `process_all_pcapngs()`, only included files where `f.endswith(".pcapng")`. README and `docs/current_pipeline.md` also mentioned pcap and pcapng captures in different places. The capture scripts now request `tshark -F pcapng` and write `.pcapng` extensions. Documentation now states that .pcapng is the required capture format.
- Why it matters: A user following the capture scripts can move `.pcap` files into `Data/captures` and have the conversion stage report zero files.
- Risk level: medium
- Suggested fix scope: Implemented as a conservative pcapng contract: all new captures should be .pcapng; conversion discovers `.pcapng` plus legacy `.pcap` filenames, validates magic bytes, warns and continues only when a `.pcap` file is actually pcapng, and raises a clear error for true pcap magic bytes. True pcap processing was reviewed but not built; supporting it would need separate schema/link-layer validation.
- Suggested validation/test: `tests/test_process_pcapng_windows.py` now includes magic-byte coverage for legacy `.pcap` filenames containing pcapng data, true pcap rejection, and discovery behavior. The existing baseline validation still processes:
	- `Beacon_Capture_20251110090000_00012.pcapng`
	- `Beacon_Capture_20251110100000_00013.pcapng`
	- `Beacon_Capture_20251110110000_00014.pcapng`
	and compares the generated parquet files to the existing known-good files in `Data/processed_data`.
- Safe to fix before notebook conversion: Yes; the code behavior change was made under an explicit conversion prompt and remains limited to capture-format validation/discovery.
- Files changed: `process_pcapng.py`, `tests/test_process_pcapng_windows.py`, `beacon_capture.sh`, `beacon_capture_direct.sh`, `beacon_capture_band.sh`, `capture_direct.sh`, `README.md`, `docs/current_pipeline.md`, `tests/fixtures/README.md`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_process_pcapng_windows.py -q` — result: `5 passed` (warning: pytest cache write permission warning unrelated to conversion logic).
- Recommended order: 10

### 11. README lists stale script names

- Status: fixed
- Category: documentation mismatch
- Evidence: Original README listed stale or inconsistent script names. README now names the current operational scripts, including `group_data_bssid.py`, `group_data_aruba.py`, `group_data_cleanup.py`, and lowercase `samples.py`; the historical grouping reference is archived under `old/group_data_old.py`.
- Why it matters: A new runbook or future extraction prompt can start from the wrong file names.
- Risk level: low
- Suggested fix scope: Update README command names to match the current operational scripts and note which grouping script is preferred.
- Suggested validation/test: Documentation-only check that every script named in README exists, except files explicitly labeled historical/obsolete.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 11
- Files changed: `README.md`, `docs/current_pipeline.md`, `step5_bug_backlog.md`.
- Validation run: Documentation-only grep confirmed stale `group_data.py` and inconsistent uppercase sample-script README references were removed; no code or tests changed.

### 12. `analyze_data.py` is obsolete but remains runnable-looking

- Status: fixed
- Category: deprecated/obsolete file
- Evidence: `analyze_data.py` top comments say the `.py` version has a fatal bug and is outdated compared with `analyze_data.ipynb`. It also contains older hardcoded paths such as `Output/all_class_beacons.parquet`, while the notebook now uses configured `DATA_FOLDER` for those outputs. The obsolete script has been moved to `old/analyze_data.py`, and the documentation now identifies `analyze_data.ipynb` as the operational source of truth.
- Why it matters: During mechanical notebook conversion, this file can be mistaken for the conversion target or used as a stale reference.
- Risk level: medium
- Suggested fix scope: Do not repair this file in Step 5. Move it out of the operational path and document the archived status until notebook conversion creates a replacement module.
- Suggested validation/test: Confirm docs point at `analyze_data.ipynb` as the source of truth until conversion creates a new module, and confirm archived obsolete files are referenced with `old/` paths.
- Safe to fix before notebook conversion: Documentation notice only is safe; deleting or rewriting is not recommended before conversion.
- Recommended order: 12
- Files changed: `README.md`, `docs/current_pipeline.md`, `config.env`, `old/README.md`, `step5_bug_backlog.md`.
- Validation run: Documentation-only grep confirmed active docs reference archived obsolete files with `old/` paths and use lowercase `samples.py`.

### 13. `BEACON_RATE` comment and value disagree

- Status: fixed
- Category: behavior-risk
- Evidence: `config.env` formerly said `#Default beacon rate is 0.1024` and then set `BEACON_RATE=1.024`. `analyze_data.ipynb` uses `BEACON_RATE` for deduplication and high-CU duration, and `analyze_class_sessions.ipynb` uses it as `BEACON_WINDOW_SEC`.
- Why it matters: The value materially affects row retention and duration metrics. IEEE 802.11 commonly defaults to a 0.1024 second beacon interval, but this application intentionally uses a 1.024 second effective metadata interval because most vendors update beacon metadata every 10 intervals, and Aruba ERM captures/forwards those metadata updates at the longer interval.
- Risk level: high
- Suggested fix scope: Do not change the value as a Step 5 bug fix. Document the IEEE interval versus the application effective interval and keep current behavior pinned with the golden fingerprint test.
- Suggested validation/test: Keep `BEACON_RATE` in the test fingerprint config and require review of fingerprint changes if this value or its interpretation changes.
- Safe to fix before notebook conversion: Yes, as documentation/comment clarification only. Behavior change is not safe without a separate analysis decision.
- Recommended order: 13
- Files changed: `config.env`, `README.md`, `docs/current_pipeline.md`, `analyze_data.ipynb`, `analyze_class_sessions.ipynb`, `step5_bug_backlog.md`.
- Validation run: Documentation-only grep confirmed the active docs and notebook comments now distinguish the IEEE 0.1024 second interval from the application's 1.024 second effective metadata interval; no analysis behavior or `config.env` values changed.
- Remaining caveats: Notebook fallback behavior for invalid `BEACON_RATE` values is unchanged. Any future value or semantic change should be paired with golden fingerprint review.

## Suspected Fragilities And Behavior Risks

### 14. Native `--no-erm` conversion output is incompatible with notebooks

- Status: fixed
- Category: fragility
- Evidence: `process_pcapng.py` native mode requests `frame.time_epoch` and omits `wlan.vs.aruba.ap_name`. `analyze_data.ipynb` expects `aruba_erm.time`, loads grouped files by AP hostname, and validates `wlan.vs.aruba.ap_name` uniqueness.
- Why it matters: The CLI advertises non-Aruba/native conversion, but the analysis path is still Aruba-centered.
- Risk level: high
- Suggested fix scope: Implemented as a narrow compatibility guard and documentation clarification. `analyze_data.ipynb` now validates grouped AP parquet schemas before analysis and raises a clear error when native `--no-erm` output with `frame.time_epoch` is detected. `process_pcapng.py`, README, and `docs/current_pipeline.md` now state that native/non-Aruba conversion is conversion-only for now and does not feed the notebooks without timestamp plus AP-name mapping/adaptation.
- Suggested validation/test: Added focused notebook-schema tests for Aruba-shaped grouped data and native `--no-erm` grouped data.
- Safe to fix before notebook conversion: Documentation or explicit validation is safe; full native support is not.
- Files changed: `analyze_data.ipynb`, `tests/test_notebook_config_paths.py`, `process_pcapng.py`, `README.md`, `docs/current_pipeline.md`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_notebook_config_paths.py -q` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_process_pcapng_windows.py -q` passed, including the known-good validation over the three capture test files.
- Remaining caveats: Full native/non-Aruba analysis support remains an important future feature, but it still needs a dedicated mapping/adaptation design after the current bug fixes and refactoring.
- Recommended order: 14

### 15. Grouped-data cleanup and analysis use different timezone assumptions

- Status: validation/documentation added; canonical timestamp normalization still deferred.
- Category: behavior-risk
- Evidence: `group_data_cleanup.py` localizes naive `aruba_erm.time` values to `PLOT_TIMEZONE`, but parses non-datetime values with `utc=True` first. `analyze_data.ipynb`, code cell 0, formerly localized naive beacon timestamps to UTC and then converted to `TIMEZONE`. `samples.py` localizes class sessions to `TIMEZONE`. Beacon inputs and class schedule inputs may arrive in varying timestamp/time formats, and that broader normalization problem should be revisited after conversion to native Python.
- Why it matters: The same grouped parquet can land at different wall-clock times depending on whether cleanup was run first, whether timestamps were strings or datetime dtype, and whether datetime values were naive or timezone-aware.
- Risk level: high
- Suggested fix scope: Do not change timestamp semantics without a dedicated timezone fixture and a canonical timezone contract. Implemented pre-conversion guard: `analyze_data.ipynb` now validates grouped AP input before analysis and fails fast unless `aruba_erm.time` is already a timezone-aware datetime dtype. Documentation now states that varied beacon timestamp strings, naive datetimes, and broader class schedule time parsing need a dedicated normalization design after notebook conversion.
- Suggested validation/test: Added focused schema tests for timezone-aware grouped input, timezone-naive grouped input, and string timestamp grouped input. Future semantic validation should build a small grouped parquet fixture with known naive and timezone-aware timestamps, run cleanup plus `analyze_data`, and assert expected class-session inclusion.
- Safe to fix before notebook conversion: No for semantic timestamp changes; yes for explicit validation and documentation.
- Files changed: `analyze_data.ipynb`, `tests/test_notebook_config_paths.py`, `README.md`, `docs/current_pipeline.md`, `step5_bug_backlog.md`.
- Validation run: `.\.venv\Scripts\python.exe -m pytest tests/test_notebook_config_paths.py -q` passed; `.\.venv\Scripts\python.exe -m pytest tests/test_analysis_forward.py --analysis-timeout 1800 -q` passed.
- Remaining caveats: The guard does not normalize varied timestamp formats. `group_data_cleanup.py` still uses `PLOT_TIMEZONE`, while class schedule processing and analysis use `TIMEZONE`; the final contract should be designed after notebook conversion.
- Recommended order: 15

### 16. SSID filters ignore `SSID1` and `SSID2`

- Category: fragility
- Evidence: `config.env` defines `SSID1=eduroam` and `SSID2=BYU-WiFi`. `analyze_all_class_beacons.ipynb`, code cell 2, hardcodes `["eduroam", "BYU-WiFi"]`. `analyze_class_sessions.ipynb`, code cell 1, hardcodes `filtered[filtered["wlan.ssid"] == "eduroam"]` for `number_radios`.
- Why it matters: Changing SSIDs in config will not change the analysis filters, producing empty or misleading metrics for other deployments.
- Risk level: medium
- Suggested fix scope: Read SSIDs from config with defaults matching current values, and keep the current default fixture output unchanged.
- Suggested validation/test: Run the existing golden fixture with default SSIDs and assert fingerprints are unchanged; add a tiny alternate-SSID fixture to assert configured SSIDs are honored.
- Safe to fix before notebook conversion: Yes, if default behavior is pinned.
- Recommended order: 16

### 17. BSSID fallback filenames do not match sample-driven analysis lookup

- Category: fragility
- Evidence: `group_data_bssid.py` falls back to `<BSSID>.parquet` when `wlan.vs.aruba.ap_name` is absent. `analyze_data.ipynb`, code cell 0, constructs grouped filenames from `Sample/sample_aps.parquet` `Hostname` values.
- Why it matters: Grouping succeeds but analysis later fails to find files unless sample hostnames are also BSSID filenames.
- Risk level: medium
- Suggested fix scope: Add a preflight check before analysis that reports missing grouped files for sample AP hostnames. A broader AP/BSSID mapping strategy is out of scope for Step 5.
- Suggested validation/test: Use a fixture where one sample hostname has no matching grouped parquet and assert the error lists the missing expected path.
- Safe to fix before notebook conversion: Yes for preflight validation; no for broad mapping changes.
- Recommended order: 17

### 18. High-CU streak logic may bridge empty time bins

- Category: behavior-risk
- Evidence: `analyze_class_sessions.ipynb`, code cell 1, updates `current_streak` only inside `if not bin_slice.empty:`. Empty bins do not reset the streak.
- Why it matters: Missing beacon intervals could be counted as continuous high-CU streaks across data gaps.
- Risk level: medium
- Suggested fix scope: Do not change this as a mechanical cleanup. First decide whether empty bins mean "no evidence" or "break continuity."
- Suggested validation/test: Create a tiny processed class-session dataframe with two high-CU bins separated by an empty bin and assert the intended max streak.
- Safe to fix before notebook conversion: No, not until intended missing-data semantics are confirmed.
- Recommended order: 18

### 19. Radio inference is Aruba/MAC-gap-specific

- Category: fragility
- Evidence: `analyze_data.ipynb`, code cell 0, derives `radio_number` by sorting `wlan.ta_int` and incrementing when adjacent MAC integers differ by more than 1. The notebook comment says channel numbers would probably be better but this works for Aruba.
- Why it matters: A different vendor, MAC allocation pattern, or missing transmitter can split or merge radios incorrectly and affect `total_scount`, plots, and radio summaries.
- Risk level: high
- Suggested fix scope: For Step 5, add explicit documentation or validation that this inference is Aruba-specific. Replacing radio inference should wait for module extraction or a dedicated analysis prompt.
- Suggested validation/test: Pin current inferred `radio_number` counts in the golden fingerprint test and add a small targeted fixture if a future channel-based implementation is selected.
- Safe to fix before notebook conversion: Documentation/validation only is safe; algorithm change is not.
- Recommended order: 19

## Next Bug To Fix First

Fix item 1, the hardcoded `Data/sampled` path in `analyze_data.ipynb`, first. It is narrow, clearly verified against the code, does not require changing analysis math, and makes later output hygiene tests more reliable because the stage will consistently honor its configured sampled-output folder.
