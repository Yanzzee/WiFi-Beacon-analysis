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

- Category: bug
- Evidence: `analyze_data.ipynb`, code cell 0, `append_to_parquet()` reads an existing AP sampled parquet file and writes `existing_table` plus the new table back to the same path. There is no stage-start cleanup or stale-output guard for `SAMPLED_FOLDER`, `Data/all_class_beacons.parquet`, or `Data/radio_summary.parquet`.
- Why it matters: Running the same stage twice in the same workspace can silently inflate row counts and downstream statistics. The current pytest harness uses a fresh temp workspace, so it will not catch this by default.
- Risk level: high
- Suggested fix scope: Add a small, explicit stage output policy: either fail fast when sampled/output files already exist, or clear only this stage's generated files at the start of the stage. Avoid changing analysis math.
- Suggested validation/test: Run the analysis-forward pipeline twice against the same prepared workspace and assert the second run either fails with a clear stale-output message or produces identical row counts and fingerprints.
- Safe to fix before notebook conversion: Yes, if implemented as an explicit output hygiene fix.
- Recommended order: 2

### 3. Empty sampled folder crashes `combine_all_class_beacons()`

- Category: bug
- Evidence: `analyze_data.ipynb`, code cell 0, `combine_all_class_beacons()` collects `glob.glob(directory + "/*.parquet")` and immediately calls `pd.concat([...])`. Pandas raises `ValueError: No objects to concatenate` when no sampled files are present.
- Why it matters: If no AP/session data matches the sample, if grouped file names do not match AP hostnames, or if `SAMPLED_FOLDER` is misconfigured, the stage fails with an indirect pandas error rather than a pipeline-specific explanation.
- Risk level: medium
- Suggested fix scope: Add a clear precondition check that raises a helpful error when no sampled parquet files exist. Do not synthesize empty outputs unless that behavior is explicitly chosen.
- Suggested validation/test: Use a fixture with valid sample parquet files but no matching grouped AP files, or a deliberately empty configured sampled folder, and assert the stage raises the clear error.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 3

### 4. `calculate_total_scount()` only handles radios 1 and 2

- Category: bug
- Evidence: `analyze_data.ipynb`, code cell 0, `calculate_total_scount()` builds transmitter lists only for `radio_number == 1` and `radio_number == 2`. The `else` path logs `"error - no radio"` but then continues toward `radio_transmitters.remove(current_ta)` without defining a valid transmitter list for that row. The same cell only creates radio summary rows for `filtered_radio_1_df` and `filtered_radio_2_df`.
- Why it matters: Any AP/session slice inferred as more than two radios can fail, use stale state, or silently omit radio 3+ from `radio_summary.parquet`.
- Risk level: high
- Suggested fix scope: For Step 5, prefer a small explicit guard that fails clearly when inferred `radio_number > 2`. Generalizing all per-radio calculations can wait for module extraction unless a dedicated prompt asks for it.
- Suggested validation/test: Add a tiny synthetic dataframe or fixture slice with three inferred radio groups and assert the current stage fails with a clear unsupported-radio message.
- Safe to fix before notebook conversion: Partially. A guard is safe; changing the analysis to support arbitrary radios is broader.
- Recommended order: 4

### 5. Radio-summary regression crashes when one radio-count group is empty

- Category: bug
- Evidence: `analyze_radio_summary.ipynb`, code cell 3, runs `linear_regression(df_radio_1[...])` and `linear_regression(df_radio_2[...])`; the notebook comment says this will error if either dataframe has no rows.
- Why it matters: A valid dataset containing only one-radio APs or only two-radio APs can fail during plotting/regression even after producing valid summary rows.
- Risk level: medium
- Suggested fix scope: Skip the missing group's regression and plot only available groups, while logging that the group was absent.
- Suggested validation/test: Build a minimal `Data/radio_summary.parquet` fixture where all rows have `radio_count == 1`, run `analyze_radio_summary.ipynb`, and assert it writes CSVs and plots without crashing.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 5

### 6. Radio-summary plots divide by `ssid_count` without guarding zero

- Category: bug
- Evidence: `analyze_radio_summary.ipynb`, code cells 1 through 6 repeatedly compute `beacons_per_second / ssid_count`. There is no filter for missing or zero `ssid_count`.
- Why it matters: A zero or missing SSID count can create `inf`, `NaN`, misleading plots, or statsmodels failures.
- Risk level: medium
- Suggested fix scope: Add a pre-plot filter or explicit validation requiring `ssid_count > 0`, and log excluded rows.
- Suggested validation/test: Use a small `radio_summary.parquet` with one row where `ssid_count` is zero and assert the notebook excludes it or fails with a clear validation error.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 6

### 7. `Samples.py` crashes on schedules with fewer than 11 rows

- Category: bug
- Evidence: `Samples.py`, debug section after cleaning, unconditionally executes `first_record = sample_schedule_df.iloc[10]`; the value is only used by commented debug printing.
- Why it matters: Small sample fixtures or reduced regression fixtures with fewer than 11 schedule rows fail even though the row access is not part of required processing.
- Risk level: low
- Suggested fix scope: Remove the unused debug row access or guard it behind a length check.
- Suggested validation/test: Run `Samples.py` against a minimal schedule CSV with one valid class row and assert the three sample parquet files are created.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 7

### 8. `Samples.py` casts dirty numeric fields before cell cleanup

- Category: bug
- Evidence: `Samples.py` strips schedule column names, then immediately calls `sample_schedule_df.astype({... "Size": "int", "Enrl": "int" ...})`. The `clean_cell()` pass that removes non-breaking spaces and control characters runs only after the cast.
- Why it matters: The pipeline documentation notes dirty/non-breaking-space CSV inputs, but dirty numeric `Size` or `Enrl` values can fail before cleanup runs.
- Risk level: medium
- Suggested fix scope: Clean cells before strict dtype conversion, then cast numeric columns. Keep output column names and parquet schemas unchanged.
- Suggested validation/test: Add a focused sample CSV fixture where `Enrl` or `Size` contains whitespace/non-breaking-space formatting and assert `sample_classes.parquet` has integer `enrolled`.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 8

### 9. `process_pcapng.py` does not check `tshark` exit status

- Category: dependency issue
- Evidence: `process_pcapng.py`, `process_pcapng()`, starts `subprocess.Popen(tshark_command, stdout=subprocess.PIPE, text=True)` and passes stdout to `pd.read_csv()`, but never inspects `proc.returncode` or stderr after the process exits.
- Why it matters: A missing `tshark`, invalid decode option, bad capture, or partial failure can surface as a confusing pandas/parquet error or, worse, write incomplete converted data.
- Risk level: medium
- Suggested fix scope: Capture/check subprocess failure and raise a clear conversion error that includes the capture path and relevant `tshark` stderr. This should be a capture-conversion-specific prompt because repo guidance says not to modify `process_pcapng.py` unless conversion is in scope.
- Suggested validation/test: Mock or run a failing `tshark` command and assert `process_pcapng()` raises a clear error instead of continuing to parquet writing.
- Safe to fix before notebook conversion: Yes, but only under an explicit capture-conversion bug prompt.
- Recommended order: 9

## Confirmed Documentation, Dependency, And Obsolete-File Issues

### 10. Capture scripts produce `.pcap`, but conversion only discovers `.pcapng`

- Category: documentation mismatch
- Evidence: `beacon_capture.sh`, `beacon_capture_direct.sh`, `beacon_capture_band.sh`, and `capture_direct.sh` write `.pcap` files. `process_pcapng.py`, `process_all_pcapngs()`, only includes files where `f.endswith(".pcapng")`. README and `docs/current_pipeline.md` mention pcap and pcapng captures in different places.
- Why it matters: A user following the capture scripts can move `.pcap` files into `Data/captures` and have the conversion stage report zero files.
- Risk level: medium
- Suggested fix scope: Either document that captures must be converted/renamed to `.pcapng`, or extend discovery to include `.pcap` if `tshark` handling is the same. Keep this as a conversion-stage prompt.
- Suggested validation/test: Put one `.pcap` and one `.pcapng` fixture path in a temporary capture folder and test that file discovery behavior matches the documented policy.
- Safe to fix before notebook conversion: Yes for documentation; code behavior should be fixed only with an explicit conversion prompt.
- Recommended order: 10

### 11. README lists stale script names

- Category: documentation mismatch
- Evidence: `README.md` lists `group_data.py` and `samples.py`; the repository contains `group_data_bssid.py`, `group_data_aruba.py`, `group_data_old.py`, and `Samples.py`. `docs/current_pipeline.md` already records this mismatch.
- Why it matters: A new runbook or future extraction prompt can start from the wrong file names.
- Risk level: low
- Suggested fix scope: Update README command names to match the current operational scripts and note which grouping script is preferred.
- Suggested validation/test: Documentation-only check that every script named in README exists, except files explicitly labeled historical/obsolete.
- Safe to fix before notebook conversion: Yes.
- Recommended order: 11

### 12. `analyze_data.py` is obsolete but remains runnable-looking

- Category: deprecated/obsolete file
- Evidence: `analyze_data.py` top comments say the `.py` version has a fatal bug and is outdated compared with `analyze_data.ipynb`. It also contains older hardcoded paths such as `Output/all_class_beacons.parquet`, while the notebook now uses configured `DATA_FOLDER` for those outputs.
- Why it matters: During mechanical notebook conversion, this file can be mistaken for the conversion target or used as a stale reference.
- Risk level: medium
- Suggested fix scope: Do not repair this file in Step 5. Add a clearer obsolete-file notice or move it out of the operational path only after deciding how notebook conversion will create replacement modules.
- Suggested validation/test: Confirm tests and docs point at `analyze_data.ipynb` as the source of truth until conversion creates a new module.
- Safe to fix before notebook conversion: Documentation notice only is safe; deleting or rewriting is not recommended before conversion.
- Recommended order: 12

### 13. `BEACON_RATE` comment and value disagree

- Category: behavior-risk
- Evidence: `config.env` says `#Default beacon rate is 0.1024` and then sets `BEACON_RATE=1.024`. `analyze_data.ipynb` uses `BEACON_RATE` for deduplication and high-CU duration, and `analyze_class_sessions.ipynb` uses it as `BEACON_WINDOW_SEC`.
- Why it matters: The value materially affects row retention and duration metrics, but the code and current fixture behavior may intentionally rely on `1.024`.
- Risk level: high
- Suggested fix scope: Do not change the value as a Step 5 bug fix. First document the ambiguity and pin current behavior with the golden fingerprint test. Only change behavior if a separate analysis decision proves the intended interval.
- Suggested validation/test: Keep `BEACON_RATE` in the test fingerprint config and require review of fingerprint changes if this value or its interpretation changes.
- Safe to fix before notebook conversion: Safe only as documentation/test clarification. Behavior change is not safe yet.
- Recommended order: 13

## Suspected Fragilities And Behavior Risks

### 14. Native `--no-erm` conversion output is incompatible with notebooks

- Category: fragility
- Evidence: `process_pcapng.py` native mode requests `frame.time_epoch` and omits `wlan.vs.aruba.ap_name`. `analyze_data.ipynb` expects `aruba_erm.time`, loads grouped files by AP hostname, and validates `wlan.vs.aruba.ap_name` uniqueness.
- Why it matters: The CLI advertises non-Aruba/native conversion, but the analysis path is still Aruba-centered.
- Risk level: high
- Suggested fix scope: Treat vendor-neutral analysis as out of scope for Step 5. A small safe step is to document that `--no-erm` conversion does not currently feed the notebooks without additional mapping/adaptation.
- Suggested validation/test: Add a schema compatibility check that fails clearly when required notebook columns are absent.
- Safe to fix before notebook conversion: Documentation or explicit validation is safe; full native support is not.
- Recommended order: 14

### 15. Grouped-data cleanup and analysis use different timezone assumptions

- Category: behavior-risk
- Evidence: `group_data_cleanup.py` localizes naive `aruba_erm.time` values to `PLOT_TIMEZONE`. `analyze_data.ipynb`, code cell 0, localizes naive beacon timestamps to UTC and then converts to `TIMEZONE`. `Samples.py` localizes class sessions to `TIMEZONE`.
- Why it matters: The same grouped parquet can land at different wall-clock times depending on whether cleanup was run first and whether timestamps were naive or timezone-aware.
- Risk level: high
- Suggested fix scope: Do not change timestamp semantics without a dedicated timezone fixture. First define the canonical timezone contract for grouped parquet inputs.
- Suggested validation/test: Build a small grouped parquet fixture with known naive and timezone-aware timestamps, run cleanup plus `analyze_data`, and assert expected class-session inclusion.
- Safe to fix before notebook conversion: No, except for adding validation/documentation.
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
