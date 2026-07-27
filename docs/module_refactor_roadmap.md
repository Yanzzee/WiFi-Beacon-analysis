# Module Refactor Roadmap

This document is the ordered plan for refactoring the converted Python scripts
into reusable modules. It describes behavior-preserving work only. Analysis
changes belong in `docs/post_refactor_design.md` and require separate review.

The active analysis entry points remain:

- `analyze_data.py`
- `analyze_all_class_beacons.py`
- `analyze_radio_summary.py`
- `analyze_class_sessions.py`

The sample and grouping entry points remain `samples.py`,
`group_data_bssid.py`, `group_data_aruba.py`, and `group_data_cleanup.py`.
`process_pcapng.py` is outside this roadmap unless a task explicitly includes
capture conversion.

## Current Structure

- `analysis_config.py` is currently the only shared runtime module. It contains
  the three completed extractions below.
- `analyze_all_class_beacons.py`, `analyze_radio_summary.py`, and
  `analyze_class_sessions.py` each contain nearly their entire stage inside one
  large `main()` function, including configuration, I/O, calculation, plotting,
  and logging.
- `analyze_data.py` has several callable functions, but many depend on module
  globals for configuration, logging, accumulated dataframes, and output paths.
- The clearest remaining exact duplication is timestamped log-file setup and
  NumPy PDF/CDF calculation. These are ordered before stage-specific logic.
- Grouping behavior has little focused coverage and contains similar but
  non-identical filename policies. Tests must precede its extraction.
- The highest-risk blocks are room station-count enrichment, class-session
  streak calculation, beacon deduplication, Aruba MAC-gap radio inference, and
  per-radio summaries. They remain late in the sequence and require compact
  characterization fixtures before movement.

## Refactor Rules

Apply this roadmap incrementally. One numbered extraction should normally be
one pass. If a step reveals behavior that is not adequately characterized,
add the missing characterization test and stop before moving the logic.

Every pass must preserve:

- analysis math and dataframe row-selection behavior;
- output filenames, directories, schemas, and write order;
- plotting calls, parameters, filenames, and close/show behavior;
- timestamp parsing, timezone conversion, and interval boundaries;
- `BEACON_RATE`, CU thresholds, filtering thresholds, and station-count windows;
- current Aruba-specific grouping and radio inference;
- public script variables, callable names, and standalone entry-point behavior;
- current exception types and useful error text.

Do not combine merely similar code unless its behavior is first shown to be
identical or its differences are represented explicitly. In particular,
grouping filename sanitizers, logging behavior, directory creation variants,
and plotting cleanup currently have meaningful differences.

For each pass:

1. Add or identify focused tests that pin the extracted behavior.
2. Move the smallest coherent unit and make only the required import/call-site
   updates.
3. Keep copied-module dependencies in `tests/pipeline_harness.py` current.
4. Run focused tests and the complete non-golden suite.
5. When an active analysis stage changes, run the golden regression without
   updating `tests/golden/fingerprints.json`. On the current Windows fixture,
   use `--analysis-timeout 3600` to avoid treating slow execution as a behavior
   failure.
6. Confirm `git diff --check` passes and `process_pcapng.py` is unchanged.

## Ordered Steps

### 1. Extract blank/default environment lookup — completed

- Added `env_or_default()` to `analysis_config.py`.
- Updated `analyze_all_class_beacons.py` and
  `analyze_class_sessions.py` to use it for SSID defaults.
- Preserved stripping, blank-value fallback, configured SSID behavior, and
  standalone execution.
- Completed in commit `736b975` (`refactor 1`).

### 2. Extract `BEACON_RATE` parsing — completed

- Added `get_beacon_rate()` to `analysis_config.py`.
- Updated `analyze_data.py` and `analyze_class_sessions.py`.
- Preserved valid `float()` conversion, the invalid-value message, the
  `0.1024` invalid-value fallback, and the existing missing-value `TypeError`.
- Added direct tests for all three branches.
- Completed in commit `a875b0a` (`refactor 2`).

### 3. Extract conditional directory creation — completed

- Added `ensure_directory()` to `analysis_config.py`.
- Updated `analyze_data.py`, `analyze_all_class_beacons.py`,
  `analyze_radio_summary.py`, `analyze_class_sessions.py`, and `samples.py`.
- Preserved the exact `exists`-then-`makedirs` behavior and call order.
- Left the grouping scripts' distinct `makedirs(..., exist_ok=True)` behavior
  unchanged.
- Added direct missing-directory and existing-directory tests.
- Completed in commit `914fd64` (`refactor 3`).

### 4. Extract analysis log-file opening — completed

- Added `open_analysis_log()` in `analysis_logging.py` for constructing and
  opening timestamped stage log files.
- Updated the four active analysis scripts and retained their local
  `timestamp`, `log_filename`, and `log_file` variables.
- Preserved the timestamp format `%Y-%m-%d_%H-%M`, forward-slash path
  construction, stage-specific filename stems, append mode, encoding default,
  and the point in execution at which each file is opened.
- Kept each script's existing `log_and_print()` implementation unchanged:
  `analyze_data.py` writes without printing, while the other stages print and
  write.
- Added focused tests for the exact filename, append mode, and append behavior.

### 5. Extract the shared PDF/CDF calculation — completed

- Added the pure `compute_pdf_cdf()` helper in `analysis_statistics.py`.
- Updated `analyze_all_class_beacons.py` and `analyze_radio_summary.py` while
  keeping their caller-specific bin counts explicit: 256 for channel
  utilization and 50 for beacon rate.
- Preserved `np.histogram(..., density=True)`, bin-center calculation,
  cumulative calculation, return order, dtype behavior, and empty/small-input
  behavior.
- Added fixed-array tests comparing every returned array for multi-value,
  single-value, and empty inputs. Plotting was not moved or changed.

### 6. Extract grouped-AP schema validation — completed

- Added `analysis_validation.py` containing `REQUIRED_GROUPED_AP_COLUMNS`,
  `_grouped_timestamp_series()`, `validate_grouped_ap_timestamps()`, and
  `validate_grouped_ap_schema()`.
- Preserved accepted index/column forms, pandas dtype checks, native-capture
  detection, validation order, and exception messages.
- Imported every moved name back into `analyze_data.py` under its existing name
  so current tests and callers remain valid.
- Confirmed the existing timezone-aware, timezone-naive, string-timestamp, and
  native-output characterization tests pass before and after the move.
(note, validation took 47 minutes to pass after making this change)

### 7. Extract analysis preflight checks — completed

- Moved `validate_radio_inference_platform()`,
  `validate_no_existing_analysis_outputs()`, and
  `validate_grouped_files_for_sample_aps()` into `analysis_validation.py`.
- Passed required paths and platform values explicitly inside the module while
  retaining compatibility wrappers with the existing names and signatures in
  `analyze_data.py`.
- Preserved validation order in `main()`, sampled-location scoping, sorted error
  path lists, the BSSID fallback explanation, and all failure types.
- Kept the Aruba-only platform requirement unchanged and did not introduce
  vendor inference.

### 8. Extract parquet persistence helpers — completed

- Added `analysis_io.py` and moved the mechanics of `append_to_parquet()` and
  `combine_all_class_beacons()` into it.
- Retained compatibility wrappers with the existing names and signatures in
  `analyze_data.py`, passing configured folders and the existing logger into
  the module.
- Preserved file discovery patterns and ordering, PyArrow schema creation,
  Snappy compression, index behavior, append/rewrite sequence, log messages,
  empty-data behavior, and output paths.
- Added small-parquet characterization tests before extraction for
  create/append ordering, exact messages, empty input, and glob ordering.
- Added the module to the golden-test workspace and confirmed the output
  fingerprints still match.

### 9. Extract pure sample-cleaning helpers — completed

- Added `sample_processing.py` containing `clean_cell()`,
  `parse_class_period()`, and `expand_schedule_rows()`.
- Extracted `clean_cell()` first, then added direct parsing and expansion tests
  before replacing the original schedule loop in `samples.py`.
- Preserved string coercion, non-breaking-space replacement,
  control-character removal, accepted lowercase `a`/`p` time strings,
  rejection and messages for malformed periods, weekday and input-row order,
  inclusive schedule dates, and null capacity placeholders.
- Left explicit schedule dtype conversion, timezone localization, capacity
  lookup, final sorting, output filenames, and standalone execution in
  `samples.py` for Step 10.
- Confirmed the focused sample tests, full non-golden suite, and analysis
  golden fingerprints pass.

### 10. Make `samples.py` a thin standalone entry point — completed

- Added callable `process_samples()` to `sample_processing.py` and moved the
  remaining CSV reads, dataframe cleaning/casts, date expansion setup,
  capacity lookup, timezone conversion, sorting, and parquet writes into it.
- Left `samples.py` responsible for loading configuration, validating required
  settings, ensuring the sample directory exists, and invoking
  `process_samples()`.
- Preserved all CSV and parquet filenames, output columns and dtypes, date and
  completion messages, missing-configuration failures, timezone behavior, and
  standalone exit behavior.
- Added direct callable coverage and expanded subprocess coverage for all three
  outputs, exact messages, dtypes, and configuration failures.
- Confirmed the focused tests and source-sample parquet golden fingerprints
  pass.

### 11. Characterize grouping behavior before modularizing it — completed

- Added `tests/test_grouping_scripts.py` with focused subprocess fixtures for
  `group_data_bssid.py`, `group_data_aruba.py`, and
  `group_data_cleanup.py`; none of the grouping implementations were changed.
- Pinned BSSID validation warnings, AP-name selection, BSSID fallback names,
  source-index preservation, partition append order, and the skipped `None`,
  empty, `nan`, `none`, and `<NA>` key forms.
- Pinned Aruba AP-name grouping across partitions, blank-name output, null-name
  omission, filename sanitization, and the current overwrite behavior when
  distinct AP names sanitize to the same output filename.
- Tested the two sanitizers independently, including their distinct trailing
  character stripping expressions, illegal-character replacement, fallback,
  and length limits.
- Pinned cleanup of an `aruba_erm.time` index, naive localization, aware
  conversion, string parsing/coercion, index removal on rewrite, skipped-file
  preservation, and same-path output behavior.
- Confirmed the focused tests and short processed-data grouping golden
  regression pass.

### 12. Extract grouping utilities and callable workflows — completed

- Added separate `grouping_bssid.py`, `grouping_aruba.py`, and
  `grouping_cleanup.py` modules; vendor grouping and cleanup policies remain
  independent.
- Moved BSSID sanitization and partition traversal into
  `group_data_by_bssid()`, preserving invalid-key handling, AP-name fallback,
  PyArrow schema checks, writer reuse/closure, append order, output names, and
  warnings.
- Moved Aruba sanitization and AP-name grouping into
  `group_data_by_aruba_name()`, preserving Dask computation, null/blank-name
  behavior, sanitized-name overwrites, output paths, and schema errors.
- Moved per-file timestamp cleanup into `fix_one()` and threaded orchestration
  into `cleanup_grouped_data()`, preserving file discovery, timezone rules,
  same-path rewrites, worker count, completion ordering, and status messages.
- Kept `group_data_bssid.py`, `group_data_aruba.py`, and
  `group_data_cleanup.py` as separate configuration-and-invocation entry
  points.
- Added the modules to isolated pre-analysis workspaces and confirmed the
  focused grouping contracts and short grouping golden regression pass.

### 13. Extract radio-summary dataframe preparation — completed

- Added `radio_summary_processing.py` with `sort_radio_summary()` and
  `prepare_radio_summary_frames()`.
- Moved sorting, class-duration calculation, the `total_beacons >= 300` and
  positive-duration filter, numeric positive-`ssid_count` filtering,
  exclusion logging, beacon-rate calculation, and radio-count splits into the
  module.
- Kept the sort and preparation calls on opposite sides of the existing raw
  CSV/report block so `radio_summary.csv` remains unchanged and the exclusion
  warning retains its execution order.
- Preserved source-frame non-mutation during sorting, explicit filtered-frame
  copies, numeric coercion, indices and row order, thresholds, columns, and
  warning text.
- Added complete-frame comparisons for the sorted, filtered, one-radio,
  two-radio, and combined outputs while retaining focused plot and regression
  coverage.
- Left all regression fitting and plotting in `analyze_radio_summary.py`.
- Confirmed the focused tests, full non-golden suite, and analysis-forward
  golden fingerprints pass.

### 14. Characterize class-session metric edge cases — completed

- Added `tests/test_class_session_metrics.py`, which executes the current
  initialization and metric cells without changing the production script.
- Pinned no-beacon session defaults and inclusive start/end filtering,
  including the current distinction that an end-boundary beacon contributes
  to session summaries but not the half-open duration bins.
- Pinned high/empty/high behavior, where an empty bin does not reset the
  current streak, and high/non-high/high behavior, where an observed non-high
  bin does reset it.
- Pinned the current location-wide `any()` rule using alternating high CU
  across two radios, along with unique-transmitter station sums and configured
  primary-SSID radio counting.
- Pinned missing-CU nullable summary values, zero high-CU durations/sums for
  missing CU, and zero `stations_enrolled` when enrollment is zero.
- Left the intended per-radio streak and missing-data redesigns in
  [the post-refactor design backlog](post_refactor_design.md#analysis-semantics);
  no analysis semantics were corrected in this step.
- Confirmed all focused tests and the full non-golden suite pass.

### 15. Extract class-session metric calculation — completed

- Added `class_session_analysis.py` with
  `calculate_class_session_metrics()` and the existing CU thresholds.
- Moved only the calculation for one class-session row, preserving inclusive
  session filtering, half-open time bins, threshold values `191` and `127`,
  duration/streak updates, unique-transmitter station sums, configured
  primary-SSID radio counting, missing-CU values, and zero/default behavior.
- Kept session iteration and dataframe assignments in their original order in
  `analyze_class_sessions.py`, so the existing nullable output dtypes remain
  controlled by the entry script.
- Left all aggregate reporting, plots, output paths, and logging in
  `analyze_class_sessions.py`.
- Added the new module to the isolated analysis-forward workspace and
  confirmed focused characterization tests, the full non-golden suite, and
  analysis-forward golden fingerprints pass.

### 16. Extract room station-count enrichment — completed

- Added `room_station_count.py` with
  `enrich_room_station_counts()`.
- Added a compact multi-AP characterization fixture before extraction,
  pinning nearest-observation selection, inclusive plus/minus five-second
  bounds, transmitter de-duplication, first-observation tie behavior, missing
  AP/location defaults, integer conversion, time sorting, index reset, output
  columns, source-frame non-mutation, and the pre-filter parquet output.
- Updated `analyze_all_class_beacons.py` to call the extracted function while
  keeping the processed-data write, SSID filtering, aggregate calculations,
  plots, and final output write in their original locations.
- Preserved the current five-second calculation exactly and did not implement
  the station-count redesign from `post_refactor_design.md`.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

### 17. Extract beacon deduplication utilities — completed

- Added `beacon_filtering.py` with `filter_by_time()` and
  `remove_duplicate_beacons()`.
- Passed the effective beacon rate explicitly from `analyze_data.py` while
  preserving the `0.95 * BEACON_RATE` comparison, first-row retention,
  per-`wlan.ta` grouping, sorting, index resets, and string dtype restoration.
- Added focused characterization tests before extraction for exact-boundary,
  just-under-boundary, multiple-transmitter, and single-row inputs.
- Preserved the existing pandas `groupby().apply()` behavior and deprecation
  warning; the warning was not addressed in this step.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

### 18. Extract current Aruba radio inference — completed

- Added `aruba_radio_inference.py` with
  `add_aruba_transmitter_integers()` and
  `assign_aruba_radio_numbers()`.
- Kept the two calls on opposite sides of the existing AP-name validation and
  SSID/BSSID counts, preserving statement order and current error behavior.
- Added targeted allocation fixtures before extraction, pinning integer MAC
  conversion, MAC sort order, the `> 1` split condition, numbering from 1,
  final timestamp sorting/index reset, and malformed/missing transmitter
  exceptions.
- Retained and exercised the existing `AP_PLATFORM=aruba` guard before
  analysis processing.
- Did not replace the Aruba adjacent-MAC algorithm with channel- or
  vendor-neutral identity.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

### 19. Extract per-radio calculations — completed

- Added `radio_analysis.py` with `calculate_total_scount()` and
  `analyze_radio_group()`.
- Passed the effective beacon rate and existing `log_and_print` function
  explicitly, preserving the entry script's logging policy.
- Preserved transmitter iteration and forward/backward lookup order,
  in-place integer `total_scount` assignment, timestamp sorting, CU percentage
  conversion, strict `wlan.qbss.cu > 191` condition, streak grouping, duration
  calculation, returned tuple order, and all log text.
- Added direct dataframe tests before extraction for station-count lookup,
  tuple positions, CU thresholds/conversions, duration, and both high-CU and
  no-high-CU logging branches.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

### 20. Extract stage plotting functions, one plot family per pass — in progress

#### Pass 1: Per-radio class-session plot — completed

- Added `radio_session_plotting.py` with `plot_channel_stations()` after its
  supplying per-radio dataframe calculations were extracted and directly
  tested in Step 19.
- Preserved the `(14, 7)` figure size, channel-utilization and station-count
  series order, CU percentage conversion, filled area, labels, title, y-axis
  limits, configured timezone formatter, literal-slash filename construction,
  `savefig()`, omitted `show()`, `close()`, and returned filename.
- Added a focused test that creates the expected file and asserts important
  pyplot calls and parameters without comparing PNG bytes.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

#### Pass 2: Class-session high-CU streak histograms — completed

- Added `class_session_plotting.py` with
  `plot_high_cu_streak_histograms()` after the class-session metrics were
  extracted and directly characterized in Steps 14–15.
- Preserved both `(10, 5)` figures, the 50%-then-75% series order, shared
  integer bins through `max + 1`, alpha and labels, titles, legends, grids,
  tight layouts, the second plot's `0–60` x-axis limit, literal-slash
  filenames, `savefig()`, `show()`, and `close("all")` behavior.
- Added a focused test that creates both expected files and asserts the
  important pyplot calls and parameters without comparing PNG bytes.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

#### Pass 3: Stations-per-enrolled distribution histogram — completed

- Added `plot_stations_per_enrolled_histogram()` to
  `class_session_plotting.py` after its supplying class-session metric was
  extracted and directly characterized in Steps 14–15.
- Preserved the dataframe `hist(bins=60)` call, title, axis labels,
  literal-slash filename, `savefig()`, `show()`, and `close("all")`
  behavior, including the absence of an explicit figure-size call.
- Added a focused test that creates the expected file and asserts the
  important histogram and pyplot calls without comparing PNG bytes.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.

#### Pass 4: Enrollment-versus-station-count scatter — completed

- Added `plot_enrollment_station_scatter()` to
  `class_session_plotting.py`, using the already-extracted and directly tested
  class-session summary columns.
- Preserved the `(10, 6)` figure size, enrolled-then-max-stations series
  order, alpha, edge color, labels, title, grid, literal-slash filename,
  `savefig()`, `show()`, and `close("all")` behavior.
- Kept `log_file.close()` in `analyze_class_sessions.py`; the extracted
  helper owns plotting only.
- Added a focused test that creates the expected file and asserts the
  important pyplot calls and parameters without comparing PNG bytes.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.

#### Pass 5: Beacon-rate-versus-channel-utilization scatter — completed

- Added `radio_summary_plotting.py` with
  `plot_beacon_rate_channel_utilization_scatter()` after the filtered
  radio-summary frames were extracted and directly tested in Step 13.
- Preserved the `(10, 6)` figure size, highest-CU-then-normalized-beacon-rate
  series order, existing beacon-rate/SSID-count division, alpha, edge color,
  labels, title, grid, literal-slash filename, `savefig()`, and `show()`
  behavior.
- Preserved the original omission of `close()` for this plot.
- Added a focused test that creates the expected file and asserts the
  important pyplot calls and parameters without comparing PNG bytes.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

#### Pass 6: Radio-count split scatter panels — completed

- Added `plot_radio_count_split_scatter()` to
  `radio_summary_plotting.py`, using the radio-count frames extracted and
  directly tested in Step 13.
- Preserved the `1 x 3` shared-y layout, `(18, 6)` figure size, dataframe
  order and beacon-rate/SSID-count divisions, radio-count color mapping,
  alpha, edge colors, titles, labels, `0–100` x-axis limits, grids, tight
  layout, literal-slash filename, `savefig()`, and `show()` behavior.
- Preserved the original omission of `close()` for this figure.
- Added a focused test that creates the expected file and checks subplot
  layout, plotted coordinates and colors, axes, and important pyplot calls
  without comparing PNG bytes.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.

#### Pass 7: Beacon-rate histogram — completed

- Added `plot_beacon_rate_histogram()` to
  `radio_summary_plotting.py`, using the filtered radio-summary frame
  extracted and directly tested in Step 13.
- Preserved the `(10, 6)` figure size, existing beacon-rate/SSID-count
  division, 50 bins, edge color, alpha, labels, title, y-axis grid settings,
  literal-slash filename, `savefig()`, and `show()` behavior.
- Preserved the original omission of `close()` for this figure.
- Added a focused test that creates the expected file and asserts the
  important histogram and pyplot calls without comparing PNG bytes.
- Left the preceding linear-regression family in the stage script because its
  model fitting, empty-data logging, summaries, and plotting remain coupled;
  this pass did not alter regression math or behavior.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.

#### Pass 8: Stacked beacon-rate histogram — completed

- Added `plot_stacked_beacon_rate_histogram()` to
  `radio_summary_plotting.py`.
- Kept the aligned beacon-rate/SSID-count divisions, `dropna()` calls,
  dataframe-variable reassignments, and combined series in
  `analyze_radio_summary.py`, preserving the inputs used by the later PDF/CDF
  and reporting cells.
- Preserved the `(10, 6)` figure size, radio-count series order, 50 bins,
  edge color, alpha, stacked mode, colors, labels, title, legend, y-axis grid
  settings, literal-slash filename, `savefig()`, and `show()` behavior.
- Preserved the original omission of `close()` for this figure.
- Added a focused test that pins the retained normalized Series and their
  indices, creates the expected file, and asserts the important histogram and
  pyplot calls without comparing PNG bytes.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.

#### Pass 9: Beacon-rate PDF and CDF figures — completed

- Added `plot_beacon_rate_pdf_cdf()` to
  `radio_summary_plotting.py`.
- Kept all three `compute_pdf_cdf(..., bins=50)` calls and their nine result
  assignments in `analyze_radio_summary.py`, preserving the tested
  distribution calculations and their execution order.
- Preserved both `(10, 6)` figures, PDF-then-CDF order, radio-1/radio-2/
  combined series order, line labels and colors, axis labels, titles,
  legends, grids, literal-slash filenames, `savefig()`, and `show()`
  behavior.
- Preserved the original omission of `close()` for both figures.
- Added a focused test that pins the distribution calls and plotted arrays,
  creates both expected files, and asserts important pyplot parameters
  without comparing PNG bytes.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.

#### Pass 10: Radio-count linear-regression figures — completed

- Added `plot_radio_regressions()` to
  `radio_summary_plotting.py`, moving the coupled model fitting, rendering,
  empty-data handling, and summary logging together as one plot family.
- Preserved `sm.add_constant()`, `sm.OLS(...).fit()`, prediction inputs,
  beacon-rate/SSID-count division, radio-count order, `None` behavior for
  empty frames, skip text, summary text, and logging order.
- Preserved the `1 x 2` shared-y layout, `(14, 6)` figure size, point and
  regression-line styling, labels, titles, `0–100` x-axis limits, legends,
  grids, tight layout, literal-slash filename, `savefig()`, and `show()`
  behavior.
- Preserved the original omission of `close()` and returned both models to
  the stage script's existing `model_1` and `model_2` variable names.
- Added focused populated- and empty-radio tests that pin fitted parameters,
  predictions, plotted data, summaries, skip behavior, and the expected file
  without comparing PNG bytes.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.
- All radio-summary plot families are now extracted; plots in other stages
  remain for later Step 20 passes.

#### Pass 11: Class-beacon channel-utilization histogram — completed

- Added `class_beacon_plotting.py` with
  `plot_channel_utilization_histogram()`.
- Kept the `100 / 255` percentage conversion and
  `np.linspace(0, 100, 256)` bin assignment in
  `analyze_all_class_beacons.py`, preserving calculation values and execution
  order.
- Preserved the `(10, 6)` figure size, input series and bins, edge color,
  alignment, labels, label padding, title, tight layout, literal-slash
  filename, `savefig()`, `show()`, and bare `close()` behavior.
- Added a focused test that pins the retained percentage and bin calculations,
  creates the expected file, and asserts important pyplot parameters without
  comparing PNG bytes.
- Added the module to isolated analysis-forward workspaces and confirmed the
  focused tests, full non-golden suite, and golden fingerprints pass.

#### Pass 12: Class-beacon channel-utilization PDF and CDF — completed

- Added `plot_channel_utilization_pdf_cdf()` to
  `class_beacon_plotting.py`.
- Kept `dropna()`, the `100 / 255` conversion,
  `compute_pdf_cdf(..., bins=256)`, all result assignments, threshold means,
  and four threshold log messages in `analyze_all_class_beacons.py`.
- Preserved both `(10, 6)` figures, PDF-then-CDF order, plotted arrays, line
  colors and labels, axis labels, titles, legends, grids, literal-slash
  filenames, `savefig()`, and `show()` behavior.
- Preserved the original omission of `close()` for both figures.
- Added a focused test that pins the retained calculations, threshold values,
  log text, plotted arrays, expected files, and important pyplot parameters
  without comparing PNG bytes.
- Confirmed the focused tests, full non-golden suite, and golden fingerprints
  pass.
- Left all remaining plot families in their current stage scripts. Continue
  Step 20 in separate passes, without re-styling or introducing a generic
  plotting abstraction.

### 21. Reduce stage scripts to orchestration, one script per pass

- Once the preceding functions are modular and directly tested, make each
  entry point primarily load configuration/data, call module functions, write
  outputs, and create the existing plots.
- Use separate passes in this order:
  1. `analyze_radio_summary.py`
  2. `analyze_class_sessions.py`
  3. `analyze_all_class_beacons.py`
  4. `analyze_data.py`
- Keep plotting in its existing stage until a separately scoped plotting
  extraction can preserve every parameter, filename, `show()`, and `close()`
  call. Do not combine these four script reductions into one change.

## Refactor Completion Boundary

The module-refactor phase is complete when the active scripts are thin,
standalone orchestration layers; extracted functions have focused tests; and
the golden fingerprints remain unchanged. At that point, evaluate semantic
work from `docs/post_refactor_design.md` as new, explicitly approved projects.
That later work includes canonical timezone handling, configurable thresholds,
missing-data semantics, physical-radio identity, multi-vendor normalization,
unified grouping/cleanup, traceable versioned outputs, and 6 GHz support.
