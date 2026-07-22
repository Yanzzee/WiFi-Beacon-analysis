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

### 5. Extract the shared PDF/CDF calculation

- Move the identical NumPy histogram/PDF/CDF calculation from
  `analyze_all_class_beacons.py` and `analyze_radio_summary.py` into a pure
  helper such as `analysis_statistics.py`.
- Keep the caller-specific bin counts explicit: 256 for channel utilization
  and 50 for beacon rate.
- Preserve `np.histogram(..., density=True)`, bin-center calculation, cumulative
  calculation, return order, dtype behavior, and empty/small-input behavior.
- Add fixed-array tests comparing every returned array before changing either
  script. Do not move plotting in this step.

### 6. Extract grouped-AP schema validation

- Move `REQUIRED_GROUPED_AP_COLUMNS`, `_grouped_timestamp_series()`,
  `validate_grouped_ap_timestamps()`, and `validate_grouped_ap_schema()` from
  `analyze_data.py` into a validation module.
- Preserve accepted index/column forms, pandas dtype checks, native-capture
  detection, validation order, and exception messages.
- Keep the functions imported into `analyze_data.py` under their existing
  names so current tests and callers remain valid.
- Use the existing timezone-aware, timezone-naive, string-timestamp, and
  native-output tests as the primary characterization suite.

### 7. Extract analysis preflight checks

- Move `validate_radio_inference_platform()`,
  `validate_no_existing_analysis_outputs()`, and
  `validate_grouped_files_for_sample_aps()` into the validation/path layer.
- Pass required paths and platform values explicitly inside the module while
  retaining compatible names in `analyze_data.py` if tests or callers depend
  on them.
- Preserve validation order in `main()`, sampled-location scoping, sorted error
  path lists, BSSID fallback explanation, and all failure types.
- Do not change the Aruba-only platform requirement or implement vendor
  inference here.

### 8. Extract parquet persistence helpers

- Move the mechanics of `append_to_parquet()` and
  `combine_all_class_beacons()` into an I/O module.
- Preserve file discovery patterns, file ordering, PyArrow schema creation,
  compression, index handling, append/rewrite sequence, log messages, and
  output paths.
- Add focused small-parquet tests before extraction; the golden fingerprint is
  required because row order and schema can affect downstream outputs.
- Do not redesign the append strategy or output format in this step.

### 9. Extract pure sample-cleaning helpers

- Move `clean_cell()` and the existing class-period parsing/weekday expansion
  operations from `samples.py` into a `sample_processing.py` module one helper
  at a time.
- Preserve non-breaking-space cleanup, control-character removal, explicit
  dtype conversion, accepted time strings, skipped malformed periods, inclusive
  date expansion, timezone localization, and row order.
- Expand `tests/test_samples_script.py` with direct helper tests before moving
  schedule expansion.
- Do not broaden accepted schedule formats or change timezone semantics.

### 10. Make `samples.py` a thin standalone entry point

- After Step 9, move the remaining sample dataframe workflow into a callable
  function in `sample_processing.py` and leave `samples.py` responsible for
  configuration plus invocation.
- Preserve CSV/parquet filenames, output dtypes, printed messages, and script
  exit behavior.
- Continue testing the script through a subprocess so standalone behavior is
  pinned.

### 11. Characterize grouping behavior before modularizing it

- Add focused fixtures for `group_data_bssid.py`, `group_data_aruba.py`, and
  `group_data_cleanup.py` without changing their implementation.
- Pin AP-name grouping, BSSID fallback, null/invalid BSSID handling, filename
  sanitization, partition append behavior, Aruba timestamp-index cleanup,
  timezone-aware/naive handling, and in-place output behavior.
- Explicitly test the current sanitizer differences; do not assume the two
  filename functions are equivalent.

### 12. Extract grouping utilities and callable workflows

- Only after Step 11, create reusable grouping helpers while keeping the three
  current scripts as separate entry points.
- Preserve separate Aruba and BSSID policies, Dask partition traversal,
  PyArrow writer lifecycle/schema checks, output naming, cleanup concurrency,
  and error/warning messages.
- Do not unify vendor detection, grouping, and cleanup in this refactor step.
  That is a post-refactor design change tracked separately.

### 13. Extract radio-summary dataframe preparation

- Move the non-plot dataframe work from `analyze_radio_summary.py` into a
  callable module function: sorting, class-duration calculation, the
  `total_beacons >= 300` filter, positive numeric `ssid_count` filtering,
  beacon-rate calculation, and radio-count splits.
- Preserve dataframe copies, coercion behavior, row order, warning/log text,
  thresholds, and column names.
- Extend the existing focused radio-summary tests to compare complete prepared
  frames. Leave regression fitting and every plot in the script for this pass.

### 14. Characterize class-session metric edge cases

- Add tests for the current implementation before moving it: empty sessions,
  exact start/end boundaries, high/empty/high bins, high/non-high bins, multiple
  radios in one location, missing CU, zero enrollment, and primary-SSID radio
  counting.
- These are characterization tests, not corrected semantics. In particular,
  preserve the current location-wide `any()` rule and the rule that an empty
  bin does not reset a streak.
- Cross-reference the intended later redesign in
  `docs/post_refactor_design.md` rather than changing behavior here.

### 15. Extract class-session metric calculation

- After Step 14, move calculation for one class-session row into a pure or
  narrowly stateful function in a class-session analysis module.
- Preserve inclusive session filtering, bin construction, threshold values
  `191` and `127`, duration/streak updates, unique-transmitter station sums,
  nullable output dtypes, and zero/default behavior.
- Keep dataframe assignment order and all aggregate reporting/plots in
  `analyze_class_sessions.py` until a later pass.

### 16. Extract room station-count enrichment

- Move the room-level station-count algorithm from
  `analyze_all_class_beacons.py` into a dedicated callable function.
- First add a compact multi-AP fixture that pins nearest-observation selection,
  the plus/minus five-second window, transmitter de-duplication, tie behavior,
  missing AP/location handling, integer conversion, row sorting, and output
  columns.
- Preserve the five-second tolerance and current calculation exactly. Do not
  implement the station-count redesign from `post_refactor_design.md`.

### 17. Extract beacon deduplication utilities

- Move `filter_by_time()` and `remove_duplicate_beacons()` from
  `analyze_data.py` into a filtering module.
- Pass the effective beacon rate explicitly while preserving the
  `0.95 * BEACON_RATE` comparison, first-row retention, per-`wlan.ta` grouping,
  pandas `groupby().apply()` behavior, index resets, sorting, and string dtype
  restoration.
- Add focused tests for exact-boundary, just-under-boundary, multiple
  transmitters, and single-row inputs before extraction.
- Do not use this step to address the existing pandas deprecation warning.

### 18. Extract current Aruba radio inference

- Move MAC conversion and the adjacent-MAC gap algorithm from `graph()` into an
  explicitly Aruba-named module/function.
- Preserve sort order, integer conversion, the `> 1` split condition, numbering
  from 1, and current error behavior for malformed/missing transmitters.
- Keep and enforce the `AP_PLATFORM=aruba` guard.
- Add targeted allocation fixtures before extraction. Do not replace the
  algorithm with channel- or vendor-neutral identity in this step.

### 19. Extract per-radio calculations

- Move `calculate_total_scount()` and `analyze_radio_group()` into a radio
  analysis module after Steps 17 and 18 make their inputs explicit.
- Preserve iteration order, total-station assignment, CU conversion, high-CU
  condition, duration calculation, returned tuple order, and log text.
- Supply logging as a callback rather than introducing a new logging policy.
- Add direct dataframe tests and require an unchanged golden fingerprint.

### 20. Extract stage plotting functions, one plot family per pass

- Move plotting only after the dataframe calculations supplying a plot have
  been extracted and directly tested.
- Use stage-specific plotting modules or functions; do not create a generic
  plotting abstraction that hides meaningful differences.
- Preserve figure sizes, series order, bins, colors, labels, titles, grids,
  axis limits, regression overlays, filenames, `savefig()`, `show()`, and
  `close()` behavior exactly.
- Add focused tests that assert expected files and important plotting call
  parameters. Continue to avoid PNG byte comparisons.
- Use separate passes for each plot family and do not re-style plots during
  extraction.

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
