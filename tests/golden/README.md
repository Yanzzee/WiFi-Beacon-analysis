# Golden Fingerprints

The regression test compares compact fingerprints of analysis outputs against:

```text
tests/golden/fingerprints.json
```

The fingerprint intentionally does not compare PNG bytes. It records stable summary values such as row counts, unique AP/BSSID/transmitter/SSID summaries, min/max timestamps, and max/median numeric metrics.

## Build The Initial Golden File

After placing the fixture dataset described in `tests/fixtures/README.md`, run:

```powershell
py tests/build_golden_outputs.py
```

If the fixture dataset is somewhere else:

```powershell
py tests/build_golden_outputs.py --fixture-dir C:\path\to\fixture
```

The script runs these stages in order in a temporary workspace:

1. `analyze_data.py`
2. `analyze_all_class_beacons.py`
3. `analyze_radio_summary.py`
4. `analyze_class_sessions.py`

Then it writes `tests/golden/fingerprints.json`.

## Updating The Golden File

Only update `fingerprints.json` when an intentional analysis change is expected to alter outputs. Review the JSON diff before committing it. A changed fingerprint can indicate an analysis regression, an input fixture change, or a changed dependency/runtime behavior.

## Incremental Golden Tests

Use the narrowest boundary that covers the changed files.

### Fast Input Checks

These checks do not run `tshark` or the analysis pipeline:

```powershell
pytest tests/test_pipeline_golden.py `
  -k "manifest_is_complete or sample_csv_inputs" -q
```

They verify that all 80 named full captures and the three sample CSVs are
present, and that the CSVs regenerate the tracked sample parquet inputs.

### Short Pre-Analysis Tests

For `process_pcapng.py`, run the three-capture conversion regression:

```powershell
pytest tests/test_process_pcapng_windows.py::test_process_pcapng_windows_matches_known_good -q
```

This requires `tshark`, the three files in `Data/captures_test`, and the three
known-good parquets in `Data/processed_data_test/original`.

For `group_data_aruba.py` or `group_data_cleanup.py`, start from the known-good
short processed parquets:

```powershell
pytest tests/test_pipeline_golden.py::test_short_processed_grouping_matches_known_good_rows `
  --run-short-pre-analysis-golden --pre-analysis-timeout 600 -q
```

This confirms that grouping and cleanup preserve every sampled-AP row from the
short processed set. It also confirms that those rows occur in
`Data/grouped_data_test`, which is a full grouped golden reference rather than
a three-hour grouped output.

### Full Pre-Analysis Test

For changes that may behave differently over the complete capture set:

```powershell
pytest tests/test_pipeline_golden.py::test_pre_analysis_inputs_match_golden_fixture `
  --run-pre-analysis-golden --pre-analysis-timeout 3600 --capture-workers 4 -q
```

This runs capture conversion, Aruba grouping, grouped-data cleanup, and sample
CSV processing. The checkpoint compares row-order-independent fingerprints for
each sampled AP/class session. Off-hours rows outside every sampled class are
intentionally excluded because the restored captures and the historical
grouped fixture have different off-hours coverage.

### Analysis-Forward Test

For `analyze_data.py` or any later analysis stage:

```powershell
pytest tests/test_analysis_forward.py --analysis-timeout 3600 -q
```

This starts from the tracked grouped/sample fixture, runs all four analysis
stages, and compares the final golden fingerprint.

### Complete Pipeline Test

To run everything from the original captures:

```powershell
pytest tests/test_pipeline_golden.py::test_full_pipeline_regression_from_original_inputs `
  --run-full-golden --pre-analysis-timeout 3600 `
  --analysis-timeout 3600 --capture-workers 4 -q
```

The test validates the class-session input checkpoint before starting the
expensive analysis stages, then compares the existing final golden
fingerprint. The verified Windows run with four capture workers took about
1 hour 28 minutes.

The long original-input tests are opt-in. Source directories can be overridden
with the corresponding command-line options or environment variables:

- `--golden-captures-dir` / `BEACON_ANALYSIS_CAPTURES_DIR`
- `--golden-sample-dir` / `BEACON_ANALYSIS_SAMPLE_DIR`
- `--short-captures-dir` / `BEACON_ANALYSIS_SHORT_CAPTURES_DIR`
- `--short-processed-dir` / `BEACON_ANALYSIS_SHORT_PROCESSED_DIR`
- `--short-grouped-dir` / `BEACON_ANALYSIS_SHORT_GROUPED_DIR`

## Files To Retain

| Test boundary | Untracked/local inputs that must remain |
|---|---|
| Fast sample check | `Sample/Sample_APs.csv`, `Sample/Sample_classrooms.csv`, `Sample/Sample_schedule.csv` |
| Short capture conversion | Three `09:00`, `10:00`, and `11:00` captures in `Data/captures_test`; matching parquets in `Data/processed_data_test/original` |
| Short grouping/cleanup | The three parquets in `Data/processed_data_test/original`; five sampled-AP parquets in `Data/grouped_data_test` |
| Analysis-forward | None; `tests/fixtures/dataset` and `tests/golden/fingerprints.json` are tracked |
| Full pre-analysis or complete pipeline | All 80 captures in `Data/captures` plus the three root `Sample/*.csv` files |

The three top-level parquets in `Data/processed_data_test` are regenerated
outputs and are not required. Root `Data/processed_data`, `Data/grouped_data`,
generated root `Sample/*.parquet`, and existing `Output` files are also not
required by the isolated test workspaces.

## Verified Source State

As verified on 2026-07-25:

- `Data/captures` contains the expected 80 files: hours `07:00` through
  `22:00`, inclusive, for November 10 through November 14, 2025.
- The root sample CSVs regenerate the tracked sample parquet inputs.
- The short raw captures reproduce
  `Data/processed_data_test/original` semantically.
- Short grouping and cleanup preserve all expected sampled-AP rows, and every
  short row is contained in `Data/grouped_data_test`.
- `Data/grouped_data_test`, root `Data/grouped_data`, and the tracked grouped
  fixture are byte-for-byte identical.
- The complete 80-capture pipeline passed the sampled class-session checkpoint
  and reproduced `tests/golden/fingerprints.json` through all four analysis
  stages.

## Running Against A Different Golden File

```powershell
$env:BEACON_ANALYSIS_GOLDEN_FILE = "C:\path\to\fingerprints.json"
pytest tests/test_analysis_forward.py
```

or:

```powershell
pytest tests/test_analysis_forward.py --analysis-golden-file C:\path\to\fingerprints.json
```
