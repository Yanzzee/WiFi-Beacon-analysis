# Analysis Fixture Dataset

The analysis-forward regression test starts after capture processing and grouping. Do not place raw captures here, and do not include anything that requires `tshark` or Linux wireless tools.

By default, the tests look for a fixture dataset at:

```text
tests/fixtures/dataset/
```

You can also point pytest at a different fixture root:

```powershell
$env:BEACON_ANALYSIS_FIXTURE_DIR = "C:\path\to\fixture"
pytest tests/test_analysis_forward.py
```

or:

```powershell
pytest tests/test_analysis_forward.py --analysis-fixture-dir C:\path\to\fixture
```

## Required Layout

```text
dataset/
  config.env                         # optional; repo config.env is used if absent
  Sample/
    sample_aps.parquet
    sample_classrooms.parquet
    sample_classes.parquet
  Data/
    grouped_data/
      <AP-or-BSSID>.parquet
      ...
```

The grouped parquet filenames must match the AP hostnames used by `Sample/sample_aps.parquet` unless the current notebook logic has been deliberately updated. The current analysis notebook loads grouped AP data with:

```text
Data/grouped_data/<Hostname>.parquet
```

## What Not To Include

- Raw `.pcap` or `.pcapng` captures.
- `Data/processed_data`.
- Existing `Data/sampled` outputs.
- Existing `Output` files.
- Any files that require `process_pcapng.py`, `tshark`, or Linux wireless tools.

The test harness copies this fixture dataset into a temporary workspace before each run, then runs the analysis notebooks there.
