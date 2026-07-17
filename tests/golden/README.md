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

## Running Against A Different Golden File

```powershell
$env:BEACON_ANALYSIS_GOLDEN_FILE = "C:\path\to\fingerprints.json"
pytest tests/test_analysis_forward.py
```

or:

```powershell
pytest tests/test_analysis_forward.py --analysis-golden-file C:\path\to\fingerprints.json
```
