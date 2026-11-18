# Environment Setup

Use these commands from the repository root in Windows PowerShell. Use 64-bit
Python 3.13 for the scientific package stack.

## Create a fresh virtual environment

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

## Upgrade pip

```powershell
python -m pip install --upgrade pip
```

## Install dependencies

```powershell
python -m pip install -r requirements.txt
```

## Run regression tests

```powershell
python -m pytest tests -q -p no:cacheprovider -m "not pre_analysis_short and not requires_external_tools"
```

This is the regular suite. `python -m pytest tests` also selects real-capture
tests and needs TShark plus the retained capture/reference files. To reproduce
that separate evidence, use the external-suite command and explicit
`--short-processed-dir` described in the
[readiness record](benchmarks/alpha_0_2_release_readiness_2026-10-29.md).
Do not regenerate expected data to replace a missing reference folder.

For report-only changes, the focused checks are:

```powershell
python -m pytest tests/test_corrected_report_overview.py tests/test_corrected_report_refresh.py tests/test_beacon_analysis_cli.py -q -p no:cacheprovider
```

These focused checks supplement the final-candidate suite; their pass count is
not a new complete release-validation result. Current reporting/refresh behavior
is documented in [reporting and publication](reporting_and_publication.md).

## Regenerate pinned requirements

`requirements.txt` is generated from `requirements.in` with `pip-tools`. Install
`pip-tools` in the active development environment before updating pins:

```powershell
python -m pip install pip-tools
python -m piptools compile --output-file requirements.txt requirements.in
```
