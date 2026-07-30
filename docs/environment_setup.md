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
python -m pytest
```

## Regenerate pinned requirements

`requirements.txt` is generated from `requirements.in` with `pip-tools`. Install
`pip-tools` in the active development environment before updating pins:

```powershell
python -m pip install pip-tools
python -m piptools compile --output-file requirements.txt requirements.in
```
