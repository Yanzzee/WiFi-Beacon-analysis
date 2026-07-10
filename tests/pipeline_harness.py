from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable


ANALYSIS_NOTEBOOK_PLAN: tuple[tuple[str, tuple[int, ...] | None], ...] = (
    ("analyze_data.ipynb", (0, 1)),
    ("analyze_all_class_beacons.ipynb", tuple(range(13))),
    ("analyze_radio_summary.ipynb", None),
    ("analyze_class_sessions.ipynb", None),
)

REQUIRED_FIXTURE_FILES: tuple[str, ...] = (
    "Sample/sample_aps.parquet",
    "Sample/sample_classrooms.parquet",
    "Sample/sample_classes.parquet",
)

EXPECTED_OUTPUT_FILES: tuple[str, ...] = (
    "Data/all_class_beacons.parquet",
    "Data/radio_summary.parquet",
    "Data/all_class_beacons_processed.parquet",
    "Output/radio_summary.csv",
    "Output/filtered_radio_summary.csv",
)

EXPECTED_PLOT_FILES: tuple[str, ...] = (
    "Output/all_class_beacons/Channel_use_histogram.png",
    "Output/all_class_beacons/Channel_use_pdf.png",
    "Output/all_class_beacons/Channel_use_cdf.png",
    "Output/all_class_beacons/Scount_histogram.png",
    "Output/all_class_beacons/Scount_channel_use_hexbin.png",
    "Output/all_class_beacons/Scount_adc_hexbin.png",
    "Output/all_class_beacons/adc_channel_use_hexbin.png",
    "Output/radio_summary/Beacons_per_second_class_cu.png",
    "Output/radio_summary/Beacons_per_second_class_split.png",
    "Output/radio_summary/Beacons_per_second_class_linear_regression.png",
    "Output/radio_summary/Beacons_per_second_class_histogram.png",
    "Output/radio_summary/Beacons_per_second_stacked.png",
    "Output/radio_summary/Beacons_per_second_class_PDF.png",
    "Output/radio_summary/Beacons_per_second_class_CDF.png",
    "Output/class_sessions/High_CU_streaks.png",
    "Output/class_sessions/High_CU_streaks_zoomed.png",
    "Output/class_sessions/stations_enrolled_histogram.png",
    "Output/class_sessions/stations_enrolled_plot.png",
)

REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "Data/all_class_beacons.parquet": (
        "aruba_erm.time",
        "wlan.ta",
        "wlan.vs.aruba.ap_name",
        "wlan.ssid",
        "wlan.qbss.scount",
        "wlan.qbss.cu",
        "wlan.qbss.adc",
        "wlan.ta_int",
        "radio_number",
        "total_scount",
    ),
    "Data/radio_summary.parquet": (
        "subject",
        "location",
        "AP_hostname",
        "radio_number",
        "start_time",
        "end_time",
        "capacity",
        "enrolled",
        "AP_count",
        "radio_count",
        "bssid_count",
        "ssid_count",
        "total_beacons",
        "high_cu_beacons",
        "percent_high_cu",
        "highest_cu",
        "median_cu",
        "lowest_cu",
        "high_scount",
        "median_scount",
        "low_scount",
        "longest_duration_high_cu",
        "graph_filename",
    ),
    "Data/all_class_beacons_processed.parquet": (
        "aruba_erm.time",
        "wlan.ta",
        "wlan.vs.aruba.ap_name",
        "wlan.ssid",
        "wlan.qbss.scount",
        "wlan.qbss.cu",
        "wlan.qbss.adc",
        "radio_number",
        "total_scount",
        "location",
        "room_scount",
    ),
    "Output/radio_summary.csv": (
        "subject",
        "location",
        "AP_hostname",
        "radio_number",
        "start_time",
        "end_time",
        "total_beacons",
        "highest_cu",
        "median_cu",
        "high_scount",
        "ssid_count",
    ),
    "Output/filtered_radio_summary.csv": (
        "subject",
        "location",
        "AP_hostname",
        "radio_number",
        "start_time",
        "end_time",
        "total_beacons",
        "highest_cu",
        "median_cu",
        "high_scount",
        "ssid_count",
        "class_duration",
        "beacons_per_second",
    ),
}

FINGERPRINT_TABLES: dict[str, tuple[str, str]] = {
    "sample_classes": ("Sample/sample_classes.parquet", "parquet"),
    "all_class_beacons": ("Data/all_class_beacons.parquet", "parquet"),
    "all_class_beacons_processed": (
        "Data/all_class_beacons_processed.parquet",
        "parquet",
    ),
    "radio_summary": ("Data/radio_summary.parquet", "parquet"),
    "radio_summary_csv": ("Output/radio_summary.csv", "csv"),
    "filtered_radio_summary_csv": ("Output/filtered_radio_summary.csv", "csv"),
}

UNIQUE_COLUMNS: tuple[str, ...] = (
    "wlan.vs.aruba.ap_name",
    "wlan.bssid",
    "wlan.ta",
    "wlan.ssid",
    "AP_hostname",
    "location",
    "subject",
    "course",
    "group",
)

TIMESTAMP_COLUMNS: tuple[str, ...] = (
    "aruba_erm.time",
    "start_time",
    "end_time",
)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "radio_number",
    "radio_count",
    "wlan.qbss.cu",
    "wlan.qbss.scount",
    "wlan.qbss.adc",
    "total_scount",
    "room_scount",
    "highest_cu",
    "median_cu",
    "lowest_cu",
    "high_scount",
    "median_scount",
    "low_scount",
    "max_cu",
    "average_cu",
    "max_stations",
    "stations_enrolled",
    "duration_75",
    "duration_50",
    "total_beacons",
    "beacons_per_second",
    "enrolled",
    "capacity",
)

RUN_NOTEBOOK_CODE = r"""
import json
import os
import pathlib
import sys

os.environ.setdefault("MPLBACKEND", "Agg")

notebook_path = pathlib.Path(sys.argv[1])
selected_cells = json.loads(sys.argv[2])
nb = json.loads(notebook_path.read_text(encoding="utf-8"))

if selected_cells is None:
    selected_cells = list(range(len(nb["cells"])))

namespace = {
    "__name__": "__main__",
    "__file__": str(notebook_path),
}

for cell_index in selected_cells:
    cell = nb["cells"][cell_index]
    if cell.get("cell_type") != "code":
        continue
    source = cell.get("source", "")
    if isinstance(source, list):
        source = "".join(source)
    code = compile(source, f"{notebook_path.name}::cell_{cell_index + 1}", "exec")
    exec(code, namespace)
"""


def fixture_dataset_is_present(fixture_dir: Path) -> bool:
    fixture_dir = Path(fixture_dir)
    if not all((fixture_dir / rel).is_file() for rel in REQUIRED_FIXTURE_FILES):
        return False
    return any((fixture_dir / "Data" / "grouped_data").glob("*.parquet"))


def missing_fixture_inputs(fixture_dir: Path) -> list[str]:
    fixture_dir = Path(fixture_dir)
    missing = [
        rel for rel in REQUIRED_FIXTURE_FILES if not (fixture_dir / rel).is_file()
    ]
    if not any((fixture_dir / "Data" / "grouped_data").glob("*.parquet")):
        missing.append("Data/grouped_data/*.parquet")
    return missing


def prepare_analysis_workspace(
    fixture_dir: Path,
    workspace: Path,
    repo_root: Path,
) -> None:
    fixture_dir = Path(fixture_dir)
    workspace = Path(workspace)
    repo_root = Path(repo_root)

    missing = missing_fixture_inputs(fixture_dir)
    if missing:
        raise FileNotFoundError(
            "Fixture dataset is incomplete. Missing: " + ", ".join(missing)
        )

    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "Data" / "grouped_data").mkdir(parents=True, exist_ok=True)
    (workspace / "Data" / "sampled").mkdir(parents=True, exist_ok=True)
    (workspace / "Output").mkdir(parents=True, exist_ok=True)

    shutil.copytree(
        fixture_dir / "Sample",
        workspace / "Sample",
        dirs_exist_ok=True,
    )
    shutil.copytree(
        fixture_dir / "Data" / "grouped_data",
        workspace / "Data" / "grouped_data",
        dirs_exist_ok=True,
    )

    config_source = (
        fixture_dir / "config.env"
        if (fixture_dir / "config.env").is_file()
        else repo_root / "config.env"
    )
    shutil.copy2(config_source, workspace / "config.env")

    for notebook_name, _ in ANALYSIS_NOTEBOOK_PLAN:
        shutil.copy2(repo_root / notebook_name, workspace / notebook_name)


def run_analysis_pipeline(workspace: Path, timeout: int = 600) -> None:
    workspace = Path(workspace)
    for notebook_name, cells in ANALYSIS_NOTEBOOK_PLAN:
        run_notebook_cells(workspace / notebook_name, cells, workspace, timeout)


def run_notebook_cells(
    notebook_path: Path,
    cells: Iterable[int] | None,
    cwd: Path,
    timeout: int,
) -> None:
    env = os.environ.copy()
    env.setdefault("MPLBACKEND", "Agg")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            RUN_NOTEBOOK_CODE,
            str(notebook_path),
            json.dumps(list(cells) if cells is not None else None),
        ],
        cwd=str(cwd),
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout,
        check=False,
    )

    if completed.returncode != 0:
        raise RuntimeError(
            "\n".join(
                [
                    f"Notebook stage failed: {notebook_path.name}",
                    f"Exit code: {completed.returncode}",
                    "--- stdout ---",
                    completed.stdout,
                    "--- stderr ---",
                    completed.stderr,
                ]
            )
        )


def missing_expected_outputs(workspace: Path) -> list[str]:
    workspace = Path(workspace)
    return [
        rel
        for rel in EXPECTED_OUTPUT_FILES
        if not (workspace / rel).is_file()
    ]


def missing_expected_plots(workspace: Path) -> list[str]:
    workspace = Path(workspace)
    missing = [
        rel for rel in EXPECTED_PLOT_FILES if not (workspace / rel).is_file()
    ]
    if not any((workspace / "Output" / "plots").glob("*.png")):
        missing.append("Output/plots/*.png")
    return missing


def missing_required_columns(workspace: Path) -> dict[str, list[str]]:
    pd = _require_pandas()
    missing: dict[str, list[str]] = {}
    for rel_path, required_columns in REQUIRED_COLUMNS.items():
        path = Path(workspace) / rel_path
        if rel_path.endswith(".csv"):
            df = pd.read_csv(path, nrows=1)
        else:
            df = pd.read_parquet(path)
        absent = [column for column in required_columns if column not in df.columns]
        if absent:
            missing[rel_path] = absent
    return missing


def build_fingerprints(workspace: Path) -> dict[str, object]:
    workspace = Path(workspace)
    return {
        "config": _config_fingerprint(workspace / "config.env"),
        "tables": {
            name: _table_fingerprint(workspace / rel_path, file_type)
            for name, (rel_path, file_type) in FINGERPRINT_TABLES.items()
        },
        "plots": {
            "Output/plots": _png_count(workspace / "Output" / "plots"),
            "Output/all_class_beacons": _png_count(
                workspace / "Output" / "all_class_beacons"
            ),
            "Output/radio_summary": _png_count(
                workspace / "Output" / "radio_summary"
            ),
            "Output/class_sessions": _png_count(
                workspace / "Output" / "class_sessions"
            ),
        },
    }


def write_fingerprints(path: Path, fingerprints: dict[str, object]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(fingerprints, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _table_fingerprint(path: Path, file_type: str) -> dict[str, object]:
    pd = _require_pandas()
    if file_type == "csv":
        df = pd.read_csv(path)
    elif file_type == "parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported table type: {file_type}")

    return {
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "unique": _unique_fingerprint(df),
        "timestamps": _timestamp_fingerprint(df),
        "numeric": _numeric_fingerprint(df),
    }


def _unique_fingerprint(df) -> dict[str, object]:
    result = {}
    for column in UNIQUE_COLUMNS:
        if column not in df.columns:
            continue
        series = df[column].dropna().astype(str)
        values = sorted(series.unique().tolist())
        joined = "\n".join(values).encode("utf-8")
        result[column] = {
            "count": len(values),
            "sha256": hashlib.sha256(joined).hexdigest(),
            "values": values if len(values) <= 100 else values[:20],
        }
    return result


def _timestamp_fingerprint(df) -> dict[str, object]:
    pd = _require_pandas()
    result = {}
    for column in TIMESTAMP_COLUMNS:
        if column not in df.columns:
            continue
        values = pd.to_datetime(df[column], errors="coerce", utc=True).dropna()
        if values.empty:
            result[column] = {"min": None, "max": None}
            continue
        result[column] = {
            "min": values.min().isoformat(),
            "max": values.max().isoformat(),
        }
    return result


def _numeric_fingerprint(df) -> dict[str, object]:
    pd = _require_pandas()
    result = {}
    for column in NUMERIC_COLUMNS:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if values.empty:
            result[column] = {
                "min": None,
                "max": None,
                "median": None,
            }
            continue
        result[column] = {
            "min": _round_float(values.min()),
            "max": _round_float(values.max()),
            "median": _round_float(values.median()),
        }
    return result


def _config_fingerprint(path: Path) -> dict[str, str]:
    keys = {
        "START_DATE",
        "END_DATE",
        "TIMEZONE",
        "PLOT_TIMEZONE",
        "BEACON_RATE",
        "SSID1",
        "SSID2",
    }
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if key in keys:
            result[key] = value.split("#", 1)[0].strip()
    return result


def _png_count(path: Path) -> int:
    return len(list(Path(path).glob("*.png")))


def _round_float(value) -> float:
    return round(float(value), 6)


def _require_pandas():
    import pandas as pd

    return pd
