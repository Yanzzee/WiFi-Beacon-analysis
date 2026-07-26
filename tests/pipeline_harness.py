from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


ANALYSIS_SCRIPTS: tuple[str, ...] = (
    "analyze_data.py",
    "analyze_all_class_beacons.py",
    "analyze_radio_summary.py",
    "analyze_class_sessions.py",
)

ANALYSIS_MODULES: tuple[str, ...] = (
    "analysis_config.py",
    "analysis_io.py",
    "analysis_logging.py",
    "analysis_statistics.py",
    "analysis_validation.py",
    "beacon_filtering.py",
    "class_session_analysis.py",
    "radio_summary_processing.py",
    "room_station_count.py",
)

PRE_ANALYSIS_SCRIPTS: tuple[str, ...] = (
    "process_pcapng.py",
    "group_data_aruba.py",
    "group_data_cleanup.py",
    "samples.py",
)

PRE_ANALYSIS_MODULES: tuple[str, ...] = (
    "grouping_aruba.py",
    "grouping_bssid.py",
    "grouping_cleanup.py",
    "sample_processing.py",
)

SOURCE_SAMPLE_FILES: tuple[str, ...] = (
    "Sample_APs.csv",
    "Sample_classrooms.csv",
    "Sample_schedule.csv",
)

FULL_CAPTURE_FILES: tuple[str, ...] = tuple(
    (
        f"Beacon_Capture_202511{day:02d}{hour:02d}0000_"
        f"{first_sequence + hour - 7:05d}.pcapng"
    )
    for day, first_sequence in (
        (10, 10),
        (11, 34),
        (12, 58),
        (13, 82),
        (14, 106),
    )
    for hour in range(7, 23)
)

SHORT_CAPTURE_FILES: tuple[str, ...] = (
    "Beacon_Capture_20251110090000_00012.pcapng",
    "Beacon_Capture_20251110100000_00013.pcapng",
    "Beacon_Capture_20251110110000_00014.pcapng",
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

ANALYSIS_INPUT_TABLES: dict[str, tuple[str, str]] = {
    "sample_aps": ("Sample/sample_aps.parquet", "parquet"),
    "sample_classrooms": ("Sample/sample_classrooms.parquet", "parquet"),
    "sample_classes": ("Sample/sample_classes.parquet", "parquet"),
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

    for source_name in (*ANALYSIS_SCRIPTS, *ANALYSIS_MODULES):
        shutil.copy2(repo_root / source_name, workspace / source_name)


def missing_original_inputs(captures_dir: Path, sample_dir: Path) -> list[str]:
    captures_dir = Path(captures_dir)
    sample_dir = Path(sample_dir)
    missing = [
        f"Sample/{name}"
        for name in SOURCE_SAMPLE_FILES
        if not (sample_dir / name).is_file()
    ]
    missing.extend(
        f"Data/captures/{name}"
        for name in FULL_CAPTURE_FILES
        if not (captures_dir / name).is_file()
    )
    return missing


def prepare_original_input_workspace(
    captures_dir: Path,
    sample_dir: Path,
    workspace: Path,
    repo_root: Path,
) -> None:
    captures_dir = Path(captures_dir)
    sample_dir = Path(sample_dir)
    workspace = Path(workspace)
    repo_root = Path(repo_root)

    missing = missing_original_inputs(captures_dir, sample_dir)
    if missing:
        raise FileNotFoundError(
            "Original pipeline inputs are incomplete. Missing: " + ", ".join(missing)
        )

    (workspace / "Data" / "processed_data").mkdir(parents=True, exist_ok=True)
    (workspace / "Data" / "grouped_data").mkdir(parents=True, exist_ok=True)
    (workspace / "Data" / "sampled").mkdir(parents=True, exist_ok=True)
    (workspace / "Sample").mkdir(parents=True, exist_ok=True)
    (workspace / "Output").mkdir(parents=True, exist_ok=True)

    for sample_name in SOURCE_SAMPLE_FILES:
        shutil.copy2(sample_dir / sample_name, workspace / "Sample" / sample_name)

    shutil.copy2(repo_root / "config.env", workspace / "config.env")
    pipeline_sources = dict.fromkeys(
        (
            *PRE_ANALYSIS_SCRIPTS,
            *PRE_ANALYSIS_MODULES,
            *ANALYSIS_SCRIPTS,
            *ANALYSIS_MODULES,
        )
    )
    for source_name in pipeline_sources:
        shutil.copy2(repo_root / source_name, workspace / source_name)


def prepare_sample_input_workspace(
    sample_dir: Path,
    workspace: Path,
    repo_root: Path,
) -> None:
    sample_dir = Path(sample_dir)
    workspace = Path(workspace)
    repo_root = Path(repo_root)
    missing = [
        f"Sample/{name}"
        for name in SOURCE_SAMPLE_FILES
        if not (sample_dir / name).is_file()
    ]
    if missing:
        raise FileNotFoundError(
            "Original sample inputs are incomplete. Missing: " + ", ".join(missing)
        )

    (workspace / "Sample").mkdir(parents=True, exist_ok=True)
    for sample_name in SOURCE_SAMPLE_FILES:
        shutil.copy2(sample_dir / sample_name, workspace / "Sample" / sample_name)
    shutil.copy2(repo_root / "config.env", workspace / "config.env")
    for source_name in ("samples.py", "sample_processing.py", "analysis_config.py"):
        shutil.copy2(repo_root / source_name, workspace / source_name)


def prepare_grouping_workspace(
    processed_dir: Path,
    fixture_dir: Path,
    workspace: Path,
    repo_root: Path,
) -> None:
    processed_dir = Path(processed_dir)
    fixture_dir = Path(fixture_dir)
    workspace = Path(workspace)
    repo_root = Path(repo_root)
    processed_files = sorted(processed_dir.glob("*.parquet"))
    if not processed_files:
        raise FileNotFoundError(
            f"No processed parquet inputs are available in {processed_dir}"
        )

    (workspace / "Data" / "processed_data").mkdir(parents=True, exist_ok=True)
    (workspace / "Data" / "grouped_data").mkdir(parents=True, exist_ok=True)
    (workspace / "Sample").mkdir(parents=True, exist_ok=True)
    for processed_path in processed_files:
        shutil.copy2(
            processed_path,
            workspace / "Data" / "processed_data" / processed_path.name,
        )
    shutil.copytree(
        fixture_dir / "Sample",
        workspace / "Sample",
        dirs_exist_ok=True,
    )
    shutil.copy2(repo_root / "config.env", workspace / "config.env")
    for source_name in (
        "group_data_aruba.py",
        "group_data_cleanup.py",
        "grouping_aruba.py",
        "grouping_cleanup.py",
    ):
        shutil.copy2(repo_root / source_name, workspace / source_name)


def run_sample_pipeline(workspace: Path, timeout: int = 600) -> None:
    workspace = Path(workspace)
    run_script(workspace / "samples.py", workspace, timeout)


def run_capture_pipeline(
    workspace: Path,
    timeout: int = 3600,
    capture_workers: int = 4,
    captures_dir: Path | None = None,
) -> None:
    workspace = Path(workspace)
    captures_dir = (
        Path(captures_dir).resolve()
        if captures_dir is not None
        else workspace / "Data" / "captures"
    )
    run_script(
        workspace / "process_pcapng.py",
        workspace,
        timeout,
        args=(
            "--pcapng_dir",
            str(captures_dir),
            "--output_dir",
            "Data/processed_data",
            "--num_processes",
            str(capture_workers),
        ),
    )


def run_grouping_pipeline(workspace: Path, timeout: int = 3600) -> None:
    workspace = Path(workspace)
    for script_name in PRE_ANALYSIS_SCRIPTS[1:]:
        if script_name == "samples.py":
            continue
        run_script(workspace / script_name, workspace, timeout)


def run_pre_analysis_pipeline(
    workspace: Path,
    timeout: int = 3600,
    capture_workers: int = 4,
    captures_dir: Path | None = None,
) -> None:
    run_capture_pipeline(
        workspace,
        timeout=timeout,
        capture_workers=capture_workers,
        captures_dir=captures_dir,
    )
    run_grouping_pipeline(workspace, timeout=timeout)
    run_sample_pipeline(workspace, timeout=timeout)


def run_analysis_pipeline(workspace: Path, timeout: int = 600) -> None:
    workspace = Path(workspace)
    for script_name in ANALYSIS_SCRIPTS:
        run_script(workspace / script_name, workspace, timeout)


def run_script(
    script_path: Path,
    cwd: Path,
    timeout: int,
    args: tuple[str, ...] = (),
) -> None:
    env = os.environ.copy()
    env.setdefault("MPLBACKEND", "Agg")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    completed = subprocess.run(
        [sys.executable, str(script_path), *args],
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
                    f"Script stage failed: {script_path.name}",
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


def build_analysis_input_fingerprints(dataset_root: Path) -> dict[str, object]:
    dataset_root = Path(dataset_root)
    pd = _require_pandas()
    sample_aps_path = dataset_root / "Sample" / "sample_aps.parquet"
    sample_aps = pd.read_parquet(sample_aps_path)
    if "Hostname" not in sample_aps.columns:
        raise ValueError(f"{sample_aps_path} does not contain the Hostname column")

    hostnames = sorted(sample_aps["Hostname"].dropna().astype(str).unique())
    grouped_tables = {}
    for hostname in hostnames:
        grouped_path = dataset_root / "Data" / "grouped_data" / f"{hostname}.parquet"
        if not grouped_path.is_file():
            raise FileNotFoundError(
                f"Grouped input for sampled AP is missing: {grouped_path}"
            )
        grouped_tables[hostname] = _input_table_fingerprint(
            grouped_path,
            "parquet",
        )

    return {
        "sample_tables": build_sample_input_fingerprints(dataset_root),
        "grouped_tables": grouped_tables,
    }


def build_analysis_scope_input_fingerprints(
    dataset_root: Path,
) -> dict[str, object]:
    dataset_root = Path(dataset_root)
    pd = _require_pandas()
    sample_aps = pd.read_parquet(dataset_root / "Sample" / "sample_aps.parquet")
    sample_classes = pd.read_parquet(
        dataset_root / "Sample" / "sample_classes.parquet"
    )
    sessions = {}

    for _, ap_row in sample_aps.iterrows():
        hostname = str(ap_row["Hostname"])
        location = ap_row["Building-Room"]
        grouped_path = (
            dataset_root / "Data" / "grouped_data" / f"{hostname}.parquet"
        )
        if not grouped_path.is_file():
            raise FileNotFoundError(
                f"Grouped input for sampled AP is missing: {grouped_path}"
            )
        grouped = pd.read_parquet(grouped_path)
        timestamps = pd.to_datetime(
            grouped["aruba_erm.time"],
            errors="coerce",
            utc=True,
        )
        location_classes = sample_classes[sample_classes["location"] == location]
        for class_index, class_row in location_classes.iterrows():
            start_time = pd.to_datetime(class_row["start_time"], utc=True)
            end_time = pd.to_datetime(class_row["end_time"], utc=True)
            filtered = grouped[
                (timestamps >= start_time) & (timestamps <= end_time)
            ]
            key = f"{class_index}:{hostname}"
            sessions[key] = _fingerprint_dataframe(filtered)

    return {
        "sample_tables": build_sample_input_fingerprints(dataset_root),
        "sessions": sessions,
    }


def build_grouped_reference_fingerprints(
    processed_dir: Path,
    hostnames: list[str],
) -> dict[str, object]:
    pd = _require_pandas()
    frames = []
    for processed_path in sorted(Path(processed_dir).glob("*.parquet")):
        frame = pd.read_parquet(processed_path)
        if frame.index.name == "aruba_erm.time":
            frame = frame.reset_index()
        frames.append(frame)
    if not frames:
        raise FileNotFoundError(
            f"No processed parquet inputs are available in {processed_dir}"
        )

    combined = pd.concat(frames, ignore_index=True)
    ap_names = combined["wlan.vs.aruba.ap_name"].astype("string")
    return {
        hostname: _fingerprint_dataframe(combined[ap_names == hostname])
        for hostname in sorted(hostnames)
    }


def build_grouped_fingerprints(
    grouped_dir: Path,
    hostnames: list[str],
) -> dict[str, object]:
    return {
        hostname: _input_table_fingerprint(
            Path(grouped_dir) / f"{hostname}.parquet",
            "parquet",
        )
        for hostname in sorted(hostnames)
    }


def missing_grouped_reference_rows(
    processed_dir: Path,
    grouped_dir: Path,
    hostnames: list[str],
) -> dict[str, int]:
    import numpy as np

    pd = _require_pandas()
    frames = []
    for processed_path in sorted(Path(processed_dir).glob("*.parquet")):
        frame = pd.read_parquet(processed_path)
        if frame.index.name == "aruba_erm.time":
            frame = frame.reset_index()
        frames.append(frame)
    if not frames:
        raise FileNotFoundError(
            f"No processed parquet inputs are available in {processed_dir}"
        )

    combined = pd.concat(frames, ignore_index=True)
    ap_names = combined["wlan.vs.aruba.ap_name"].astype("string")
    missing = {}
    for hostname in sorted(hostnames):
        expected = combined[ap_names == hostname]
        reference = pd.read_parquet(
            Path(grouped_dir) / f"{hostname}.parquet"
        )
        expected_hashes = _canonical_row_hashes(expected)
        reference_hashes = _canonical_row_hashes(reference)
        expected_values, expected_counts = np.unique(
            expected_hashes,
            return_counts=True,
        )
        reference_values, reference_counts = np.unique(
            reference_hashes,
            return_counts=True,
        )
        positions = np.searchsorted(reference_values, expected_values)
        available_counts = np.zeros_like(expected_counts)
        valid = positions < len(reference_values)
        matched = valid.copy()
        matched[valid] = (
            reference_values[positions[valid]] == expected_values[valid]
        )
        available_counts[matched] = reference_counts[positions[matched]]
        missing_count = int(
            np.maximum(expected_counts - available_counts, 0).sum()
        )
        if missing_count:
            missing[hostname] = missing_count
    return missing


def build_sample_input_fingerprints(dataset_root: Path) -> dict[str, object]:
    dataset_root = Path(dataset_root)
    return {
        name: _input_table_fingerprint(dataset_root / rel_path, file_type)
        for name, (rel_path, file_type) in ANALYSIS_INPUT_TABLES.items()
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


def _input_table_fingerprint(path: Path, file_type: str) -> dict[str, object]:
    pd = _require_pandas()
    if file_type == "csv":
        df = pd.read_csv(path)
    elif file_type == "parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported table type: {file_type}")

    return _fingerprint_dataframe(df)


def _fingerprint_dataframe(df) -> dict[str, object]:
    fingerprint = {
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "unique": _unique_fingerprint(df),
        "timestamps": _timestamp_fingerprint(df),
        "numeric": _numeric_fingerprint(df),
    }
    row_hashes = _canonical_row_hashes(df)
    row_hashes.sort()
    fingerprint["content_sha256"] = hashlib.sha256(
        row_hashes.astype("<u8", copy=False).tobytes()
    ).hexdigest()
    return fingerprint


def _canonical_row_hashes(df):
    pd = _require_pandas()
    canonical = df.copy()
    for column in TIMESTAMP_COLUMNS:
        if column in canonical.columns:
            canonical[column] = pd.to_datetime(
                canonical[column],
                errors="coerce",
                utc=True,
            )
    row_hashes = pd.util.hash_pandas_object(
        canonical,
        index=False,
        categorize=True,
    ).to_numpy(dtype="uint64", copy=True)
    return row_hashes


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
