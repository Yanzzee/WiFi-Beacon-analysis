from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
GROUPING_ENV_KEYS = (
    "PROCESSED_DATA_FOLDER",
    "GROUPED_DATA_FOLDER",
    "PLOT_TIMEZONE",
    "THREAD_COUNT",
)


def _env_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _run_grouping_script(
    script_name: str,
    workspace: Path,
    config_values: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    (workspace / "config.env").write_text(
        "\n".join(
            f"{key}={value}" for key, value in config_values.items()
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    for key in GROUPING_ENV_KEYS:
        env.pop(key, None)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get(
        "PYTHONPATH",
        "",
    )
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / script_name)],
        cwd=workspace,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _function_source(script_name: str, function_name: str) -> str:
    script_path = REPO_ROOT / script_name
    source = script_path.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            function_source = ast.get_source_segment(source, node)
            assert function_source is not None
            return function_source
    raise AssertionError(f"{function_name}() was not found in {script_name}")


def _load_function(script_name: str, function_name: str):
    function_source = _function_source(script_name, function_name)
    namespace = {"re": re}
    exec(function_source, namespace)
    return namespace[function_name]


def test_grouping_sanitizers_pin_separate_trailing_rules():
    bssid_source = _function_source("group_data_bssid.py", "sanitize")
    aruba_source = _function_source(
        "group_data_aruba.py",
        "sanitize_filename",
    )
    bssid_sanitize = _load_function("group_data_bssid.py", "sanitize")
    aruba_sanitize = _load_function(
        "group_data_aruba.py",
        "sanitize_filename",
    )

    assert '.rstrip(".")' in bssid_source
    assert '.rstrip(" .")' in aruba_source

    assert bssid_sanitize(None) == "UNKNOWN"
    assert bssid_sanitize("  AP/One.  ") == "AP_One"
    assert bssid_sanitize("AP:Two") == "AP_Two"
    assert bssid_sanitize("x" * 101) == "x" * 100

    assert aruba_sanitize(None) == "UNKNOWN"
    assert aruba_sanitize("  AP/One.  ") == "AP_One"
    assert aruba_sanitize(r"AP\Two") == "AP_Two"
    assert aruba_sanitize("x" * 101) == "x" * 100


def test_group_data_bssid_groups_fallbacks_and_appends_partitions(
    tmp_path,
):
    dd = pytest.importorskip("dask.dataframe")
    processed_dir = tmp_path / "processed"
    grouped_dir = tmp_path / "grouped"
    processed_dir.mkdir()
    grouped_dir.mkdir()

    first_df = pd.DataFrame(
        {
            "wlan.bssid": ["aa", "bb", None, "nan", ""],
            "wlan.vs.aruba.ap_name": [
                "AP/One",
                None,
                None,
                "Ignored",
                "Ignored",
            ],
            "sequence": [1, 2, 3, 4, 5],
        },
        index=[10, 11, 12, 13, 14],
    )
    second_df = pd.DataFrame(
        {
            "wlan.bssid": ["aa", "cc", "dd", "none", "<NA>"],
            "wlan.vs.aruba.ap_name": [
                "AP/One",
                "",
                "AP/One",
                "Ignored",
                "Ignored",
            ],
            "sequence": [6, 7, 8, 9, 10],
        },
        index=[20, 21, 22, 23, 24],
    )
    first_df.to_parquet(processed_dir / "part.0.parquet")
    second_df.to_parquet(processed_dir / "part.1.parquet")
    assert dd.read_parquet(processed_dir).npartitions == 2

    result = _run_grouping_script(
        "group_data_bssid.py",
        tmp_path,
        {
            "PROCESSED_DATA_FOLDER": _env_path(processed_dir),
            "GROUPED_DATA_FOLDER": _env_path(grouped_dir),
        },
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.count(
        "[WARN] Missing BSSID in some rows; skipping those rows."
    ) == 5
    assert result.stdout.endswith("Finished grouping all APs.\n")
    assert {path.name for path in grouped_dir.glob("*.parquet")} == {
        "AP_One.parquet",
        "bb.parquet",
        "cc.parquet",
    }

    ap_df = pd.read_parquet(grouped_dir / "AP_One.parquet")
    assert ap_df["sequence"].tolist() == [1, 6, 8]
    assert ap_df["wlan.bssid"].tolist() == ["aa", "aa", "dd"]
    assert ap_df.index.tolist() == [10, 20, 22]

    fallback_bb_df = pd.read_parquet(grouped_dir / "bb.parquet")
    fallback_cc_df = pd.read_parquet(grouped_dir / "cc.parquet")
    assert fallback_bb_df["sequence"].tolist() == [2]
    assert fallback_cc_df["sequence"].tolist() == [7]


def test_group_data_aruba_groups_names_overwrites_collisions_and_skips_nulls(
    tmp_path,
):
    dd = pytest.importorskip("dask.dataframe")
    processed_dir = tmp_path / "processed"
    grouped_dir = tmp_path / "grouped"
    processed_dir.mkdir()
    grouped_dir.mkdir()

    first_df = pd.DataFrame(
        {
            "wlan.vs.aruba.ap_name": [
                "AP/One.",
                "",
                None,
                "AP:Two",
            ],
            "sequence": [1, 2, 3, 4],
        }
    )
    second_df = pd.DataFrame(
        {
            "wlan.vs.aruba.ap_name": [
                "AP/One.",
                " ",
                None,
                "AP:Two",
            ],
            "sequence": [5, 6, 7, 8],
        }
    )
    first_df.to_parquet(processed_dir / "part.0.parquet")
    second_df.to_parquet(processed_dir / "part.1.parquet")
    assert dd.read_parquet(processed_dir).npartitions == 2

    result = _run_grouping_script(
        "group_data_aruba.py",
        tmp_path,
        {
            "PROCESSED_DATA_FOLDER": _env_path(processed_dir),
            "GROUPED_DATA_FOLDER": _env_path(grouped_dir),
        },
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == ""
    assert {path.name for path in grouped_dir.glob("*.parquet")} == {
        "AP_One.parquet",
        "AP_Two.parquet",
        "UNKNOWN.parquet",
    }

    ap_one_df = pd.read_parquet(grouped_dir / "AP_One.parquet")
    ap_two_df = pd.read_parquet(grouped_dir / "AP_Two.parquet")
    unknown_df = pd.read_parquet(grouped_dir / "UNKNOWN.parquet")
    assert ap_one_df["sequence"].tolist() == [1, 5]
    assert ap_two_df["sequence"].tolist() == [4, 8]
    assert unknown_df["sequence"].tolist() == [2]
    all_output_sequences = pd.concat(
        [ap_one_df, ap_two_df, unknown_df],
        ignore_index=True,
    )["sequence"].tolist()
    assert 3 not in all_output_sequences
    assert 6 not in all_output_sequences
    assert 7 not in all_output_sequences


def test_group_data_cleanup_normalizes_timestamps_in_place(tmp_path):
    grouped_dir = tmp_path / "grouped"
    grouped_dir.mkdir()

    indexed_path = grouped_dir / "indexed.parquet"
    aware_path = grouped_dir / "aware.parquet"
    strings_path = grouped_dir / "strings.parquet"
    missing_path = grouped_dir / "missing.parquet"

    indexed_df = pd.DataFrame(
        {"value": [1]},
        index=pd.DatetimeIndex(
            ["2025-01-15 08:00:00"],
            name="aruba_erm.time",
        ),
    )
    indexed_df.to_parquet(indexed_path)

    aware_df = pd.DataFrame(
        {
            "aruba_erm.time": pd.to_datetime(
                ["2025-01-15 15:00:00+00:00"],
                utc=True,
            ),
            "value": [2],
        },
        index=pd.Index([77], name="source_index"),
    )
    aware_df.to_parquet(aware_path)

    strings_df = pd.DataFrame(
        {
            "aruba_erm.time": [
                "2025-01-15 15:00:00+00:00",
                "not-a-time",
            ],
            "value": [3, 4],
        }
    )
    strings_df.to_parquet(strings_path)

    missing_df = pd.DataFrame({"value": [5]})
    missing_df.to_parquet(missing_path)
    missing_bytes = missing_path.read_bytes()

    result = _run_grouping_script(
        "group_data_cleanup.py",
        tmp_path,
        {
            "PLOT_TIMEZONE": "America/Denver",
            "GROUPED_DATA_FOLDER": _env_path(grouped_dir),
            "THREAD_COUNT": "1",
        },
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Processing 4 files with 1 threads" in result.stdout
    assert "[ok] indexed.parquet" in result.stdout
    assert "[ok] aware.parquet" in result.stdout
    assert "[ok] strings.parquet" in result.stdout
    assert (
        "[skip] No 'aruba_erm.time' column in missing.parquet"
        in result.stdout
    )
    assert {path.name for path in grouped_dir.glob("*.parquet")} == {
        "indexed.parquet",
        "aware.parquet",
        "strings.parquet",
        "missing.parquet",
    }
    assert missing_path.read_bytes() == missing_bytes

    indexed_result = pd.read_parquet(indexed_path)
    assert indexed_result.index.tolist() == [0]
    assert indexed_result.columns.tolist() == ["aruba_erm.time", "value"]
    assert str(indexed_result["aruba_erm.time"].dt.tz) == "America/Denver"
    assert indexed_result["aruba_erm.time"].dt.strftime(
        "%Y-%m-%d %H:%M"
    ).tolist() == ["2025-01-15 08:00"]

    aware_result = pd.read_parquet(aware_path)
    assert aware_result.index.tolist() == [0]
    assert str(aware_result["aruba_erm.time"].dt.tz) == "America/Denver"
    assert aware_result["aruba_erm.time"].dt.strftime(
        "%Y-%m-%d %H:%M"
    ).tolist() == ["2025-01-15 08:00"]

    strings_result = pd.read_parquet(strings_path)
    assert str(strings_result["aruba_erm.time"].dt.tz) == "America/Denver"
    assert strings_result["aruba_erm.time"].iloc[0].strftime(
        "%Y-%m-%d %H:%M"
    ) == "2025-01-15 08:00"
    assert pd.isna(strings_result["aruba_erm.time"].iloc[1])
