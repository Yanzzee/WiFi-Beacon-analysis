from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from aruba_beacon_adapter import adapt_aruba_beacons, filter_configured_aruba_scope
from aruba_radio_resolver import resolve_aruba_project_radios
from extreme_beacon_adapter import (
    adapt_extreme_beacons,
    filter_configured_extreme_scope,
)
from radio_resolver import resolve_project_radios
from project_identity_output import (
    CORRECTED_METRIC_ID,
    PROJECT_IDENTITY_ARROW_SCHEMA,
    RESULT_SCHEMA_ID,
    build_project_identity_map,
    write_project_identity_map,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "corrected_semantics"


def _integrated_identity_input() -> pd.DataFrame:
    evidence = pd.read_csv(
        FIXTURE_ROOT / "input" / "identity_evidence.csv",
        dtype="string",
        keep_default_na=False,
    )
    identity = evidence.loc[evidence["fixture_id"].eq("CF05_10")].copy()
    identity["radio_number"] = identity["radio_group_id"].map(
        {"R24": 1, "R5": 2, "R6": 3}
    )
    identity["channels"] = identity["channel"]
    identity["frequencies_mhz"] = identity["band"].map(
        {"2.4": "2437", "5": "5180", "6": "5975"}
    )
    identity["ap_model"] = "TEST-MODEL"
    return identity


def _csv_list(value: object) -> list[int]:
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [int(item) for item in str(value).split(";")]


def test_identity_map_matches_accepted_logical_oracle():
    actual = build_project_identity_map(
        _integrated_identity_input(),
        run_id="oracle-corrected-v1",
    )
    expected = pd.read_csv(
        FIXTURE_ROOT / "expected" / "project_identity_map.csv",
        dtype="string",
        keep_default_na=False,
    )
    expected["radio_number"] = pd.to_numeric(expected["radio_number"])
    expected["observed_channels"] = expected["observed_channels"].map(
        _csv_list
    )
    expected["observed_frequencies_mhz"] = expected[
        "observed_frequencies_mhz"
    ].map(_csv_list)
    expected["mapping_diagnostics"] = expected["mapping_diagnostics"].map(
        lambda value: [] if value == "" else value.split(";")
    )

    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )
    assert "is_preferred_cu_source" not in actual.columns


def test_identity_writer_uses_list_types_and_run_scoped_path(tmp_path: Path):
    identity = _integrated_identity_input()
    identity.loc[identity["bssid"].eq("A5"), "channels"] = "40,36,36"
    identity.loc[
        identity["bssid"].eq("A5"), "mapping_diagnostics"
    ] = "z_warning;a_warning;z_warning"

    path = write_project_identity_map(
        identity,
        output_root=tmp_path / "output" / "runs",
        run_id="test-run",
    )

    assert path == (
        tmp_path / "output" / "runs" / "test-run"
        / "project_identity_map.parquet"
    )
    table = pq.read_table(path)
    assert table.schema == PROJECT_IDENTITY_ARROW_SCHEMA
    rows = {row["bssid"]: row for row in table.to_pylist()}
    assert rows["A5"]["observed_channels"] == [36, 40]
    assert rows["A5"]["mapping_diagnostics"] == ["a_warning", "z_warning"]
    assert rows["A5"]["schema_id"] == RESULT_SCHEMA_ID
    assert rows["A5"]["metric_id"] == CORRECTED_METRIC_ID
    assert rows["A5"]["ap_model"] == "TEST-MODEL"


def test_identity_writer_retains_fallback_rows_and_refuses_overwrite(
    tmp_path: Path,
):
    identity = _integrated_identity_input().iloc[[0]].copy()
    identity.loc[:, "radio_group_id"] = pd.NA
    identity["radio_number"] = pd.Series(
        pd.NA,
        index=identity.index,
        dtype="Int64",
    )
    identity.loc[:, "result_grain"] = "bssid"
    identity.loc[:, "mapping_status"] = "unresolved"
    identity.loc[:, "mapping_diagnostics"] = "channel_missing"

    path = write_project_identity_map(
        identity,
        output_root=tmp_path,
        run_id="fallback-run",
    )
    row = pq.read_table(path).to_pylist()[0]
    assert row["radio_group_id"] is None
    assert row["result_grain"] == "bssid"
    assert row["mapping_diagnostics"] == ["channel_missing"]

    with pytest.raises(FileExistsError, match="already exists"):
        write_project_identity_map(
            identity,
            output_root=tmp_path,
            run_id="fallback-run",
        )


def test_identity_map_rejects_duplicate_bssid_and_unsafe_run_id():
    identity = _integrated_identity_input().iloc[[0, 0]].copy()
    with pytest.raises(ValueError, match="one row per BSSID"):
        build_project_identity_map(identity, run_id="duplicate-test")
    with pytest.raises(ValueError, match="safe path segment"):
        build_project_identity_map(identity.iloc[[0]], run_id="../outside")


def test_current_aruba_scope_resolver_map_writes_without_schema_translation(
    tmp_path: Path,
):
    records = pd.DataFrame(
        {
            "wlan.bssid": [
                "aa:bb:cc:dd:ee:10",
                "aa:bb:cc:dd:ee:11",
            ],
            "wlan.ta": [
                "aa:bb:cc:dd:ee:10",
                "aa:bb:cc:dd:ee:11",
            ],
            "wlan.ssid": ["eduroam", "guest"],
            "wlan.vs.aruba.ap_name": ["ROOM-AP-01", "ROOM-AP-01"],
            "frequency_mhz": [5180, 5180],
            "wlan.ds.current_channel": [36, 36],
            "wlan_radio.channel": [36, 36],
        }
    )
    configured = pd.DataFrame(
        {
            "Hostname": ["ROOM-AP-01"],
            "Building-Room": ["ROOM1"],
            "Model": ["515"],
        }
    )
    scoped = filter_configured_aruba_scope(records, configured)
    resolved = resolve_aruba_project_radios(
        adapt_aruba_beacons(scoped.observations)
    )

    path = write_project_identity_map(
        resolved.identity_map,
        output_root=tmp_path,
        run_id="aruba-integration",
    )
    rows = pq.read_table(path).to_pylist()

    assert len(rows) == 2
    assert {row["radio_group_id"] for row in rows} == {
        "ROOM-AP-01/radio/1"
    }
    assert {row["ap_identity_method"] for row in rows} == {
        "vendor_ap_name"
    }
    assert all(row["observed_channels"] == [36] for row in rows)
    assert {row["ap_model"] for row in rows} == {"515"}


def test_extreme_scope_resolver_map_writes_without_schema_translation(
    tmp_path: Path,
):
    records = pd.DataFrame(
        {
            "wlan.bssid": [
                "24:1f:bd:53:f0:60",
                "24:1f:bd:53:f0:61",
                "24:1f:bd:53:f0:70",
            ],
            "wlan.ta": [
                "24:1f:bd:53:f0:60",
                "24:1f:bd:53:f0:61",
                "24:1f:bd:53:f0:70",
            ],
            "wlan.ssid": ["eduroam", "guest", "eduroam"],
            "wlan.vs.extreme.ap_name": ["EXTREME-AP-01"] * 3,
            "frequency_mhz": [5220, 5220, 6355],
            "wlan_radio.channel": [44, 44, 81],
        }
    )
    configured = pd.DataFrame(
        {"Hostname": ["EXTREME-AP-01"], "Building-Room": ["ROOM1"]}
    )
    scoped = filter_configured_extreme_scope(records, configured)
    resolved = resolve_project_radios(
        adapt_extreme_beacons(scoped.observations)
    )

    path = write_project_identity_map(
        resolved.identity_map,
        output_root=tmp_path,
        run_id="extreme-integration",
    )
    rows = pq.read_table(path).to_pylist()

    assert len(rows) == 3
    assert len({row["radio_group_id"] for row in rows}) == 2
    assert {row["band"] for row in rows} == {"5", "6"}
    assert {tuple(row["observed_channels"]) for row in rows} == {
        (44,),
        (81,),
    }
