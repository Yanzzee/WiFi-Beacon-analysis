from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from aruba_beacon_adapter import (
    adapt_aruba_beacons,
    filter_configured_aruba_scope,
    select_configured_aruba_class_observations,
)
from aruba_radio_resolver import resolve_aruba_project_radios
from corrected_beacon_selection import build_class_bins
from corrected_summary_output import build_corrected_summaries
from project_identity_output import build_project_identity_map
import process_pcapng as pp
from radio_resolver import (
    RADIO_IDENTITY_ATTACHMENT_COLUMNS,
    attach_project_radio_identity,
)


def _configured_aps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Hostname": ["AP-ONE", "AP-TWO"],
            "Building-Room": ["ROOM1", "ROOM2"],
        }
    )


def _project_rows() -> pd.DataFrame:
    definitions = [
        ("aa:bb:cc:00:00:10", "eduroam", "AP-ONE", 5180, 36, pd.NA, 10),
        ("aa:bb:cc:00:00:11", "guest", "AP-ONE", 5180, 36, pd.NA, 5),
        ("aa:bb:cc:00:00:20", "eduroam", "AP-ONE", 5975, 5, 5, 3),
        ("aa:bb:cc:00:01:10", "eduroam", "AP-TWO", 5180, 36, pd.NA, 7),
    ]
    rows: list[dict[str, object]] = []
    frame_number = 1
    for (
        bssid,
        ssid,
        ap_name,
        frequency,
        channel,
        he_channel,
        stations,
    ) in definitions:
        for second in (0, 1):
            rows.append(
                {
                    "frame.time_epoch": str(1768471200.1 + second),
                    "wlan.fixed.timestamp": 100_000 + second * 1_024_000,
                    "wlan.fixed.beacon": 100,
                    "wlan.bssid": bssid,
                    "wlan.ta": bssid,
                    "wlan.ssid": ssid,
                    "wlan.vs.aruba.ap_name": ap_name,
                    "wlan.qbss.cu": 153,
                    "wlan.qbss.scount": stations + second,
                    "wlan.qbss.adc": 1000,
                    "frequency_mhz": frequency,
                    "wlan.ds.current_channel": channel,
                    "wlan.ext_tag.he_operation.6ghz.primary_channel": he_channel,
                    "wlan_radio.channel": channel,
                    "source_capture": "project.pcapng",
                    "source_rank": 0,
                    "frame.number": frame_number,
                }
            )
            frame_number += 1

    outside = rows[0].copy()
    outside["wlan.bssid"] = "aa:bb:cc:ff:ff:ff"
    outside["wlan.ta"] = "aa:bb:cc:ff:ff:ff"
    outside["wlan.vs.aruba.ap_name"] = "NEIGHBOR-AP"
    outside["frame.number"] = frame_number
    rows.append(outside)
    return pd.DataFrame(rows)


def _resolved_logical_map(records: pd.DataFrame) -> pd.DataFrame:
    scoped = filter_configured_aruba_scope(records, _configured_aps())
    adapted = adapt_aruba_beacons(scoped.observations)
    resolved = resolve_aruba_project_radios(adapted)
    return build_project_identity_map(
        resolved.identity_map,
        run_id="pr0040-invariants",
    )


def test_current_aruba_project_identity_invariants():
    identity = _resolved_logical_map(_project_rows())

    assert identity["bssid"].is_unique
    assert not identity["bssid"].eq("aa:bb:cc:ff:ff:ff").any()
    assert identity["mapping_status"].eq("resolved").all()
    assert identity["result_grain"].eq("ap_radio_group").all()
    assert identity["ap_identity_method"].eq("vendor_ap_name").all()
    assert identity["radio_group_id"].notna().all()

    for _radio_group_id, members in identity.groupby("radio_group_id"):
        assert members["ap_id"].nunique() == 1
        assert members["classroom"].nunique() == 1
        assert members["band"].nunique() == 1
        assert members["ssid"].is_unique

    same_channel = identity.loc[
        identity["observed_channels"].map(lambda values: values == [36])
    ]
    assert same_channel["ap_id"].nunique() == 2
    assert same_channel["radio_group_id"].nunique() == 2


def test_project_identity_is_independent_of_input_row_order():
    records = _project_rows()
    expected = _resolved_logical_map(records)

    shuffled = records.sample(frac=1, random_state=40).reset_index(drop=True)
    actual = _resolved_logical_map(shuffled)

    pd.testing.assert_frame_equal(actual, expected)


def test_compact_identity_map_reattaches_same_radio_fields():
    scoped = filter_configured_aruba_scope(_project_rows(), _configured_aps())
    adapted = adapt_aruba_beacons(scoped.observations)
    resolved = resolve_aruba_project_radios(adapted)

    attached = attach_project_radio_identity(adapted, resolved.identity_map)

    columns = list(RADIO_IDENTITY_ATTACHMENT_COLUMNS)
    pd.testing.assert_frame_equal(
        attached.loc[:, columns].reset_index(drop=True),
        resolved.observations.loc[:, columns].reset_index(drop=True),
    )


def test_selected_rows_resolve_once_and_keep_each_bssid_station_count():
    result = select_configured_aruba_class_observations(
        _project_rows(),
        _configured_aps(),
        classroom="ROOM1",
        class_session_id="class-1",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:02Z",
        bin_width_us=1_024_000,
    )
    identity = result.radio_identity_map.loc[:, ["bssid", "radio_group_id"]]
    selected = result.selection.selected

    linked = selected.merge(
        identity,
        on="bssid",
        how="left",
        validate="many_to_one",
        suffixes=("", "_identity"),
        indicator=True,
    )
    assert linked["_merge"].eq("both").all()
    assert linked["radio_group_id"].eq(
        linked["radio_group_id_identity"]
    ).all()
    assert selected["classroom"].eq("ROOM1").all()
    assert not selected["bssid"].eq("aa:bb:cc:00:01:10").any()
    assert "is_preferred_cu_source" not in result.radio_identity_map.columns

    five_ghz_bin_zero = selected.loc[
        selected["band"].eq("5") & selected["bin_number"].eq(0)
    ]
    assert set(five_ghz_bin_zero["ssid"]) == {"eduroam", "guest"}
    assert five_ghz_bin_zero["qbss_station_count"].sum() == 15


def test_two_same_band_radios_survive_scope_selection_and_summary():
    records = _project_rows()
    first_radio = records.loc[
        records["wlan.bssid"].eq("aa:bb:cc:00:00:10")
    ].copy()
    second_radio = first_radio.copy()
    second_radio.loc[:, "wlan.bssid"] = "aa:bb:cc:00:00:30"
    second_radio.loc[:, "wlan.ta"] = "aa:bb:cc:00:00:30"
    second_radio.loc[:, "frequency_mhz"] = 5785
    second_radio.loc[:, "wlan.ds.current_channel"] = 157
    second_radio.loc[:, "wlan_radio.channel"] = 157
    records = pd.concat([records, second_radio], ignore_index=True)

    result = select_configured_aruba_class_observations(
        records,
        _configured_aps(),
        classroom="ROOM1",
        class_session_id="two-radio-class",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:02.048Z",
        bin_width_us=1_024_000,
    )
    room_identity = result.radio_identity_map.loc[
        result.radio_identity_map["classroom"].eq("ROOM1")
    ]
    five_identity = room_identity.loc[room_identity["band"].eq("5")]
    five_selected = result.selection.selected.loc[
        result.selection.selected["band"].eq("5")
    ]

    assert five_identity["radio_group_id"].nunique() == 2
    assert five_selected["radio_group_id"].nunique() == 2
    selected_bins = five_selected.groupby("radio_group_id")[
        "bin_number"
    ].nunique()
    assert selected_bins.eq(2).all()

    bins = build_class_bins(
        "2026-01-15T10:00:00Z",
        "2026-01-15T10:00:02.048Z",
        1_024_000,
    )
    bins["class_session_id"] = "two-radio-class"
    bins["classroom"] = "ROOM1"
    summaries = build_corrected_summaries(
        bins,
        room_identity,
        result.selection.selected,
        run_id="two-radio-invariant",
    )
    five_summary = summaries.radio_class.loc[
        summaries.radio_class["band"].eq("5")
    ]
    assert len(five_summary) == 2
    assert five_summary["valid_cu_duration_us"].gt(0).all()


@pytest.mark.requires_external_tools
def test_owner_type6_capture_replays_current_aruba_identity(
    tmp_path: Path,
    monkeypatch,
):
    capture = Path("Data/captures_test") / (
        "Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng"
    )
    if not capture.exists():
        pytest.skip(f"Owner capture is not available: {capture}")
    portable_tshark = Path(
        r"C:\temp\WiresharkPortable64\App\Wireshark\tshark.exe"
    )
    if portable_tshark.exists():
        monkeypatch.setattr(pp, "TSHARK_EXECUTABLE", str(portable_tshark))

    converted_path = pp.process_pcapng(capture, tmp_path)
    records = pd.read_parquet(converted_path)
    adapted = adapt_aruba_beacons(records)
    candidates = []
    for ap_name, rows in adapted.dropna(subset=["ap_name"]).groupby("ap_name"):
        if {"2.4", "5", "6"}.issubset(set(rows["band"].dropna())):
            candidates.append(str(ap_name))
    assert candidates, "expected one configured tri-band Aruba AP"

    configured = pd.DataFrame(
        {"Hostname": [sorted(candidates)[0]], "Building-Room": ["ROOM1"]}
    )
    scoped = filter_configured_aruba_scope(records, configured)
    resolved = resolve_aruba_project_radios(
        adapt_aruba_beacons(scoped.observations)
    )
    identity = resolved.identity_map

    assert set(identity["band"]) == {"2.4", "5", "6"}
    assert identity["radio_group_id"].nunique() == 3
    assert identity.groupby("band")["radio_group_id"].nunique().to_dict() == {
        "2.4": 1,
        "5": 1,
        "6": 1,
    }
