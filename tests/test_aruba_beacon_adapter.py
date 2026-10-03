from __future__ import annotations

import pandas as pd
import pytest

from aruba_beacon_adapter import (
    ARUBA_BEACON_ADAPTER,
    ARUBA_ERM_ADAPTER,
    adapt_aruba_beacons,
    filter_configured_aruba_scope,
    select_aruba_class_observations,
    select_configured_aruba_class_observations,
)
from aruba_radio_resolver import resolve_aruba_project_radios


def _aruba_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "aruba_erm.time": [
                "2026-01-15T10:00:00.250000000Z",
                "2026-01-15T10:00:00.260000000Z",
                "2026-01-15T10:00:00.370000000Z",
            ],
            "frame.time_epoch": [
                "1768471200.255000000",
                "1768471200.265000000",
                "1768471200.375000000",
            ],
            "wlan.fixed.timestamp": [1_000_000, 1_000_000, 1_102_400],
            "wlan.fixed.beacon": [100, 100, 100],
            "wlan.bssid": ["AA:BB:CC:DD:EE:01"] * 3,
            "wlan.ta": ["AA:BB:CC:DD:EE:99"] * 3,
            "wlan.vs.aruba.ap_name": ["ROOM-AP-01"] * 3,
            "wlan.ssid": ["example"] * 3,
            "wlan.qbss.cu": [80, 60, 70],
            "wlan.qbss.scount": [8, 6, 7],
            "wlan.qbss.adc": [800, 600, 700],
            "frequency_mhz": [5180, 5180, 5180],
            "wlan.ds.current_channel": [36, 36, 36],
            "source_rank": [1, 0, 0],
            "source_capture": ["b.pcapng", "a.pcapng", "a.pcapng"],
            "frame.number": [10, 20, 21],
            "payload_id": ["early-origin", "duplicate-winner", "next"],
        }
    )


def _configured_aps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Hostname": ["ROOM-AP-01", "ROOM-AP-02"],
            "Building-Room": ["ROOM1", "ROOM2"],
            "Model": ["515", "555"],
        }
    )


def test_aruba_adapter_declares_only_the_implemented_boundary():
    assert ARUBA_ERM_ADAPTER.identifier == "beacon.adapter/aruba-erm"
    assert ARUBA_ERM_ADAPTER.version == 3
    assert ARUBA_ERM_ADAPTER.vendor == "aruba"
    assert ARUBA_ERM_ADAPTER.capture_profiles == frozenset(
        {
            "aruba_erm_type0",
            "aruba_erm_type3",
            "aruba_erm_type6",
        }
    )
    assert ARUBA_BEACON_ADAPTER.descriptor is ARUBA_ERM_ADAPTER
    assert ARUBA_ERM_ADAPTER.extraction_fields == (
        "wlan.vs.aruba.ap_name",
    )
    assert {
        "preferred_observation_time",
        "frame_time_fallback",
        "tsf",
        "beacon_interval",
        "capture_lineage",
    }.issubset(ARUBA_ERM_ADAPTER.capabilities)
    assert "radio_identity" in ARUBA_ERM_ADAPTER.capabilities
    assert "band_classification" in ARUBA_ERM_ADAPTER.capabilities
    assert "channel_classification" in ARUBA_ERM_ADAPTER.capabilities


def test_aruba_adapter_normalizes_selection_fields_without_mutating_input():
    source = _aruba_rows()
    original = source.copy(deep=True)

    adapted = adapt_aruba_beacons(source)

    pd.testing.assert_frame_equal(source, original)
    assert adapted["bssid"].unique().tolist() == ["aa:bb:cc:dd:ee:01"]
    assert adapted["transmitter_address"].unique().tolist() == [
        "aa:bb:cc:dd:ee:99"
    ]
    assert adapted["ap_name"].unique().tolist() == ["ROOM-AP-01"]
    assert adapted["tsf_us"].tolist() == [1_000_000, 1_000_000, 1_102_400]
    assert adapted["beacon_interval_tu"].tolist() == [100, 100, 100]
    assert adapted["source_frame"].tolist() == [10, 20, 21]
    assert adapted["qbss_cu_raw"].tolist() == [80, 60, 70]
    assert adapted["band"].tolist() == ["5", "5", "5"]
    assert adapted["channel"].tolist() == [36, 36, 36]
    assert adapted["band_channel_status"].tolist() == ["complete"] * 3
    assert str(adapted["aruba_erm_time"].dtype) == "datetime64[ns, UTC]"
    assert str(adapted["frame_time_epoch"].dtype) == "datetime64[ns, UTC]"


def test_aruba_adapter_fills_direct_capture_lineage_when_not_persisted():
    source = _aruba_rows().drop(columns=["source_capture", "source_rank"])

    adapted = adapt_aruba_beacons(
        source,
        source_capture="capture-a.pcapng",
        source_rank=3,
    )

    assert adapted["source_capture"].tolist() == ["capture-a.pcapng"] * 3
    assert adapted["source_rank"].tolist() == [3, 3, 3]


def test_aruba_adapter_feeds_corrected_selector_and_keeps_full_pool_origin():
    result = select_aruba_class_observations(
        _aruba_rows(),
        class_session_id="class-1",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:01Z",
        bin_width_us=102_400,
    )

    assert result.timing.loc[0, "timing_method"] == "tsf_derived"
    assert result.timing.loc[0, "tsf_origin"] == pd.Timestamp(
        "2026-01-15T09:59:59.250000Z"
    )
    assert result.observations["payload_id"].tolist() == [
        "duplicate-winner",
        "next",
    ]
    assert result.observations["placement_time"].tolist() == [
        pd.Timestamp("2026-01-15T10:00:00.250000Z"),
        pd.Timestamp("2026-01-15T10:00:00.352400Z"),
    ]
    assert result.excluded.loc[0, "payload_id"] == "early-origin"
    assert result.excluded.loc[0, "exclusion_reason"] == "duplicate_view"


def test_scope_filter_keeps_only_configured_aps_in_the_classroom():
    source = _aruba_rows()
    source["wlan.vs.aruba.ap_name"] = " room-ap-01 "
    additions = pd.concat([source.iloc[[0]]] * 3, ignore_index=True)
    additions["wlan.vs.aruba.ap_name"] = [
        "ROOM-AP-02",
        "NEIGHBOR-AP",
        pd.NA,
    ]
    additions["payload_id"] = ["other-room", "outside", "missing-name"]
    combined = pd.concat([source, additions], ignore_index=True)
    original = combined.copy(deep=True)

    result = filter_configured_aruba_scope(
        combined,
        _configured_aps(),
        classroom="ROOM1",
    )

    pd.testing.assert_frame_equal(combined, original)
    assert result.observations["payload_id"].tolist() == [
        "early-origin",
        "duplicate-winner",
        "next",
    ]
    assert result.observations["ap_name"].tolist() == ["ROOM-AP-01"] * 3
    assert result.observations["ap_id"].tolist() == ["ROOM-AP-01"] * 3
    assert result.observations["classroom"].tolist() == ["ROOM1"] * 3
    assert result.observations["ap_model"].tolist() == ["515"] * 3
    assert result.observations["ap_identity_method"].tolist() == [
        "vendor_ap_name"
    ] * 3
    assert result.observations["mapping_status"].tolist() == ["resolved"] * 3
    assert result.counts.input_rows == 6
    assert result.counts.retained_rows == 3
    assert result.counts.missing_ap_name_rows == 1
    assert result.counts.outside_project_scope_rows == 1
    assert result.counts.other_classroom_rows == 1


def test_configured_ap_mapping_rejects_missing_and_duplicate_hostnames():
    duplicate = pd.DataFrame(
        {
            "Hostname": ["AP-ONE", " ap-one "],
            "Building-Room": ["ROOM1", "ROOM2"],
        }
    )

    with pytest.raises(ValueError, match="must be unique after normalization"):
        filter_configured_aruba_scope(_aruba_rows(), duplicate)

    missing_column = pd.DataFrame({"Hostname": ["AP-ONE"]})
    with pytest.raises(ValueError, match="Building-Room"):
        filter_configured_aruba_scope(_aruba_rows(), missing_column)


def test_configured_scope_is_applied_before_corrected_selection():
    source = _aruba_rows()
    outside = source.iloc[[0]].copy()
    outside["wlan.vs.aruba.ap_name"] = "NEIGHBOR-AP"
    outside["payload_id"] = "outside"
    combined = pd.concat([outside, source], ignore_index=True)

    result = select_configured_aruba_class_observations(
        combined,
        _configured_aps(),
        classroom="ROOM1",
        class_session_id="class-1",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:01Z",
        bin_width_us=102_400,
    )

    assert result.scope_counts.outside_project_scope_rows == 1
    assert set(result.selection.observations["payload_id"]) == {
        "duplicate-winner",
        "next",
    }
    assert result.selection.observations["ap_id"].tolist() == [
        "ROOM-AP-01",
        "ROOM-AP-01",
    ]
    assert result.selection.observations["classroom"].tolist() == [
        "ROOM1",
        "ROOM1",
    ]


def test_configured_scope_returns_an_empty_selection_when_no_ap_matches():
    source = _aruba_rows()
    source["wlan.vs.aruba.ap_name"] = "NEIGHBOR-AP"

    result = select_configured_aruba_class_observations(
        source,
        _configured_aps(),
        classroom="ROOM1",
        class_session_id="class-1",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:01Z",
        bin_width_us=102_400,
    )

    assert result.scope_counts.outside_project_scope_rows == 3
    assert result.scope_counts.retained_rows == 0
    assert result.selection.observations.empty
    assert result.selection.selected.empty


def _radio_rows() -> pd.DataFrame:
    rows = []
    for bssid, ssid, frequency, ds_channel, he_channel in [
        ("ec:fc:c6:cb:b7:40", "eduroam", 2462, 11, pd.NA),
        ("ec:fc:c6:cb:b7:41", "guest", 2462, 11, pd.NA),
        ("ec:fc:c6:cb:b7:30", "eduroam", 5520, 104, pd.NA),
        ("ec:fc:c6:cb:b7:31", "guest", 5520, 104, pd.NA),
        ("ec:fc:c6:cb:b7:24", "eduroam", 6775, pd.NA, 165),
    ]:
        rows.append(
            {
                "wlan.bssid": bssid,
                "wlan.ta": bssid,
                "wlan.ssid": ssid,
                "wlan.vs.aruba.ap_name": "TRIBAND-AP",
                "frequency_mhz": frequency,
                "wlan.ds.current_channel": ds_channel,
                "wlan.ext_tag.he_operation.6ghz.primary_channel": he_channel,
                "wlan_radio.channel": (
                    he_channel if pd.notna(he_channel) else ds_channel
                ),
                "ap_id": "TRIBAND-AP",
                "classroom": "ROOM1",
            }
        )
    return pd.DataFrame(rows)


def test_tri_band_evidence_distinguishes_overlapping_channel_numbers():
    rows = _radio_rows()
    collision = rows.iloc[[0]].copy()
    collision["wlan.bssid"] = "ec:fc:c6:cb:b7:20"
    collision["wlan.ta"] = "ec:fc:c6:cb:b7:20"
    collision["frequency_mhz"] = 2412
    collision["wlan.ds.current_channel"] = 1
    collision["wlan_radio.channel"] = 1
    six = rows.iloc[[-1]].copy()
    six["wlan.bssid"] = "ec:fc:c6:cb:b7:10"
    six["wlan.ta"] = "ec:fc:c6:cb:b7:10"
    six["frequency_mhz"] = 5955
    six["wlan.ext_tag.he_operation.6ghz.primary_channel"] = 1
    six["wlan_radio.channel"] = 1
    rows = pd.concat([collision, six], ignore_index=True)

    adapted = adapt_aruba_beacons(rows)
    resolved = resolve_aruba_project_radios(adapted)

    assert adapted["band"].tolist() == ["2.4", "6"]
    assert adapted["channel"].tolist() == [1, 1]
    assert resolved.identity_map["radio_group_id"].nunique() == 2


def test_project_resolver_groups_by_band_channel_and_tracks_channel_changes():
    rows = _radio_rows()
    changed = rows.loc[rows["wlan.bssid"].isin([
        "ec:fc:c6:cb:b7:30",
        "ec:fc:c6:cb:b7:31",
    ])].copy()
    changed["frequency_mhz"] = 5540
    changed["wlan.ds.current_channel"] = 108
    changed["wlan_radio.channel"] = 108
    rows = pd.concat([rows, changed], ignore_index=True)

    resolved = resolve_aruba_project_radios(adapt_aruba_beacons(rows))
    identity = resolved.identity_map.set_index("bssid")

    assert identity.loc["ec:fc:c6:cb:b7:40", "band"] == "2.4"
    assert identity.loc["ec:fc:c6:cb:b7:30", "band"] == "5"
    assert identity.loc["ec:fc:c6:cb:b7:24", "band"] == "6"
    assert identity.loc["ec:fc:c6:cb:b7:30", "ap_name"] == "TRIBAND-AP"
    assert identity.loc["ec:fc:c6:cb:b7:30", "classroom"] == "ROOM1"
    assert identity.loc["ec:fc:c6:cb:b7:30", "channels"] == "104,108"
    assert identity.loc[
        "ec:fc:c6:cb:b7:30", "frequencies_mhz"
    ] == "5520,5540"
    assert (
        identity.loc["ec:fc:c6:cb:b7:30", "radio_group_id"]
        == identity.loc["ec:fc:c6:cb:b7:31", "radio_group_id"]
    )
    assert resolved.identity_map["radio_group_id"].nunique() == 3
    assert resolved.counts.resolved_bssids == 5


def test_distinct_channels_within_named_ap_resolve_as_distinct_radios():
    rows = _radio_rows().iloc[[2, 3]].copy()
    rows.loc[rows.index[1], "frequency_mhz"] = 5785
    rows.loc[rows.index[1], "wlan.ds.current_channel"] = 157
    rows.loc[rows.index[1], "wlan_radio.channel"] = 157

    resolved = resolve_aruba_project_radios(adapt_aruba_beacons(rows))

    assert resolved.identity_map["radio_group_id"].nunique() == 2
    assert resolved.identity_map["mapping_diagnostics"].tolist() == ["", ""]


def test_channel_groups_bssids_only_within_the_same_named_ap():
    same_ap = _radio_rows().iloc[[2, 3]].copy()
    same_ap.loc[same_ap.index[1], "wlan.bssid"] = "ec:fc:c6:cb:b7:51"
    same_ap.loc[same_ap.index[1], "wlan.ta"] = "ec:fc:c6:cb:b7:51"

    same_ap_result = resolve_aruba_project_radios(
        adapt_aruba_beacons(same_ap)
    )

    assert same_ap_result.identity_map["radio_group_id"].nunique() == 1
    methods = same_ap_result.identity_map[
        "radio_grouping_method"
    ].unique().tolist()
    assert methods == ["band_channel"]

    other_ap = same_ap.iloc[[1]].copy()
    other_ap["wlan.bssid"] = "ec:fc:c6:cb:b8:51"
    other_ap["wlan.ta"] = "ec:fc:c6:cb:b8:51"
    other_ap["ap_id"] = "OTHER-AP"
    other_ap["wlan.vs.aruba.ap_name"] = "OTHER-AP"
    cross_ap = pd.concat([same_ap.iloc[[0]], other_ap], ignore_index=True)

    cross_ap_result = resolve_aruba_project_radios(
        adapt_aruba_beacons(cross_ap)
    )

    assert cross_ap_result.identity_map["radio_group_id"].nunique() == 2


def test_duplicate_ssid_within_channel_group_uses_bssid_fallback():
    rows = _radio_rows().iloc[[2, 3]].copy()
    rows["wlan.ssid"] = "eduroam"

    resolved = resolve_aruba_project_radios(adapt_aruba_beacons(rows))

    assert resolved.identity_map["radio_group_id"].isna().all()
    assert resolved.identity_map["result_grain"].tolist() == ["bssid", "bssid"]
    assert resolved.identity_map["mapping_status"].tolist() == [
        "conflict",
        "conflict",
    ]
    assert resolved.counts.conflict_bssids == 2


def test_bssid_mapped_to_two_aps_uses_conflict_fallback():
    rows = _radio_rows().iloc[[2, 2]].copy().reset_index(drop=True)
    rows.loc[rows.index[1], "ap_id"] = "OTHER-AP"
    rows.loc[rows.index[1], "wlan.vs.aruba.ap_name"] = "OTHER-AP"

    resolved = resolve_aruba_project_radios(adapt_aruba_beacons(rows))

    assert resolved.identity_map["radio_group_id"].isna().all()
    assert resolved.identity_map.loc[0, "mapping_status"] == "conflict"
    assert "project_bssid_ap_conflict" in resolved.identity_map.loc[
        0, "mapping_diagnostics"
    ]


def test_bssid_without_band_channel_evidence_uses_fallback():
    rows = _radio_rows().iloc[[2, 3]].copy()
    rows.loc[rows.index[1], [
        "frequency_mhz",
        "wlan.ds.current_channel",
        "wlan_radio.channel",
    ]] = pd.NA

    resolved = resolve_aruba_project_radios(adapt_aruba_beacons(rows))
    identity = resolved.identity_map.set_index("bssid")

    assert pd.notna(identity.loc["ec:fc:c6:cb:b7:30", "radio_group_id"])
    assert pd.isna(identity.loc["ec:fc:c6:cb:b7:31", "radio_group_id"])
    assert identity.loc[
        "ec:fc:c6:cb:b7:31", "mapping_diagnostics"
    ] == "band_missing;channel_missing"


def test_configured_selection_resolves_radios_before_classroom_filtering():
    room1 = _aruba_rows().iloc[[0]].copy()
    room2 = room1.copy()
    room2["wlan.vs.aruba.ap_name"] = "ROOM-AP-02"
    room2["wlan.bssid"] = "aa:bb:cc:dd:ff:01"
    room2["wlan.ta"] = "aa:bb:cc:dd:ff:01"
    combined = pd.concat([room2, room1], ignore_index=True)

    result = select_configured_aruba_class_observations(
        combined,
        _configured_aps(),
        classroom="ROOM1",
        class_session_id="class-1",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:01Z",
        bin_width_us=102_400,
    )

    assert result.scope_counts.other_classroom_rows == 1
    assert set(result.radio_identity_map["ap_id"]) == {
        "ROOM-AP-01",
        "ROOM-AP-02",
    }
    assert set(result.radio_identity_map["ap_identity_method"]) == {
        "vendor_ap_name"
    }
    assert result.selection.observations["radio_group_id"].notna().all()
