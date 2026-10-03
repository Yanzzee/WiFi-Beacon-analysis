from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import process_pcapng as pp
from extreme_beacon_adapter import (
    EXTREME_BEACON_ADAPTER,
    EXTREME_NATIVE_ADAPTER,
    adapt_extreme_beacons,
    filter_configured_extreme_scope,
    select_configured_extreme_class_observations,
)
from vendor_adapter_registry import BEACON_ADAPTER_REGISTRY
from radio_resolver import resolve_project_radios


def _extreme_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "frame.time_epoch": [
                "1768471200.250000000",
                "1768471200.260000000",
                "1768471200.370000000",
            ],
            "frame.number": [10, 20, 21],
            "wlan.fixed.timestamp": [1_000_000, 1_000_000, 1_102_400],
            "wlan.fixed.beacon": [100, 100, 100],
            "wlan.bssid": ["24:1F:BD:53:F0:60"] * 3,
            "wlan.ta": ["24:1F:BD:53:F0:60"] * 3,
            "wlan.vs.extreme.ap_name": ["LVES-AP-01"] * 3,
            "wlan.ssid": ["eduroam"] * 3,
            "wlan.qbss.cu": [80, 60, 70],
            "wlan.qbss.scount": [8, 6, 7],
            "wlan.qbss.adc": [800, 600, 700],
            "radiotap.channel.freq": [5220, 5220, 5220],
            "wlan_radio.channel": [44, 44, 44],
            "source_rank": [1, 0, 0],
            "source_capture": ["b.pcapng", "a.pcapng", "a.pcapng"],
            "payload_id": ["early-origin", "duplicate-winner", "next"],
        }
    )


def _configured_aps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Hostname": ["LVES-AP-01", "LVES-AP-02"],
            "Building-Room": ["ROOM1", "ROOM2"],
        }
    )


def test_extreme_adapter_declares_only_evidenced_native_capabilities():
    assert EXTREME_NATIVE_ADAPTER.identifier == "beacon.adapter/extreme-native"
    assert EXTREME_NATIVE_ADAPTER.version == 3
    assert EXTREME_NATIVE_ADAPTER.vendor == "extreme"
    assert EXTREME_NATIVE_ADAPTER.capture_profiles == frozenset(
        {"native_80211"}
    )
    assert EXTREME_BEACON_ADAPTER.descriptor is EXTREME_NATIVE_ADAPTER
    assert EXTREME_NATIVE_ADAPTER.extraction_fields == (
        "wlan.vs.extreme.ap_name",
    )
    assert "ap_name_evidence" in EXTREME_NATIVE_ADAPTER.capabilities
    assert "qbss" in EXTREME_NATIVE_ADAPTER.capabilities
    assert "radio_identity" in EXTREME_NATIVE_ADAPTER.capabilities


def test_extreme_adapter_uses_frame_time_and_shared_band_channel_normalization():
    source = _extreme_rows()
    original = source.copy(deep=True)

    adapted = adapt_extreme_beacons(source)

    pd.testing.assert_frame_equal(source, original)
    assert adapted["bssid"].unique().tolist() == ["24:1f:bd:53:f0:60"]
    assert adapted["ap_name"].unique().tolist() == ["LVES-AP-01"]
    assert adapted["aruba_erm_time"].isna().all()
    assert str(adapted["frame_time_epoch"].dtype) == "datetime64[ns, UTC]"
    assert adapted["tsf_us"].tolist() == [1_000_000, 1_000_000, 1_102_400]
    assert adapted["qbss_cu_raw"].tolist() == [80, 60, 70]
    assert adapted["band"].tolist() == ["5", "5", "5"]
    assert adapted["channel"].tolist() == [44, 44, 44]


def test_extreme_vendor_claim_composes_with_aruba_erm_capture_time():
    source = _extreme_rows().iloc[[0]].copy()
    source["aruba_erm.time"] = "2026-01-15T10:00:00.240000000Z"

    dispatched = BEACON_ADAPTER_REGISTRY.dispatch(
        source,
        capture_profile="aruba_erm_type3",
    )

    assert dispatched.observations["vendor_id"].tolist() == ["extreme"]
    assert dispatched.observations["aruba_erm_time"].tolist() == [
        pd.Timestamp("2026-01-15T10:00:00.240000000Z")
    ]


def test_extreme_scope_filters_neighboring_and_missing_names_early():
    source = _extreme_rows()
    additions = pd.concat([source.iloc[[0]]] * 3, ignore_index=True)
    additions["wlan.vs.extreme.ap_name"] = [
        "LVES-AP-02",
        "NEIGHBOR-AP",
        "<MISSING>",
    ]
    combined = pd.concat([source, additions], ignore_index=True)

    result = filter_configured_extreme_scope(
        combined,
        _configured_aps(),
        classroom="ROOM1",
    )

    assert result.observations["payload_id"].tolist() == [
        "early-origin",
        "duplicate-winner",
        "next",
    ]
    assert result.observations["ap_id"].tolist() == ["LVES-AP-01"] * 3
    assert result.observations["classroom"].tolist() == ["ROOM1"] * 3
    assert result.counts.input_rows == 6
    assert result.counts.retained_rows == 3
    assert result.counts.missing_ap_name_rows == 1
    assert result.counts.outside_project_scope_rows == 1
    assert result.counts.other_classroom_rows == 1


def test_extreme_adapter_dispatches_native_rows_with_provenance():
    result = BEACON_ADAPTER_REGISTRY.dispatch(
        _extreme_rows(),
        capture_profile="native_80211",
    )

    assert result.counts.dispatched_rows == 3
    assert result.counts.unknown_rows == 0
    assert result.observations["vendor_id"].eq("extreme").all()
    assert result.observations["adapter_id"].eq(
        "beacon.adapter/extreme-native"
    ).all()


def test_reviewed_registry_contains_aruba_and_extreme_adapters():
    assert [
        descriptor.identifier
        for descriptor in BEACON_ADAPTER_REGISTRY.descriptors
    ] == [
        "beacon.adapter/aruba-erm",
        "beacon.adapter/extreme-native",
    ]


def test_configured_extreme_selection_uses_tsf_and_frame_time_fallback():
    result = select_configured_extreme_class_observations(
        _extreme_rows(),
        _configured_aps(),
        classroom="ROOM1",
        class_session_id="class-1",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:01Z",
        bin_width_us=102_400,
    )

    assert result.selection.timing.loc[0, "timing_method"] == "tsf_derived"
    assert result.selection.observations["observation_time_source"].eq(
        "frame_time_epoch"
    ).all()
    assert result.selection.observations["payload_id"].tolist() == [
        "duplicate-winner",
        "next",
    ]
    assert result.selection.excluded.loc[0, "payload_id"] == "early-origin"
    assert result.selection.excluded.loc[0, "exclusion_reason"] == (
        "duplicate_view"
    )
    assert result.radio_resolution_counts.resolved_bssids == 1
    assert result.radio_identity_map.loc[0, "band"] == "5"


def test_shared_resolver_separates_extreme_radios_by_band_and_channel():
    rows = pd.DataFrame(
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
            "wlan.vs.extreme.ap_name": ["EXTREME-AP"] * 3,
            "wlan.ssid": ["eduroam", "guest", "eduroam"],
            "frequency_mhz": [5220, 5220, 6355],
            "wlan_radio.channel": [44, 44, 81],
            "ap_id": ["EXTREME-AP"] * 3,
            "classroom": ["ROOM1"] * 3,
            "ap_identity_method": ["vendor_ap_name"] * 3,
        }
    )

    resolved = resolve_project_radios(adapt_extreme_beacons(rows))
    identity = resolved.identity_map.set_index("bssid")

    assert resolved.counts.resolved_bssids == 3
    assert resolved.identity_map["radio_group_id"].nunique() == 2
    assert (
        identity.loc["24:1f:bd:53:f0:60", "radio_group_id"]
        == identity.loc["24:1f:bd:53:f0:61", "radio_group_id"]
    )
    assert (
        identity.loc["24:1f:bd:53:f0:60", "radio_group_id"]
        != identity.loc["24:1f:bd:53:f0:70", "radio_group_id"]
    )
    assert identity.loc["24:1f:bd:53:f0:60", "band"] == "5"
    assert identity.loc["24:1f:bd:53:f0:70", "band"] == "6"
    assert set(identity["radio_grouping_method"]) == {
        "band_channel",
        "band_channel_single_bssid",
    }


@pytest.mark.requires_external_tools
def test_owner_extreme_capture_reaches_shared_scope_and_adapter(
    tmp_path: Path,
    monkeypatch,
):
    capture = Path(
        "Data/captures_test/"
        "Beacons_LVES_fr5220_00004_20260905195309.pcapng"
    )
    if not capture.exists():
        pytest.skip(f"Owner capture is not available: {capture}")
    portable_tshark = Path(
        r"C:\temp\WiresharkPortable64\App\Wireshark\tshark.exe"
    )
    if portable_tshark.exists():
        monkeypatch.setattr(pp, "TSHARK_EXECUTABLE", str(portable_tshark))

    converted = pd.read_parquet(pp.process_pcapng(capture, tmp_path))
    configured = pd.DataFrame(
        {
            "Hostname": ["P-LVES-B105R22S30-AP-01-WN"],
            "Building-Room": ["B105R22S30"],
        }
    )
    scoped = filter_configured_extreme_scope(converted, configured)
    dispatched = BEACON_ADAPTER_REGISTRY.dispatch(
        scoped.observations,
        capture_profile="native_80211",
    )

    assert scoped.counts.input_rows == 36_969
    assert scoped.counts.retained_rows == 24_497
    assert scoped.counts.missing_ap_name_rows == 2_992
    assert scoped.counts.outside_project_scope_rows == 9_480
    assert dispatched.counts.dispatched_rows == 24_497
    assert dispatched.observations["bssid"].nunique() == 3
    assert set(dispatched.observations["band"].dropna()) == {"5"}
    assert set(dispatched.observations["channel"].dropna()) == {44}
    assert dispatched.observations["tsf_us"].notna().all()
    assert dispatched.observations["qbss_cu_raw"].notna().all()


@pytest.mark.requires_external_tools
def test_owner_extreme_cross_band_captures_resolve_two_radios_for_common_ap(
    tmp_path: Path,
    monkeypatch,
):
    captures = [
        Path(
            "Data/captures_test/"
            "Beacons_LVES_fr5220_00004_20260905195309.pcapng"
        ),
        Path(
            "Data/captures_test/"
            "Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng"
        ),
    ]
    missing = [str(path) for path in captures if not path.exists()]
    if missing:
        pytest.skip("Owner captures are not available: " + ", ".join(missing))
    portable_tshark = Path(
        r"C:\temp\WiresharkPortable64\App\Wireshark\tshark.exe"
    )
    if portable_tshark.exists():
        monkeypatch.setattr(pp, "TSHARK_EXECUTABLE", str(portable_tshark))

    converted = pd.concat(
        [
            pd.read_parquet(
                pp.process_pcapng(
                    capture,
                    tmp_path,
                    source_rank=source_rank,
                )
            )
            for source_rank, capture in enumerate(captures)
        ],
        ignore_index=True,
    )
    configured = pd.DataFrame(
        {
            "Hostname": ["P-LVES-B105R22S30-AP-01-WN"],
            "Building-Room": ["B105R22S30"],
        }
    )
    scoped = filter_configured_extreme_scope(converted, configured)
    dispatched = BEACON_ADAPTER_REGISTRY.dispatch(
        scoped.observations,
        capture_profile="native_80211",
    )
    resolved = resolve_project_radios(dispatched.observations)
    identity = resolved.identity_map.set_index("bssid")

    assert scoped.counts.input_rows == 41_117
    assert scoped.counts.retained_rows == 24_813
    assert resolved.counts.resolved_bssids == 4
    assert resolved.identity_map["radio_group_id"].nunique() == 2
    assert set(identity["band"]) == {"5", "6"}
    assert set(identity.loc[identity["band"].eq("5"), "channels"]) == {"44"}
    assert set(identity.loc[identity["band"].eq("6"), "channels"]) == {"81"}
    assert identity.loc["24:1f:bd:53:f0:70", "band"] == "6"
    five_ghz_groups = set(
        identity.loc[identity["band"].eq("5"), "radio_group_id"]
    )
    six_ghz_groups = set(
        identity.loc[identity["band"].eq("6"), "radio_group_id"]
    )
    assert len(five_ghz_groups) == 1
    assert len(six_ghz_groups) == 1
    assert five_ghz_groups.isdisjoint(six_ghz_groups)
