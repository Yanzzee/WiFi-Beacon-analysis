from __future__ import annotations

import pandas as pd
import pytest

import process_pcapng as pp
from extreme_beacon_adapter import adapt_extreme_beacons
from project_scope import filter_configured_project_scope
from radio_resolver import resolve_project_radios


@pytest.mark.parametrize(
    "name",
    ["VerizonWiFiAccess", "BYU Cougars WiFi", "eduroam", "Café WiFi"],
)
def test_hex_and_readable_ssids_match_in_mixed_input(name):
    encoded = name.encode("utf-8").hex()
    frame = pd.DataFrame(
        {"wlan.ssid": [encoded, name, encoded.upper(), "<MISSING>", None]},
        index=[9, 3, 7, 2, 6],
    )

    pp.normalize_ssid_column(frame)

    assert frame["wlan.ssid"].iloc[:3].tolist() == [name] * 3
    assert frame["wlan.ssid"].iloc[3] == "<MISSING>"
    assert pd.isna(frame["wlan.ssid"].iloc[4])
    assert frame.index.tolist() == [9, 3, 7, 2, 6]
    assert str(frame["wlan.ssid"].dtype) == "string"


@pytest.mark.parametrize("missing_count", [0, 21, 30, 33, 100, 200])
def test_ssid_decode_does_not_depend_on_first_100_or_hidden_population(missing_count):
    name = "VerizonWiFiAccess"
    values = ["<MISSING>"] * missing_count + [name.encode().hex()] * 100
    source = pd.DataFrame({"wlan.ssid": values})
    shuffled = source.sample(frac=1, random_state=42)

    pp.normalize_ssid_column(source)
    pp.normalize_ssid_column(shuffled)

    pd.testing.assert_frame_equal(source, shuffled.sort_index())
    assert source["wlan.ssid"].iloc[missing_count:].eq(name).all()


def test_ssid_decode_preserves_missing_readable_and_opaque_values():
    values = [
        None, pd.NA, "", "<MISSING>", "eduroam", "cafe", "deadbeef",
        "ff", "fe", "00", "0a", "ff00", "not-hex", "abc",
    ]
    frame = pd.DataFrame({"wlan.ssid": values})
    expected = frame["wlan.ssid"].astype("string")

    pp.normalize_ssid_column(frame)

    # Invalid UTF-8 must not become replacement characters and falsely merge.
    pd.testing.assert_series_equal(frame["wlan.ssid"], expected)


def test_ssid_decode_preserves_case_and_does_not_merge_different_names():
    values = ["WiFi", "wifi", "WiFi".encode().hex(), "wifi".encode().hex()]
    frame = pd.DataFrame({"wlan.ssid": values})

    pp.normalize_ssid_column(frame)

    assert frame["wlan.ssid"].tolist() == ["WiFi", "wifi", "WiFi", "wifi"]


def test_ssid_decode_handles_empty_and_absent_columns():
    empty = pd.DataFrame({"wlan.ssid": pd.Series(dtype="object")})
    absent = pd.DataFrame({"payload": [1]})
    original = absent.copy()

    pp.normalize_ssid_column(empty)
    pp.normalize_ssid_column(absent)

    assert str(empty["wlan.ssid"].dtype) == "string"
    pd.testing.assert_frame_equal(absent, original)


def test_ssid_decode_is_once_per_distinct_token(monkeypatch):
    real_decode = pp._decode_hex_ssid
    calls = []

    def decode(value):
        calls.append(value)
        return real_decode(value)

    monkeypatch.setattr(pp, "_decode_hex_ssid", decode)
    token = "eduroam".encode().hex()
    frame = pd.DataFrame({"wlan.ssid": [token, "eduroam", None] * 1000})

    pp.normalize_ssid_column(frame)

    assert calls == [token]


def test_iowa_style_captures_resolve_same_bssid_despite_encoding_and_hidden_mix():
    aps = pd.DataFrame(
        {"Hostname": ["LVES-AP-01"], "Building-Room": ["ROOM1"]}
    )
    members = [
        ("24:1f:bd:53:f0:60", "VerizonWiFiAccess"),
        ("24:1f:bd:53:f0:61", "BYU Cougars WiFi"),
        ("24:1f:bd:53:f0:62", "<MISSING>"),
    ]
    parts = []
    for source_rank, repetitions in enumerate(((33, 34, 33), (40, 39, 21))):
        records = []
        for (bssid, name), count in zip(members, repetitions):
            token = name.encode().hex() if name != "<MISSING>" else name
            records.extend(
                {
                    "wlan.bssid": bssid,
                    "wlan.ssid": token,
                    "wlan.vs.extreme.ap_name": "LVES-AP-01",
                    "radiotap.channel.freq": 5220,
                    "wlan_radio.channel": 44,
                }
                for _ in range(count)
            )
        frame = pd.DataFrame(records)
        pp.normalize_ssid_column(frame)
        adapted = adapt_extreme_beacons(frame, source_rank=source_rank)
        parts.append(
            filter_configured_project_scope(
                adapted, aps, observed_ap_names=adapted["ap_name"]
            ).observations
        )

    result = resolve_project_radios(pd.concat(parts, ignore_index=True))

    assert result.counts.resolved_bssids == 3
    assert result.counts.conflict_bssids == 0
    assert result.identity_map["radio_group_id"].nunique() == 1
    assert result.identity_map["mapping_diagnostics"].eq("").all()
    assert result.identity_map["ssid"].iloc[:2].tolist() == [
        "VerizonWiFiAccess", "BYU Cougars WiFi"
    ]
    assert pd.isna(result.identity_map["ssid"].iloc[2])
