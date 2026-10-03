from __future__ import annotations

import process_pcapng as pp
from tshark_field_planner import (
    BEACON_DISPLAY_FILTER,
    CaptureExtractionRequirements,
    configured_ap_display_filter,
    plan_tshark_extraction,
)
from vendor_adapter_registry import BEACON_ADAPTER_REGISTRY


def _profile(*, erm: bool) -> pp.CaptureProfile:
    return pp.CaptureProfile(
        name="aruba_erm_type3" if erm else pp.CAPTURE_PROFILE_NATIVE,
        is_aruba_erm=erm,
        erm_type="aruba_erm_type3" if erm else None,
        detection_method="test",
    )


def test_native_plan_combines_common_and_all_vendor_fields_once():
    plan = pp.build_tshark_extraction_plan(_profile(erm=False))

    assert plan.capture_profile == pp.CAPTURE_PROFILE_NATIVE
    assert plan.decode_args == ()
    assert plan.display_filter == BEACON_DISPLAY_FILTER
    assert "frame.time_epoch" in plan.fields
    assert "aruba_erm.time" not in plan.fields
    assert plan.fields.count("wlan.vs.aruba.ap_name") == 1
    assert plan.fields.count("wlan.vs.extreme.ap_name") == 1
    assert plan.adapter_versions == (
        ("beacon.adapter/aruba-erm", 3),
        ("beacon.adapter/extreme-native", 3),
    )


def test_erm_plan_adds_decode_args_and_all_vendor_fields():
    plan = pp.build_tshark_extraction_plan(_profile(erm=True))

    assert plan.capture_profile == "aruba_erm_type3"
    assert plan.decode_args == (
        "-d",
        f"udp.port=={pp.ERM_UDP_PORT},aruba_erm",
        "-d",
        "aruba_erm.type,aruba_erm_type3",
    )
    assert "aruba_erm.time" in plan.fields
    assert "@aruba_erm.time" in plan.fields
    assert "frame.time_epoch" in plan.fields
    assert "wlan.vs.aruba.ap_name" in plan.fields
    assert "wlan.vs.extreme.ap_name" in plan.fields


def test_planner_preserves_order_and_deduplicates_field_union():
    requirements = CaptureExtractionRequirements(
        capture_profile="test_profile",
        fields=("frame.number", "wlan.vs.aruba.ap_name"),
    )

    plan = plan_tshark_extraction(
        requirements,
        BEACON_ADAPTER_REGISTRY,
    )

    assert plan.fields == (
        "frame.number",
        "wlan.vs.aruba.ap_name",
        "wlan.vs.extreme.ap_name",
    )


def test_project_filter_matches_all_supported_ap_name_fields_case_insensitively():
    display_filter = configured_ap_display_filter(
        BEACON_ADAPTER_REGISTRY,
        (" AP-B ", "ap-a", "AP-A"),
    )

    assert display_filter == (
        "wlan.fc.type_subtype == 8 && ("
        'lower(wlan.vs.aruba.ap_name) == "ap-a" || '
        'lower(wlan.vs.aruba.ap_name) == "ap-b" || '
        'lower(wlan.vs.extreme.ap_name) == "ap-a" || '
        'lower(wlan.vs.extreme.ap_name) == "ap-b")'
    )


def test_extraction_plan_can_apply_configured_project_filter():
    plan = pp.build_tshark_extraction_plan(
        _profile(erm=True),
        configured_ap_names=("ROOM-AP-01",),
    )

    assert plan.display_filter == (
        "wlan.fc.type_subtype == 8 && ("
        'lower(wlan.vs.aruba.ap_name) == "room-ap-01" || '
        'lower(wlan.vs.extreme.ap_name) == "room-ap-01")'
    )
