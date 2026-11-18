from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import process_pcapng as pp
import run_manifest as rm


REPO_ROOT = Path(__file__).resolve().parents[1]
ORACLE_MANIFEST = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "corrected_semantics"
    / "expected"
    / "manifest.json"
)


def _capture_plan() -> dict[str, object]:
    profile = pp.CaptureProfile(
        name=pp.CAPTURE_PROFILE_NATIVE,
        is_aruba_erm=False,
        erm_type=None,
        detection_method="test",
    )
    plan = pp.build_tshark_extraction_plan(profile)
    return rm.build_capture_plan_record(
        source_path="data/captures_test/example.pcapng",
        source_bytes=1234,
        source_sha256="a" * 64,
        capture_profile=profile.name,
        profile_detection="content_probe",
        erm_type=None,
        tshark_version="4.6.6",
        plan=plan,
    )


def _input_set() -> dict[str, object]:
    files = [
        {
            "role": "capture",
            "path": "data/captures_test/example.pcapng",
            "bytes": 1234,
            "sha256": "a" * 64,
        }
    ]
    digest = hashlib.sha256(
        json.dumps(files, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return {"sha256": digest, "files": files}


def _manifest() -> dict[str, object]:
    return rm.build_run_manifest(
        project_name="Test Project",
        project_slug="test-project",
        run_id="run-20260923",
        input_set=_input_set(),
        code_provenance={
            "source": "git",
            "revision": "b" * 40,
            "dirty": False,
        },
        requested_start="2026-09-23T15:00:00.000000000Z",
        requested_end="2026-09-23T16:00:00.000000000Z",
        actual_selected_start="2026-09-23T15:00:00.100000000Z",
        actual_selected_end="2026-09-23T15:59:59.900000000Z",
        effective_configuration={"bin_width_us": 1_024_000},
        source_precedence=["valid_aruba_erm_time", "valid_frame_time_epoch"],
        storage={"timestamp": "timestamp_ns_utc"},
        capture_plans=[_capture_plan()],
        stages=[
            {
                "name": "extract",
                "start": "2026-09-23T16:01:00.000000000Z",
                "end": "2026-09-23T16:01:01.000000000Z",
                "status": "completed",
                "warning_count": 0,
                "error_count": 0,
            }
        ],
        artifacts={
            "project_identity_map": {
                "path": "output/runs/test-project/run-20260923/project_identity_map.parquet",
                "rows": 4,
            }
        },
    )


def test_capture_plan_records_exact_dynamic_field_provenance():
    record = _capture_plan()

    assert record["capture_profile"] == pp.CAPTURE_PROFILE_NATIVE
    assert record["display_filter"] == "wlan.fc.type_subtype == 8"
    assert "wlan.vs.aruba.ap_name" in record["fields"]
    assert "wlan.vs.extreme.ap_name" in record["fields"]
    assert record["adapter_versions"] == [
        {"identifier": "beacon.adapter/aruba-erm", "version": 3},
        {"identifier": "beacon.adapter/extreme-native", "version": 3},
    ]
    assert len(record["field_plan_sha256"]) == 64


def test_manifest_writes_atomically_and_round_trips(tmp_path: Path):
    manifest = _manifest()

    path = rm.write_run_manifest(manifest, output_root=tmp_path)
    loaded = rm.load_run_manifest(path)

    assert path == tmp_path / "run-20260923" / "manifest.json"
    assert loaded == manifest
    assert list(path.parent.glob(".manifest.json.*.tmp")) == []
    with pytest.raises(FileExistsError, match="already exists"):
        rm.write_run_manifest(manifest, output_root=tmp_path)


def test_manifest_atomic_failure_removes_temporary_file(
    tmp_path: Path,
    monkeypatch,
):
    def fail_replace(_source, _target):
        raise OSError("synthetic replace failure")

    monkeypatch.setattr(rm.os, "replace", fail_replace)

    with pytest.raises(OSError, match="synthetic replace failure"):
        rm.write_run_manifest(_manifest(), output_root=tmp_path)

    run_directory = tmp_path / "run-20260923"
    assert not (run_directory / "manifest.json").exists()
    assert list(run_directory.glob(".manifest.json.*.tmp")) == []


def test_loader_rejects_duplicate_json_keys(tmp_path: Path):
    path = tmp_path / "duplicate.json"
    path.write_text(
        '{"run_id":"one","run_id":"two"}',
        encoding="utf-8",
    )

    with pytest.raises(rm.ManifestValidationError, match="duplicate.*run_id"):
        rm.load_run_manifest(path)


def test_manifest_requires_explicit_utc_timestamp_strings():
    with pytest.raises(rm.ManifestValidationError, match="ending in Z"):
        rm.build_run_manifest(
            project_name="Test Project",
            project_slug="test-project",
            run_id="bad-time",
            input_set=_input_set(),
            code_provenance={
                "source": "unavailable",
                "revision": None,
                "dirty": None,
            },
            requested_start="2026-09-23 15:00:00",
            requested_end="2026-09-23T16:00:00Z",
            actual_selected_start=None,
            actual_selected_end=None,
            effective_configuration={},
            source_precedence=[],
            storage={},
            capture_plans=[_capture_plan()],
            stages=[],
            artifacts={},
        )


def test_manifest_rejects_tampered_input_set_and_invalid_project_slug():
    manifest = _manifest()
    manifest["input_set"]["sha256"] = "f" * 64
    with pytest.raises(rm.ManifestValidationError, match="does not match"):
        rm.validate_run_manifest(manifest)

    manifest = _manifest()
    manifest["project"]["slug"] = "../outside"
    with pytest.raises(rm.ManifestValidationError, match="safe lowercase slug"):
        rm.validate_run_manifest(manifest)


def test_existing_corrected_oracle_is_valid_legacy_fixture_manifest():
    manifest = rm.load_run_manifest(
        ORACLE_MANIFEST,
        require_capture_plans=False,
    )

    assert manifest["run_id"] == "oracle-corrected-v1"
    assert manifest["project"] == {
        "name": "Corrected Semantics Oracle",
        "slug": "oracle-corrected",
    }
    assert manifest["schema_id"] == rm.RESULT_SCHEMA_ID
    assert manifest["metric_id"] == "beacon.metric/corrected-cu/v1"
    assert manifest["legacy_artifacts"] == []
