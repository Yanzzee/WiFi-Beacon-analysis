from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

import run_lifecycle as rl
import run_manifest as rm
from tshark_field_planner import TsharkExtractionPlan


def _input_set(tmp_path: Path, *, content: bytes = b"capture") -> rl.InputSetProvenance:
    capture = tmp_path / "capture.pcapng"
    schedule = tmp_path / "classes.csv"
    capture.write_bytes(content)
    schedule.write_text("class,start,end\nC1,10:00,11:00\n", encoding="utf-8")
    return rl.hash_analysis_inputs(
        [
            rl.AnalysisInput("capture", capture, "data/capture.pcapng"),
            rl.AnalysisInput("class_schedule", schedule, "data/classes.csv"),
        ]
    )


def _capture_plan(input_set: rl.InputSetProvenance) -> dict[str, object]:
    capture = next(item for item in input_set.files if item.role == "capture")
    plan = TsharkExtractionPlan(
        capture_profile="native_80211",
        fields=("frame.time_epoch", "wlan.bssid"),
        decode_args=(),
        display_filter="wlan.fc.type_subtype == 8",
        adapter_versions=(("beacon.adapter/extreme-native", 3),),
    )
    return rm.build_capture_plan_record(
        source_path=capture.path,
        source_bytes=capture.bytes,
        source_sha256=capture.sha256,
        capture_profile=plan.capture_profile,
        profile_detection="test",
        erm_type=None,
        tshark_version="4.6.6",
        plan=plan,
    )


def _write_publishable_manifest(workspace: rl.RunWorkspace) -> Path:
    artifact = workspace.staging_directory / "project_identity_map.parquet"
    artifact.write_bytes(b"parquet-placeholder")
    logical_path = (
        f"output/runs/{workspace.project_slug}/{workspace.run_id}/"
        "project_identity_map.parquet"
    )
    manifest = rm.build_run_manifest(
        project_name=workspace.project_name,
        project_slug=workspace.project_slug,
        run_id=workspace.run_id,
        input_set=workspace.input_set.as_manifest_record(),
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
        requested_start="2026-09-23T15:00:00Z",
        requested_end="2026-09-23T16:00:00Z",
        actual_selected_start=None,
        actual_selected_end=None,
        effective_configuration={"bin_width_us": 1_024_000},
        source_precedence=["valid_frame_time_epoch"],
        storage={"timestamp": "timestamp_ns_utc"},
        capture_plans=[_capture_plan(workspace.input_set)],
        stages=[
            {
                "name": "test",
                "start": "2026-09-23T16:00:00Z",
                "end": "2026-09-23T16:00:01Z",
                "status": "completed",
                "warning_count": 0,
                "error_count": 0,
            }
        ],
        artifacts={
            "project_identity_map": {"path": logical_path, "rows": 1}
        },
        detailed_log=(
            f"output/runs/{workspace.project_slug}/_logs/"
            f"{workspace.run_id}.jsonl"
        ),
    )
    return rm.write_run_manifest(
        manifest, run_directory=workspace.staging_directory
    )


def _events(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_project_slug_is_readable_portable_and_bounded():
    assert rl.project_slug(" Fall 2026 / Main ") == "fall-2026-main"
    assert rl.project_slug("CON") == "project-con"
    assert rl.project_slug("无线项目").startswith("project-")
    assert len(rl.project_slug("A" * 100)) <= 64


def test_input_hash_is_order_independent_and_content_sensitive(tmp_path: Path):
    first = _input_set(tmp_path)
    reversed_inputs = rl.hash_analysis_inputs(
        [
            rl.AnalysisInput(
                "class_schedule", tmp_path / "classes.csv", "data/classes.csv"
            ),
            rl.AnalysisInput(
                "capture", tmp_path / "capture.pcapng", "data/capture.pcapng"
            ),
        ]
    )
    assert first == reversed_inputs
    assert len(first.sha256) == 64

    changed = _input_set(tmp_path, content=b"changed capture")
    assert changed.sha256 != first.sha256


def test_run_id_uses_utc_microseconds_and_input_hash_prefix(tmp_path: Path):
    inputs = _input_set(tmp_path)
    started = datetime(2026, 9, 23, 18, 45, 30, 123456, tzinfo=timezone.utc)

    assert rl.generate_run_id(inputs.sha256, started_at=started) == (
        f"20260923T184530123456Z-{inputs.sha256[:8]}"
    )


def test_run_is_staged_logged_and_published_atomically(tmp_path: Path):
    inputs = _input_set(tmp_path)
    workspace = rl.begin_run(
        output_root=tmp_path / "output" / "runs",
        project_name="Fall 2026",
        input_set=inputs,
        started_at=datetime(2026, 9, 23, 18, 45, 30, tzinfo=timezone.utc),
    )

    assert workspace.project_directory.name == "fall-2026"
    assert workspace.staging_directory.name == f".{workspace.run_id}.staging"
    assert (workspace.staging_directory / rl.STAGING_MARKER).is_file()
    _write_publishable_manifest(workspace)

    published = rl.publish_run(workspace)

    assert published == workspace.final_directory
    assert published.is_dir()
    assert not workspace.staging_directory.exists()
    assert not (published / rl.STAGING_MARKER).exists()
    assert rm.load_run_manifest(published / "manifest.json")["run_id"] == (
        workspace.run_id
    )
    assert [event["event"] for event in _events(workspace.log_path)] == [
        "run_started",
        "run_published",
    ]


def test_publication_requires_every_referenced_staged_artifact(tmp_path: Path):
    workspace = rl.begin_run(
        output_root=tmp_path / "runs",
        project_name="Missing Artifact",
        input_set=_input_set(tmp_path),
        started_at=datetime(2026, 9, 23, 18, 45, 30, tzinfo=timezone.utc),
    )
    manifest_path = _write_publishable_manifest(workspace)
    (workspace.staging_directory / "project_identity_map.parquet").unlink()

    with pytest.raises(rl.RunLifecycleError, match="does not exist"):
        rl.publish_run(workspace)
    assert manifest_path.is_file()
    assert workspace.staging_directory.is_dir()
    rl.record_run_failure(workspace, reason="test cleanup", understood=True)


def test_understood_failure_is_logged_and_removes_staging(tmp_path: Path):
    workspace = rl.begin_run(
        output_root=tmp_path / "runs",
        project_name="Failure Test",
        input_set=_input_set(tmp_path),
        started_at=datetime(2026, 9, 23, 18, 45, 30, tzinfo=timezone.utc),
    )

    rl.record_run_failure(
        workspace, reason="unsupported capture profile", understood=True
    )

    assert not workspace.staging_directory.exists()
    assert not workspace.final_directory.exists()
    failure = _events(workspace.log_path)[-1]
    assert failure["event"] == "run_failed"
    assert failure["understood"] is True


def test_next_run_removes_only_abandoned_owned_staging(tmp_path: Path):
    output_root = tmp_path / "runs"
    first = rl.begin_run(
        output_root=output_root,
        project_name="Cleanup Test",
        input_set=_input_set(tmp_path),
        started_at=datetime(2026, 9, 23, 18, 45, 30, tzinfo=timezone.utc),
    )
    rl.record_run_failure(first, reason="unexpected failure", understood=False)
    marker_path = first.staging_directory / rl.STAGING_MARKER
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["process_id"] = 2_147_483_647
    marker_path.write_text(json.dumps(marker), encoding="utf-8")

    unowned = first.project_directory / ".unowned.staging"
    unowned.mkdir()
    (unowned / "keep.txt").write_text("not ours", encoding="utf-8")

    second = rl.begin_run(
        output_root=output_root,
        project_name="Cleanup Test",
        input_set=first.input_set,
        started_at=datetime(2026, 9, 23, 18, 45, 31, tzinfo=timezone.utc),
    )

    assert not first.staging_directory.exists()
    assert unowned.is_dir()
    assert second.staging_directory.is_dir()
    assert "abandoned_staging_removed" in {
        event["event"] for event in _events(first.log_path)
    }
    rl.record_run_failure(second, reason="test cleanup", understood=True)


def test_active_staging_collision_fails_without_deleting_it(tmp_path: Path):
    inputs = _input_set(tmp_path)
    started = datetime(2026, 9, 23, 18, 45, 30, tzinfo=timezone.utc)
    first = rl.begin_run(
        output_root=tmp_path / "runs",
        project_name="Collision Test",
        input_set=inputs,
        started_at=started,
    )

    with pytest.raises(FileExistsError, match="already exists"):
        rl.begin_run(
            output_root=tmp_path / "runs",
            project_name="Collision Test",
            input_set=inputs,
            started_at=started,
        )
    assert first.staging_directory.is_dir()
    rl.record_run_failure(first, reason="test cleanup", understood=True)


def test_git_provenance_reports_current_repository():
    provenance = rl.detect_git_code_provenance(Path(__file__).resolve().parents[1])

    assert provenance["source"] == "git"
    assert len(provenance["revision"]) in {40, 64}
    assert isinstance(provenance["dirty"], bool)
