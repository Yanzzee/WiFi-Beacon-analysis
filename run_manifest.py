"""Validated JSON run-manifest foundation for corrected analysis outputs."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import uuid
from typing import Any, Iterable, Mapping

from tshark_field_planner import TsharkExtractionPlan


RESULT_SCHEMA_ID = "beacon.schema/analysis-results/v1"
CORRECTED_METRIC_ID = "beacon.metric/corrected-cu/v1"
MANIFEST_FILENAME = "manifest.json"

_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$"
)


class ManifestValidationError(ValueError):
    """The run manifest does not satisfy its durable logical contract."""


def _nonempty(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"{label} must be a non-empty string")
    return value.strip()


def _nonnegative_integer(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ManifestValidationError(f"{label} must be a non-negative integer")
    return value


def _utc_timestamp(value: Any, label: str) -> str:
    value = _nonempty(value, label)
    if _UTC_TIMESTAMP.fullmatch(value) is None:
        raise ManifestValidationError(
            f"{label} must be an ISO-8601 UTC timestamp ending in Z"
        )
    return value


def _json_compatible(value: Any, path: str = "manifest") -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ManifestValidationError(f"{path} cannot contain NaN or infinity")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _json_compatible(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ManifestValidationError(
                    f"{path} object keys must be non-empty strings"
                )
            _json_compatible(item, f"{path}.{key}")
        return
    raise ManifestValidationError(
        f"{path} contains unsupported JSON value {type(value).__name__}"
    )


def _range_record(start: str | None, end: str | None, label: str) -> dict[str, Any]:
    if (start is None) != (end is None):
        raise ManifestValidationError(
            f"{label} start and end must both be present or both be null"
        )
    if start is not None:
        start = _utc_timestamp(start, f"{label}.start")
        end = _utc_timestamp(end, f"{label}.end")
    return {"start": start, "end": end}


def build_capture_plan_record(
    *,
    source_path: str,
    source_bytes: int,
    source_sha256: str,
    capture_profile: str,
    profile_detection: str,
    erm_type: str | None,
    tshark_version: str,
    plan: TsharkExtractionPlan,
) -> dict[str, Any]:
    """Build durable provenance for one capture and its extraction plan."""

    if not isinstance(plan, TsharkExtractionPlan):
        raise TypeError("plan must be TsharkExtractionPlan")
    capture_profile = _nonempty(capture_profile, "capture_profile")
    if capture_profile != plan.capture_profile:
        raise ManifestValidationError(
            "capture_profile must match the extraction plan"
        )
    digest = str(source_sha256).strip().lower()
    if _SHA256.fullmatch(digest) is None:
        raise ManifestValidationError("source_sha256 must be 64 lowercase hex digits")
    if erm_type is not None:
        erm_type = _nonempty(erm_type, "erm_type")

    plan_content = {
        "capture_profile": plan.capture_profile,
        "display_filter": plan.display_filter,
        "fields": list(plan.fields),
        "adapter_versions": [
            {"identifier": identifier, "version": version}
            for identifier, version in plan.adapter_versions
        ],
    }
    encoded_plan = json.dumps(
        plan_content,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "source_path": _nonempty(source_path, "source_path"),
        "source_bytes": _nonnegative_integer(source_bytes, "source_bytes"),
        "source_sha256": digest,
        "capture_profile": capture_profile,
        "profile_detection": _nonempty(
            profile_detection,
            "profile_detection",
        ),
        "erm_type": erm_type,
        "tshark_version": _nonempty(tshark_version, "tshark_version"),
        "field_plan_sha256": hashlib.sha256(encoded_plan).hexdigest(),
        **plan_content,
    }


def _validate_stage(stage: Mapping[str, Any], position: int) -> dict[str, Any]:
    required = {"name", "start", "end", "status", "warning_count", "error_count"}
    missing = sorted(required.difference(stage))
    if missing:
        raise ManifestValidationError(
            f"stages[{position}] is missing: {', '.join(missing)}"
        )
    return {
        "name": _nonempty(stage["name"], f"stages[{position}].name"),
        "start": _utc_timestamp(stage["start"], f"stages[{position}].start"),
        "end": _utc_timestamp(stage["end"], f"stages[{position}].end"),
        "status": _nonempty(stage["status"], f"stages[{position}].status"),
        "warning_count": _nonnegative_integer(
            stage["warning_count"],
            f"stages[{position}].warning_count",
        ),
        "error_count": _nonnegative_integer(
            stage["error_count"],
            f"stages[{position}].error_count",
        ),
    }


def _validate_range(value: Any, label: str, *, nullable: bool) -> None:
    if not isinstance(value, Mapping):
        raise ManifestValidationError(f"{label} must be an object")
    if "start" not in value or "end" not in value:
        raise ManifestValidationError(f"{label} requires start and end")
    start = value["start"]
    end = value["end"]
    if not nullable and (start is None or end is None):
        raise ManifestValidationError(f"{label} cannot be null")
    _range_record(start, end, label)


def _validate_capture_plan(value: Any, position: int) -> None:
    label = f"capture_plans[{position}]"
    if not isinstance(value, Mapping):
        raise ManifestValidationError(f"{label} must be an object")
    required = {
        "source_path",
        "source_bytes",
        "source_sha256",
        "capture_profile",
        "profile_detection",
        "erm_type",
        "tshark_version",
        "field_plan_sha256",
        "display_filter",
        "fields",
        "adapter_versions",
    }
    missing = sorted(required.difference(value))
    if missing:
        raise ManifestValidationError(
            f"{label} is missing: {', '.join(missing)}"
        )
    _nonempty(value["source_path"], f"{label}.source_path")
    _nonnegative_integer(value["source_bytes"], f"{label}.source_bytes")
    if _SHA256.fullmatch(str(value["source_sha256"])) is None:
        raise ManifestValidationError(f"{label}.source_sha256 is invalid")
    _nonempty(value["capture_profile"], f"{label}.capture_profile")
    _nonempty(value["profile_detection"], f"{label}.profile_detection")
    _nonempty(value["tshark_version"], f"{label}.tshark_version")
    _nonempty(value["display_filter"], f"{label}.display_filter")
    if _SHA256.fullmatch(str(value["field_plan_sha256"])) is None:
        raise ManifestValidationError(f"{label}.field_plan_sha256 is invalid")
    if value["erm_type"] is not None:
        _nonempty(value["erm_type"], f"{label}.erm_type")
    fields = value["fields"]
    if not isinstance(fields, list) or not fields:
        raise ManifestValidationError(f"{label}.fields must be a non-empty list")
    normalized_fields = [
        _nonempty(field, f"{label}.fields[{index}]")
        for index, field in enumerate(fields)
    ]
    if len(normalized_fields) != len(set(normalized_fields)):
        raise ManifestValidationError(f"{label}.fields must be unique")
    adapters = value["adapter_versions"]
    if not isinstance(adapters, list):
        raise ManifestValidationError(f"{label}.adapter_versions must be a list")
    for adapter_position, adapter in enumerate(adapters):
        adapter_label = f"{label}.adapter_versions[{adapter_position}]"
        if not isinstance(adapter, Mapping):
            raise ManifestValidationError(f"{adapter_label} must be an object")
        _nonempty(adapter.get("identifier"), f"{adapter_label}.identifier")
        version = adapter.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise ManifestValidationError(
                f"{adapter_label}.version must be a positive integer"
            )

    plan_content = {
        "capture_profile": value["capture_profile"],
        "display_filter": value["display_filter"],
        "fields": normalized_fields,
        "adapter_versions": [dict(adapter) for adapter in adapters],
    }
    expected_hash = hashlib.sha256(
        json.dumps(
            plan_content,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if value["field_plan_sha256"] != expected_hash:
        raise ManifestValidationError(f"{label}.field_plan_sha256 does not match")


def _validate_project(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ManifestValidationError("project must be an object")
    name = _nonempty(value.get("name"), "project.name")
    slug = _nonempty(value.get("slug"), "project.slug")
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", slug) is None:
        raise ManifestValidationError("project.slug must be a safe lowercase slug")
    return {"name": name, "slug": slug}


def _validate_input_set(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ManifestValidationError("input_set must be an object")
    digest = str(value.get("sha256", ""))
    if _SHA256.fullmatch(digest) is None:
        raise ManifestValidationError("input_set.sha256 is invalid")
    files = value.get("files")
    if not isinstance(files, list) or not files:
        raise ManifestValidationError("input_set.files must be a non-empty list")

    normalized: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()
    for position, item in enumerate(files):
        label = f"input_set.files[{position}]"
        if not isinstance(item, Mapping):
            raise ManifestValidationError(f"{label} must be an object")
        role = _nonempty(item.get("role"), f"{label}.role")
        path = _nonempty(item.get("path"), f"{label}.path").replace("\\", "/")
        while "//" in path:
            path = path.replace("//", "/")
        if path in {".", ".."} or ".." in path.split("/"):
            raise ManifestValidationError(
                f"{label}.path cannot traverse parent directories"
            )
        size = _nonnegative_integer(item.get("bytes"), f"{label}.bytes")
        file_digest = str(item.get("sha256", ""))
        if _SHA256.fullmatch(file_digest) is None:
            raise ManifestValidationError(f"{label}.sha256 is invalid")
        identity = (role, path)
        if identity in identities:
            raise ManifestValidationError(
                f"input_set contains duplicate role/path: {role} {path}"
            )
        identities.add(identity)
        normalized.append(
            {
                "role": role,
                "path": path,
                "bytes": size,
                "sha256": file_digest,
            }
        )
    normalized.sort(key=lambda item: (item["role"], item["path"], item["sha256"]))
    expected = hashlib.sha256(
        json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if digest != expected:
        raise ManifestValidationError("input_set.sha256 does not match its files")
    return {"sha256": digest, "files": normalized}


def _validate_code_provenance(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ManifestValidationError("code_provenance must be an object")
    source = _nonempty(value.get("source"), "code_provenance.source")
    revision = value.get("revision")
    if revision is not None:
        revision = _nonempty(revision, "code_provenance.revision")
    dirty = value.get("dirty")
    if dirty is not None and not isinstance(dirty, bool):
        raise ManifestValidationError("code_provenance.dirty must be boolean or null")
    if source == "unavailable" and (revision is not None or dirty is not None):
        raise ManifestValidationError(
            "unavailable code provenance cannot claim revision or dirty state"
        )
    return {"source": source, "revision": revision, "dirty": dirty}


def build_run_manifest(
    *,
    project_name: str,
    project_slug: str,
    run_id: str,
    input_set: Mapping[str, Any],
    code_provenance: Mapping[str, Any],
    requested_start: str,
    requested_end: str,
    actual_selected_start: str | None,
    actual_selected_end: str | None,
    effective_configuration: Mapping[str, Any],
    source_precedence: Iterable[str],
    storage: Mapping[str, Any],
    capture_plans: Iterable[Mapping[str, Any]],
    stages: Iterable[Mapping[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
    detailed_log: str | None = None,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> dict[str, Any]:
    """Build the authoritative small manifest for one corrected run."""

    run_id = _nonempty(run_id, "run_id")
    if _SAFE_RUN_ID.fullmatch(run_id) is None:
        raise ManifestValidationError("run_id must be one safe path segment")
    precedence = [
        _nonempty(value, f"source_precedence[{index}]")
        for index, value in enumerate(source_precedence)
    ]
    if len(precedence) != len(set(precedence)):
        raise ManifestValidationError("source_precedence values must be unique")
    normalized_stages = [
        _validate_stage(stage, position)
        for position, stage in enumerate(stages)
    ]
    normalized_capture_plans = [dict(value) for value in capture_plans]
    normalized_artifacts = {
        _nonempty(name, "artifact name"): dict(value)
        for name, value in artifacts.items()
    }
    if detailed_log is not None:
        detailed_log = _nonempty(detailed_log, "detailed_log")

    manifest = {
        "project": _validate_project(
            {"name": project_name, "slug": project_slug}
        ),
        "run_id": run_id,
        "input_set": _validate_input_set(input_set),
        "code_provenance": _validate_code_provenance(code_provenance),
        "schema_id": _nonempty(schema_id, "schema_id"),
        "metric_id": _nonempty(metric_id, "metric_id"),
        "requested_sample_range": _range_record(
            requested_start,
            requested_end,
            "requested_sample_range",
        ),
        "actual_selected_observation_range": _range_record(
            actual_selected_start,
            actual_selected_end,
            "actual_selected_observation_range",
        ),
        "effective_configuration": dict(effective_configuration),
        "source_precedence": precedence,
        "storage": dict(storage),
        "capture_plans": normalized_capture_plans,
        "stages": normalized_stages,
        "detailed_log": detailed_log,
        "artifacts": normalized_artifacts,
        "legacy_artifacts": [],
    }
    validate_run_manifest(manifest)
    return manifest


def validate_run_manifest(
    manifest: Mapping[str, Any],
    *,
    require_capture_plans: bool = True,
) -> None:
    """Validate required manifest identity, ranges, stages, and artifacts."""

    if not isinstance(manifest, Mapping):
        raise ManifestValidationError("manifest must be a JSON object")
    required = {
        "project",
        "run_id",
        "input_set",
        "code_provenance",
        "schema_id",
        "metric_id",
        "requested_sample_range",
        "actual_selected_observation_range",
        "effective_configuration",
        "source_precedence",
        "storage",
        "stages",
        "detailed_log",
        "artifacts",
        "legacy_artifacts",
    }
    if require_capture_plans:
        required.add("capture_plans")
    missing = sorted(required.difference(manifest))
    if missing:
        raise ManifestValidationError(
            "manifest is missing required fields: " + ", ".join(missing)
        )
    run_id = _nonempty(manifest["run_id"], "run_id")
    if _SAFE_RUN_ID.fullmatch(run_id) is None:
        raise ManifestValidationError("run_id must be one safe path segment")
    _validate_project(manifest["project"])
    _validate_input_set(manifest["input_set"])
    _validate_code_provenance(manifest["code_provenance"])
    _nonempty(manifest["schema_id"], "schema_id")
    _nonempty(manifest["metric_id"], "metric_id")
    _validate_range(
        manifest["requested_sample_range"],
        "requested_sample_range",
        nullable=False,
    )
    _validate_range(
        manifest["actual_selected_observation_range"],
        "actual_selected_observation_range",
        nullable=True,
    )
    if not isinstance(manifest["effective_configuration"], Mapping):
        raise ManifestValidationError("effective_configuration must be an object")
    precedence = manifest["source_precedence"]
    if not isinstance(precedence, list):
        raise ManifestValidationError("source_precedence must be a list")
    normalized_precedence = [
        _nonempty(value, f"source_precedence[{index}]")
        for index, value in enumerate(precedence)
    ]
    if len(normalized_precedence) != len(set(normalized_precedence)):
        raise ManifestValidationError("source_precedence values must be unique")
    if not isinstance(manifest["storage"], Mapping):
        raise ManifestValidationError("storage must be an object")
    detailed_log = manifest["detailed_log"]
    if detailed_log is not None:
        _nonempty(detailed_log, "detailed_log")
    if manifest["legacy_artifacts"] != []:
        raise ManifestValidationError("legacy_artifacts must remain empty")
    if not isinstance(manifest["stages"], list):
        raise ManifestValidationError("stages must be a list")
    for position, stage in enumerate(manifest["stages"]):
        if not isinstance(stage, Mapping):
            raise ManifestValidationError(f"stages[{position}] must be an object")
        _validate_stage(stage, position)
    if not isinstance(manifest["artifacts"], Mapping):
        raise ManifestValidationError("artifacts must be an object")
    for name, artifact in manifest["artifacts"].items():
        _nonempty(name, "artifact name")
        if not isinstance(artifact, Mapping):
            raise ManifestValidationError(f"artifact {name!r} must be an object")
        _nonempty(artifact.get("path"), f"artifacts.{name}.path")
        if "rows" in artifact:
            _nonnegative_integer(artifact["rows"], f"artifacts.{name}.rows")
    if require_capture_plans:
        if not isinstance(manifest["capture_plans"], list):
            raise ManifestValidationError("capture_plans must be a list")
        if not manifest["capture_plans"]:
            raise ManifestValidationError("capture_plans cannot be empty")
        for position, capture_plan in enumerate(manifest["capture_plans"]):
            _validate_capture_plan(capture_plan, position)
    _json_compatible(dict(manifest))


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestValidationError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_run_manifest(
    path: str | Path,
    *,
    require_capture_plans: bool = True,
) -> dict[str, Any]:
    """Load JSON while rejecting duplicate object keys, then validate it."""

    with Path(path).open("r", encoding="utf-8") as source:
        manifest = json.load(source, object_pairs_hook=_reject_duplicate_keys)
    validate_run_manifest(
        manifest,
        require_capture_plans=require_capture_plans,
    )
    return manifest


def write_run_manifest(
    manifest: Mapping[str, Any],
    *,
    output_root: str | Path | None = None,
    run_directory: str | Path | None = None,
) -> Path:
    """Atomically write one manifest to a final or staged run directory."""

    validate_run_manifest(manifest)
    run_id = str(manifest["run_id"])
    if (output_root is None) == (run_directory is None):
        raise ValueError("provide exactly one of output_root or run_directory")
    destination = (
        Path(run_directory)
        if run_directory is not None
        else Path(output_root) / run_id
    )
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / MANIFEST_FILENAME
    if target.exists():
        raise FileExistsError(f"run manifest already exists: {target}")
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        rendered = json.dumps(
            dict(manifest),
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        ) + "\n"
        temporary.write_text(rendered, encoding="utf-8", newline="\n")
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return target
