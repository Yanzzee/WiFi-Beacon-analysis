"""Create-only lifecycle support for corrected analysis runs.

This module owns project/run naming, input provenance, hidden staging,
failure logs, abandoned-staging cleanup, and atomic publication.  It does not
run analysis stages or interpret beacon data.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import unicodedata
from typing import Any, Iterable, Mapping

from run_manifest import load_run_manifest


STAGING_MARKER = ".beacon-analysis-staging.json"
STAGING_MARKER_FORMAT = "beacon.run-staging/v1"
LOG_DIRECTORY = "_logs"
INPUT_HASH_PREFIX_LENGTH = 8

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_SLUG = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SAFE_RUN_ID = re.compile(
    r"^\d{8}T\d{12}Z-[0-9a-f]{8}$"
)
_WINDOWS_RESERVED = {
    "con",
    "prn",
    "aux",
    "nul",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
}


class RunLifecycleError(RuntimeError):
    """A run cannot safely transition through its create-only lifecycle."""


@dataclass(frozen=True)
class AnalysisInput:
    """One analysis-defining input and its durable manifest identity."""

    role: str
    path: Path
    reference: str


@dataclass(frozen=True)
class InputFileProvenance:
    role: str
    path: str
    bytes: int
    sha256: str


@dataclass(frozen=True)
class InputSetProvenance:
    sha256: str
    files: tuple[InputFileProvenance, ...]

    def as_manifest_record(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "files": [asdict(item) for item in self.files],
        }


@dataclass(frozen=True)
class RunWorkspace:
    project_name: str
    project_slug: str
    run_id: str
    input_set: InputSetProvenance
    project_directory: Path
    staging_directory: Path
    final_directory: Path
    log_path: Path


def _nonempty(value: str, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def project_slug(project_name: str) -> str:
    """Return a readable, portable path segment for a project display name."""

    name = _nonempty(project_name, name="project_name")
    ascii_name = unicodedata.normalize("NFKD", name).encode(
        "ascii", "ignore"
    ).decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    if not slug:
        suffix = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
        slug = f"project-{suffix}"
    if slug in _WINDOWS_RESERVED:
        slug = f"project-{slug}"
    if len(slug) > 64:
        suffix = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
        slug = f"{slug[:55].rstrip('-')}-{suffix}"
    if _SAFE_SLUG.fullmatch(slug) is None:
        raise ValueError("project name could not be converted to a safe slug")
    return slug


def _normalized_reference(value: str) -> str:
    reference = _nonempty(value, name="input reference").replace("\\", "/")
    while "//" in reference:
        reference = reference.replace("//", "/")
    if reference in {".", ".."} or any(
        part == ".." for part in reference.split("/")
    ):
        raise ValueError("input reference cannot traverse parent directories")
    return reference


def sha256_file(path: str | Path, *, chunk_bytes: int = 1024 * 1024) -> str:
    """Hash a file incrementally so large captures are not loaded into RAM."""

    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be positive")
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"analysis input is not a file: {source}")
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        while chunk := handle.read(chunk_bytes):
            digest.update(chunk)
    return digest.hexdigest()


def hash_analysis_inputs(
    inputs: Iterable[AnalysisInput],
) -> InputSetProvenance:
    """Hash every input and then hash the deterministic provenance record."""

    records: list[InputFileProvenance] = []
    identities: set[tuple[str, str]] = set()
    for item in inputs:
        if not isinstance(item, AnalysisInput):
            raise TypeError("inputs must contain AnalysisInput values")
        role = _nonempty(item.role, name="input role")
        reference = _normalized_reference(item.reference)
        identity = (role, reference)
        if identity in identities:
            raise ValueError(
                f"duplicate analysis input role/reference: {role} {reference}"
            )
        identities.add(identity)
        path = Path(item.path)
        if not path.is_file():
            raise FileNotFoundError(f"analysis input is not a file: {path}")
        before = path.stat()
        file_digest = sha256_file(path)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (
            after.st_size,
            after.st_mtime_ns,
        ):
            raise RunLifecycleError(
                f"analysis input changed while it was being hashed: {path}"
            )
        records.append(
            InputFileProvenance(
                role=role,
                path=reference,
                bytes=after.st_size,
                sha256=file_digest,
            )
        )
    if not records:
        raise ValueError("at least one analysis input is required")

    ordered = tuple(
        sorted(records, key=lambda item: (item.role, item.path, item.sha256))
    )
    encoded = json.dumps(
        [asdict(item) for item in ordered],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return InputSetProvenance(
        sha256=hashlib.sha256(encoded).hexdigest(),
        files=ordered,
    )


def generate_run_id(
    input_set_sha256: str,
    *,
    started_at: datetime | None = None,
) -> str:
    """Generate the accepted UTC-time plus input-hash-prefix run ID."""

    digest = str(input_set_sha256).strip().lower()
    if _SHA256.fullmatch(digest) is None:
        raise ValueError("input_set_sha256 must be 64 lowercase hex digits")
    instant = started_at or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        raise ValueError("started_at must be timezone-aware")
    utc = instant.astimezone(timezone.utc)
    timestamp = utc.strftime("%Y%m%dT%H%M%S") + f"{utc.microsecond:06d}Z"
    run_id = f"{timestamp}-{digest[:INPUT_HASH_PREFIX_LENGTH]}"
    if _SAFE_RUN_ID.fullmatch(run_id) is None:
        raise AssertionError("generated run ID violated its internal contract")
    return run_id


def detect_git_code_provenance(repository: str | Path) -> dict[str, Any]:
    """Return available Git revision/dirty provenance without modifying it."""

    root = Path(repository)
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return {"source": "unavailable", "revision": None, "dirty": None}
    return {
        "source": "git",
        "revision": revision,
        "dirty": bool(status.strip()),
    }


def _utc_text(instant: datetime | None = None) -> str:
    value = instant or datetime.now(timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _write_json_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(dict(value), handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def _append_log(path: Path, event: str, **details: Any) -> None:
    record = {"time": _utc_text(), "event": event, **details}
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")


def _read_owned_marker(path: Path, *, project_slug_value: str) -> dict[str, Any] | None:
    is_junction = getattr(path, "is_junction", lambda: False)
    if path.is_symlink() or is_junction() or not path.is_dir():
        return None
    marker_path = path / STAGING_MARKER
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(marker, dict):
        return None
    run_id = marker.get("run_id")
    process_id = marker.get("process_id")
    if (
        marker.get("format") != STAGING_MARKER_FORMAT
        or marker.get("project_slug") != project_slug_value
        or not isinstance(run_id, str)
        or path.name != f".{run_id}.staging"
        or _SAFE_RUN_ID.fullmatch(run_id) is None
        or not isinstance(process_id, int)
        or isinstance(process_id, bool)
        or process_id <= 0
    ):
        return None
    return marker


def _process_is_running(process_id: int) -> bool:
    if process_id == os.getpid():
        return True
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def cleanup_abandoned_staging(project_directory: str | Path) -> tuple[str, ...]:
    """Remove only marker-proven staging children of one project directory."""

    project_path = Path(project_directory)
    if not project_path.exists():
        return ()
    if project_path.is_symlink() or not project_path.is_dir():
        raise RunLifecycleError("project directory must be a real directory")
    slug = project_path.name
    if _SAFE_SLUG.fullmatch(slug) is None:
        raise RunLifecycleError("project directory does not have a safe slug")
    logs = project_path / LOG_DIRECTORY
    logs.mkdir(exist_ok=True)
    removed: list[str] = []
    for candidate in sorted(project_path.glob(".*.staging")):
        marker = _read_owned_marker(candidate, project_slug_value=slug)
        if marker is None:
            continue
        if _process_is_running(int(marker["process_id"])):
            continue
        run_id = str(marker["run_id"])
        _append_log(
            logs / f"{run_id}.jsonl",
            "abandoned_staging_removed",
            staging_directory=candidate.name,
        )
        shutil.rmtree(candidate)
        removed.append(run_id)
    return tuple(removed)


def begin_run(
    *,
    output_root: str | Path,
    project_name: str,
    input_set: InputSetProvenance,
    started_at: datetime | None = None,
) -> RunWorkspace:
    """Create one owned hidden staging directory for a new run."""

    name = _nonempty(project_name, name="project_name")
    slug = project_slug(name)
    if not isinstance(input_set, InputSetProvenance):
        raise TypeError("input_set must be InputSetProvenance")
    run_id = generate_run_id(input_set.sha256, started_at=started_at)
    project_directory = Path(output_root) / slug
    project_directory.mkdir(parents=True, exist_ok=True)
    cleanup_abandoned_staging(project_directory)

    final_directory = project_directory / run_id
    staging_directory = project_directory / f".{run_id}.staging"
    logs = project_directory / LOG_DIRECTORY
    logs.mkdir(exist_ok=True)
    if logs.is_symlink() or not logs.is_dir():
        raise RunLifecycleError("project log path must be a real directory")
    log_path = logs / f"{run_id}.jsonl"
    if final_directory.exists() or staging_directory.exists():
        raise FileExistsError(f"run path already exists: {final_directory}")
    if log_path.exists():
        raise FileExistsError(f"run log already exists: {log_path}")
    staging_directory.mkdir()
    marker = {
        "format": STAGING_MARKER_FORMAT,
        "project_name": name,
        "project_slug": slug,
        "run_id": run_id,
        "input_set_sha256": input_set.sha256,
        "process_id": os.getpid(),
        "created_at": _utc_text(started_at),
    }
    try:
        _write_json_exclusive(staging_directory / STAGING_MARKER, marker)
        _append_log(
            log_path,
            "run_started",
            staging_directory=staging_directory.name,
        )
    except BaseException:
        shutil.rmtree(staging_directory, ignore_errors=True)
        raise
    return RunWorkspace(
        project_name=name,
        project_slug=slug,
        run_id=run_id,
        input_set=input_set,
        project_directory=project_directory,
        staging_directory=staging_directory,
        final_directory=final_directory,
        log_path=log_path,
    )


def record_run_failure(
    workspace: RunWorkspace,
    *,
    reason: str,
    understood: bool,
) -> None:
    """Log a failed execution and clean staging only for understood failures."""

    message = _nonempty(reason, name="failure reason")
    _append_log(
        workspace.log_path,
        "run_failed",
        reason=message,
        understood=bool(understood),
    )
    if understood:
        marker = _read_owned_marker(
            workspace.staging_directory,
            project_slug_value=workspace.project_slug,
        )
        if marker is None or marker.get("run_id") != workspace.run_id:
            raise RunLifecycleError(
                "refusing to remove staging without its ownership marker"
            )
        shutil.rmtree(workspace.staging_directory)


def record_run_event(
    workspace: RunWorkspace,
    *,
    event: str,
    **details: Any,
) -> None:
    """Append one structured informational event to an active run log."""

    if not isinstance(workspace, RunWorkspace):
        raise TypeError("workspace must be a RunWorkspace")
    event_name = _nonempty(event, name="event")
    _append_log(workspace.log_path, event_name, **details)


def publish_run(workspace: RunWorkspace) -> Path:
    """Validate the final manifest and atomically publish the staged run."""

    if workspace.final_directory.exists():
        raise FileExistsError(
            f"final run directory already exists: {workspace.final_directory}"
        )
    marker = _read_owned_marker(
        workspace.staging_directory,
        project_slug_value=workspace.project_slug,
    )
    if marker is None or marker.get("run_id") != workspace.run_id:
        raise RunLifecycleError("staging ownership marker is missing or invalid")

    manifest_path = workspace.staging_directory / "manifest.json"
    manifest = load_run_manifest(manifest_path)
    project = manifest.get("project")
    input_set = manifest.get("input_set")
    if manifest.get("run_id") != workspace.run_id:
        raise RunLifecycleError("manifest run_id does not match staging")
    if not isinstance(project, Mapping) or project.get("name") != workspace.project_name:
        raise RunLifecycleError("manifest project name does not match staging")
    if project.get("slug") != workspace.project_slug:
        raise RunLifecycleError("manifest project slug does not match staging")
    if not isinstance(input_set, Mapping) or input_set.get("sha256") != workspace.input_set.sha256:
        raise RunLifecycleError("manifest input-set hash does not match staging")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping) or not artifacts:
        raise RunLifecycleError("manifest must reference at least one artifact")
    logical_prefix = (
        f"output/runs/{workspace.project_slug}/{workspace.run_id}/"
    )
    for name, artifact in artifacts.items():
        artifact_path = str(artifact["path"]).replace("\\", "/").rstrip("/")
        if not artifact_path.startswith(logical_prefix):
            raise RunLifecycleError(
                f"artifact {name!r} is outside the final run directory"
            )
        relative = artifact_path[len(logical_prefix) :]
        if not relative or ".." in relative.split("/"):
            raise RunLifecycleError(f"artifact {name!r} has an unsafe path")
        if not (workspace.staging_directory / relative).exists():
            raise RunLifecycleError(
                f"staged artifact {name!r} does not exist: {relative}"
            )

    marker_path = workspace.staging_directory / STAGING_MARKER
    marker_contents = marker_path.read_text(encoding="utf-8")
    marker_path.unlink()
    try:
        os.replace(workspace.staging_directory, workspace.final_directory)
    except BaseException:
        marker_path.write_text(marker_contents, encoding="utf-8", newline="\n")
        raise
    _append_log(
        workspace.log_path,
        "run_published",
        final_directory=workspace.final_directory.name,
    )
    return workspace.final_directory
