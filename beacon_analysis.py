"""One-command entry point for the corrected beacon analysis workflow."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Mapping, Sequence

from dotenv import dotenv_values

# Keep the caller's real environment separate from legacy modules that load
# the repository's default config.env as an import side effect.
_STARTUP_ENV = dict(os.environ)

import process_pcapng as capture_processing
from corrected_capture_run import (
    DEFAULT_BIN_WIDTH_US,
    run_corrected_capture_project,
)
from corrected_input_adapter import load_raw_sample_project_inputs


@dataclass(frozen=True)
class ResolvedSetting:
    value: str
    source: str


@dataclass(frozen=True)
class PreparedRun:
    project_name: ResolvedSetting
    captures_directory: ResolvedSetting
    output_root: ResolvedSetting
    ap_csv: ResolvedSetting
    classrooms_csv: ResolvedSetting
    schedule_csv: ResolvedSetting
    start_date: ResolvedSetting
    end_date: ResolvedSetting
    timezone: ResolvedSetting
    tshark: ResolvedSetting
    capture_workers: ResolvedSetting
    capture_paths: tuple[Path, ...]
    configured_ap_count: int
    class_session_count: int
    shared_room_session_merges: tuple[dict[str, object], ...]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run corrected beacon analysis from captures and sample CSVs."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser(
        "run",
        help="validate inputs, analyze captures, and publish one corrected run",
    )
    run.add_argument("--project", help="user-facing project name")
    run.add_argument("--captures-dir", help="directory containing pcap/pcapng files")
    run.add_argument("--output-root", help="root directory for project run folders")
    run.add_argument("--sample-folder", help="folder containing the three sample CSVs")
    run.add_argument("--aps-csv", help="override path to Sample_APs.csv")
    run.add_argument(
        "--classrooms-csv",
        help="override path to Sample_classrooms.csv",
    )
    run.add_argument("--schedule-csv", help="override path to Sample_schedule.csv")
    run.add_argument("--start-date", help="inclusive schedule start date (YYYY-MM-DD)")
    run.add_argument("--end-date", help="inclusive schedule end date (YYYY-MM-DD)")
    run.add_argument("--timezone", help="IANA timezone for schedule timestamps")
    run.add_argument("--tshark", help="path or command name for TShark")
    run.add_argument(
        "--capture-workers",
        type=int,
        choices=range(1, 17),
        help="concurrent bounded capture stages (1-16; default: 1)",
    )
    run.add_argument(
        "--env-file",
        default="config.env",
        help="dotenv configuration file (default: config.env)",
    )
    run.add_argument(
        "--yes",
        action="store_true",
        help="accept the displayed plan without a confirmation prompt",
    )
    run.add_argument(
        "--non-interactive",
        action="store_true",
        help="never prompt; fail when a required setting is missing",
    )
    return parser


def _nonempty(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _load_config(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    if not path.is_file():
        raise ValueError(f"env file is not a file: {path}")
    return {
        key: value
        for key, raw in dotenv_values(path).items()
        if (value := _nonempty(raw)) is not None
    }


def _resolve_value(
    *,
    flag_value: object,
    env_key: str,
    config: Mapping[str, str],
    env_file: Path,
    prompt: str,
    non_interactive: bool,
) -> ResolvedSetting:
    if value := _nonempty(flag_value):
        return ResolvedSetting(value, "command line")
    if value := _nonempty(_STARTUP_ENV.get(env_key)):
        return ResolvedSetting(value, f"environment variable {env_key}")
    if value := _nonempty(config.get(env_key)):
        return ResolvedSetting(value, f"{env_file} ({env_key})")
    if non_interactive:
        raise ValueError(f"{env_key} is required in non-interactive mode")
    value = _nonempty(input(f"{prompt}: "))
    if value is None:
        raise ValueError(f"{env_key} is required")
    return ResolvedSetting(value, "interactive prompt")


def _resolve_output_root(
    args: argparse.Namespace,
    config: Mapping[str, str],
    env_file: Path,
) -> ResolvedSetting:
    if value := _nonempty(args.output_root):
        return ResolvedSetting(value, "command line")
    if value := _nonempty(_STARTUP_ENV.get("CORRECTED_OUTPUT_ROOT")):
        return ResolvedSetting(
            value,
            "environment variable CORRECTED_OUTPUT_ROOT",
        )
    if value := _nonempty(config.get("CORRECTED_OUTPUT_ROOT")):
        return ResolvedSetting(
            value,
            f"{env_file} (CORRECTED_OUTPUT_ROOT)",
        )
    output_folder = _nonempty(_STARTUP_ENV.get("OUTPUT_FOLDER"))
    source = "environment variable OUTPUT_FOLDER"
    if output_folder is None:
        output_folder = _nonempty(config.get("OUTPUT_FOLDER"))
        source = f"{env_file} (OUTPUT_FOLDER)"
    if output_folder is not None:
        return ResolvedSetting(str(Path(output_folder) / "runs"), source + " + runs")
    if args.non_interactive:
        raise ValueError("CORRECTED_OUTPUT_ROOT or OUTPUT_FOLDER is required")
    value = _nonempty(input("Corrected output root: "))
    if value is None:
        raise ValueError("output root is required")
    return ResolvedSetting(value, "interactive prompt")


def _csv_setting(
    *,
    flag_value: object,
    env_key: str,
    filename: str,
    sample_folder: ResolvedSetting,
    config: Mapping[str, str],
    env_file: Path,
) -> ResolvedSetting:
    if value := _nonempty(flag_value):
        return ResolvedSetting(value, "command line")
    if value := _nonempty(_STARTUP_ENV.get(env_key)):
        return ResolvedSetting(value, f"environment variable {env_key}")
    if value := _nonempty(config.get(env_key)):
        return ResolvedSetting(value, f"{env_file} ({env_key})")
    return ResolvedSetting(
        str(Path(sample_folder.value) / filename),
        f"default under {sample_folder.source}",
    )


def _has_explicit_setting(
    flag_value: object,
    env_key: str,
    config: Mapping[str, str],
) -> bool:
    return any(
        _nonempty(value) is not None
        for value in (
            flag_value,
            _STARTUP_ENV.get(env_key),
            config.get(env_key),
        )
    )


def _absolute_path(value: str) -> Path:
    return Path(value).expanduser().resolve()


def _resolve_tshark(setting: ResolvedSetting) -> ResolvedSetting:
    capture_processing.TSHARK_EXECUTABLE = setting.value
    resolved = capture_processing.resolve_tshark_executable()
    version = capture_processing.read_tshark_version(resolved)
    return ResolvedSetting(
        str(Path(resolved).resolve()),
        f"{setting.source}; detected TShark {version}",
    )


def _resolve_capture_workers(
    args: argparse.Namespace,
    config: Mapping[str, str],
    env_file: Path,
) -> ResolvedSetting:
    candidates = (
        (args.capture_workers, "command line"),
        (
            _STARTUP_ENV.get("CAPTURE_WORKERS"),
            "environment variable CAPTURE_WORKERS",
        ),
        (config.get("CAPTURE_WORKERS"), f"{env_file} (CAPTURE_WORKERS)"),
    )
    for raw, source in candidates:
        value = _nonempty(raw)
        if value is None:
            continue
        try:
            workers = int(value)
        except ValueError as exc:
            raise ValueError("CAPTURE_WORKERS must be between 1 and 16") from exc
        if not 1 <= workers <= 16:
            raise ValueError("CAPTURE_WORKERS must be between 1 and 16")
        return ResolvedSetting(str(workers), source)
    return ResolvedSetting("1", "default")


def prepare_run(args: argparse.Namespace) -> PreparedRun:
    """Resolve, validate, and summarize one run before user confirmation."""

    env_file = _absolute_path(args.env_file)
    config = _load_config(env_file)
    resolve = lambda flag, key, prompt: _resolve_value(
        flag_value=flag,
        env_key=key,
        config=config,
        env_file=env_file,
        prompt=prompt,
        non_interactive=args.non_interactive,
    )
    project = resolve(args.project, "PROJECT_NAME", "Project name")
    captures = resolve(
        args.captures_dir,
        "CAPTURES_FOLDER",
        "Capture directory",
    )
    output_root = _resolve_output_root(args, config, env_file)
    csv_overrides = (
        (args.aps_csv, "AP_CSV"),
        (args.classrooms_csv, "CLASSROOMS_CSV"),
        (args.schedule_csv, "SCHEDULE_CSV"),
    )
    if all(
        _has_explicit_setting(flag, key, config)
        for flag, key in csv_overrides
    ):
        sample_folder = ResolvedSetting("", "not used")
    else:
        sample_folder = resolve(
            args.sample_folder,
            "SAMPLE_FOLDER",
            "Sample CSV folder",
        )
    ap_csv = _csv_setting(
        flag_value=args.aps_csv,
        env_key="AP_CSV",
        filename="Sample_APs.csv",
        sample_folder=sample_folder,
        config=config,
        env_file=env_file,
    )
    classrooms_csv = _csv_setting(
        flag_value=args.classrooms_csv,
        env_key="CLASSROOMS_CSV",
        filename="Sample_classrooms.csv",
        sample_folder=sample_folder,
        config=config,
        env_file=env_file,
    )
    schedule_csv = _csv_setting(
        flag_value=args.schedule_csv,
        env_key="SCHEDULE_CSV",
        filename="Sample_schedule.csv",
        sample_folder=sample_folder,
        config=config,
        env_file=env_file,
    )
    start_date = resolve(args.start_date, "START_DATE", "Schedule start date")
    end_date = resolve(args.end_date, "END_DATE", "Schedule end date")
    timezone = resolve(args.timezone, "TIMEZONE", "Schedule timezone")
    tshark_setting = ResolvedSetting("tshark", "automatic PATH discovery")
    if value := _nonempty(args.tshark):
        tshark_setting = ResolvedSetting(value, "command line")
    elif value := _nonempty(_STARTUP_ENV.get("TSHARK_PATH")):
        tshark_setting = ResolvedSetting(value, "environment variable TSHARK_PATH")
    elif value := _nonempty(config.get("TSHARK_PATH")):
        tshark_setting = ResolvedSetting(value, f"{env_file} (TSHARK_PATH)")
    tshark = _resolve_tshark(tshark_setting)
    capture_workers = _resolve_capture_workers(args, config, env_file)

    captures_path = _absolute_path(captures.value)
    if not captures_path.is_dir():
        raise FileNotFoundError(f"capture directory does not exist: {captures_path}")
    discovered = tuple(
        Path(path).resolve()
        for path in capture_processing.discover_pcapng_files(captures_path)
    )
    if not discovered:
        raise ValueError(f"no .pcap or .pcapng files found in {captures_path}")

    ap_path = _absolute_path(ap_csv.value)
    classrooms_path = _absolute_path(classrooms_csv.value)
    schedule_path = _absolute_path(schedule_csv.value)
    inputs = load_raw_sample_project_inputs(
        ap_mapping_path=ap_path,
        classroom_path=classrooms_path,
        class_schedule_path=schedule_path,
        timezone=timezone.value,
        start_date=start_date.value,
        end_date=end_date.value,
        default_bin_width_us=DEFAULT_BIN_WIDTH_US,
    )
    return PreparedRun(
        project_name=project,
        captures_directory=ResolvedSetting(str(captures_path), captures.source),
        output_root=ResolvedSetting(
            str(_absolute_path(output_root.value)), output_root.source
        ),
        ap_csv=ResolvedSetting(str(ap_path), ap_csv.source),
        classrooms_csv=ResolvedSetting(
            str(classrooms_path), classrooms_csv.source
        ),
        schedule_csv=ResolvedSetting(str(schedule_path), schedule_csv.source),
        start_date=start_date,
        end_date=end_date,
        timezone=timezone,
        tshark=tshark,
        capture_workers=capture_workers,
        capture_paths=discovered,
        configured_ap_count=len(inputs.configured_aps),
        class_session_count=len(inputs.class_sessions),
        shared_room_session_merges=tuple(
            merge.as_record()
            for merge in inputs.shared_room_session_merges
        ),
    )


def _display_plan(plan: PreparedRun) -> None:
    print("\nCorrected analysis plan")
    rows = (
        ("Project", plan.project_name),
        ("Captures", plan.captures_directory),
        ("Output root", plan.output_root),
        ("AP CSV", plan.ap_csv),
        ("Classrooms CSV", plan.classrooms_csv),
        ("Schedule CSV", plan.schedule_csv),
        ("Start date", plan.start_date),
        ("End date", plan.end_date),
        ("Timezone", plan.timezone),
        ("TShark", plan.tshark),
        ("Capture workers", plan.capture_workers),
    )
    for label, setting in rows:
        print(f"  {label}: {setting.value} [{setting.source}]")
    print(f"  Capture files: {len(plan.capture_paths)}")
    print(f"  Configured APs: {plan.configured_ap_count}")
    print(f"  Expanded class sessions: {plan.class_session_count}")
    if plan.shared_room_session_merges:
        source_rows = sum(
            int(record["source_row_count"])
            for record in plan.shared_room_session_merges
        )
        print(
            "  Shared-room sessions normalized: "
            f"{len(plan.shared_room_session_merges)} physical sessions "
            f"from {source_rows} source rows"
        )
        for record in plan.shared_room_session_merges:
            courses = " + ".join(str(value) for value in record["courses"])
            print(
                f"    {record['classroom']} {record['class_start']} to "
                f"{record['class_end']}: {courses}; enrolled total "
                f"{record['enrolled_total']}"
            )


def _execute(plan: PreparedRun) -> Path:
    capture_processing.TSHARK_EXECUTABLE = plan.tshark.value
    return run_corrected_capture_project(
        project_name=plan.project_name.value,
        capture_paths=plan.capture_paths,
        ap_mapping_path=plan.ap_csv.value,
        classroom_path=plan.classrooms_csv.value,
        class_schedule_path=plan.schedule_csv.value,
        schedule_timezone=plan.timezone.value,
        schedule_start_date=plan.start_date.value,
        schedule_end_date=plan.end_date.value,
        output_root=plan.output_root.value,
        capture_workers=int(plan.capture_workers.value),
        repository=Path(__file__).resolve().parent,
    )


def _run_command(args: argparse.Namespace) -> int:
    try:
        plan = prepare_run(args)
    except (FileNotFoundError, OSError, TypeError, ValueError, RuntimeError) as exc:
        print(f"Preflight failed: {exc}", file=sys.stderr)
        return 2

    _display_plan(plan)
    if not args.yes and not args.non_interactive:
        answer = input("Proceed with this run? [y/N]: ").strip().casefold()
        if answer not in {"y", "yes"}:
            print("Run cancelled; no analysis outputs were created.")
            return 0

    try:
        final = _execute(plan)
    except (FileNotFoundError, OSError, TypeError, ValueError, RuntimeError) as exc:
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1
    print(f"Published corrected run: {final}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "run":
            return _run_command(args)
        raise AssertionError(f"unhandled command: {args.command}")
    except KeyboardInterrupt:
        print("\nRun interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
