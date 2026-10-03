from __future__ import annotations

from pathlib import Path

import pandas as pd

import beacon_analysis as cli


def _write_inputs(tmp_path: Path):
    captures = tmp_path / "captures"
    captures.mkdir()
    (captures / "capture.pcapng").write_bytes(b"\x0a\x0d\x0d\x0a")
    samples = tmp_path / "Sample"
    samples.mkdir()
    pd.DataFrame(
        {
            "Hostname": ["AP-01"],
            "Building-Room": ["BLDG-101"],
        }
    ).to_csv(samples / "Sample_APs.csv", index=False)
    pd.DataFrame(
        {
            "Location": ["BLDG-101"],
            "Capacity": [30],
        }
    ).to_csv(samples / "Sample_classrooms.csv", index=False)
    pd.DataFrame(
        {
            "Location": ["BLDG-101"],
            "Course": ["COURSE-A"],
            "Sec": ["1"],
            "LQ": ["A"],
            "H": ["B"],
            "S": ["C"],
            "Cr Hr": ["1"],
            "Sec Type": ["Regular"],
            "Class Period": ["9:00a - 9:50a"],
            "Days": ["M"],
            "Instructor": ["Instructor"],
            "Status": ["Active"],
            "Assign To": ["BLDG-101"],
            "Begin Date": ["2026-09-01"],
            "End Date": ["2026-12-01"],
            "sample": ["1"],
            "Size": [30],
            "Enrl": [20],
        }
    ).to_csv(samples / "Sample_schedule.csv", index=False)
    return captures, samples


def test_run_command_uses_raw_csvs_and_cli_overrides_config(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    captures, samples = _write_inputs(tmp_path)
    env_file = tmp_path / "config.env"
    env_file.write_text(
        "PROJECT_NAME=Config project\n"
        f"CAPTURES_FOLDER={captures}\n"
        f"OUTPUT_FOLDER={tmp_path / 'Output'}\n"
        f"SAMPLE_FOLDER={samples}\n"
        "START_DATE=2026-09-28\n"
        "END_DATE=2026-09-28\n"
        "TIMEZONE=America/Denver\n"
        "TSHARK_PATH=fake-tshark\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "_STARTUP_ENV", {})
    monkeypatch.setattr(
        cli,
        "_resolve_tshark",
        lambda setting: cli.ResolvedSetting(
            "C:/fake/tshark.exe",
            setting.source + "; detected TShark 4.6.6",
        ),
    )
    received = {}

    def fake_run(**kwargs):
        received.update(kwargs)
        return tmp_path / "Output" / "runs" / "published"

    monkeypatch.setattr(cli, "run_corrected_capture_project", fake_run)

    result = cli.main(
        [
            "run",
            "--env-file",
            str(env_file),
            "--project",
            "CLI project",
            "--yes",
        ]
    )

    assert result == 0
    assert received["project_name"] == "CLI project"
    assert received["ap_mapping_path"].endswith("Sample_APs.csv")
    assert received["classroom_path"].endswith("Sample_classrooms.csv")
    assert received["class_schedule_path"].endswith("Sample_schedule.csv")
    assert received["schedule_timezone"] == "America/Denver"
    assert Path(received["output_root"]) == (tmp_path / "Output" / "runs")
    assert received["capture_workers"] == 1
    assert len(received["capture_paths"]) == 1
    assert not list(samples.glob("*.parquet"))
    output = capsys.readouterr().out
    assert "Project: CLI project [command line]" in output
    assert "Configured APs: 1" in output
    assert "Expanded class sessions: 1" in output
    assert "Capture workers: 1 [default]" in output
    assert "Published corrected run:" in output


def test_non_interactive_mode_fails_before_prompt_when_required_config_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(cli, "_STARTUP_ENV", {})

    result = cli.main(
        [
            "run",
            "--env-file",
            str(tmp_path / "missing.env"),
            "--non-interactive",
        ]
    )

    assert result == 2
    assert "PROJECT_NAME is required" in capsys.readouterr().err


def test_non_interactive_explicit_csv_paths_do_not_require_sample_folder(
    tmp_path: Path,
    monkeypatch,
):
    captures, samples = _write_inputs(tmp_path)
    monkeypatch.setattr(cli, "_STARTUP_ENV", {})
    monkeypatch.setattr(
        cli,
        "_resolve_tshark",
        lambda setting: cli.ResolvedSetting("C:/fake/tshark.exe", setting.source),
    )
    received = {}
    monkeypatch.setattr(
        cli,
        "run_corrected_capture_project",
        lambda **kwargs: received.update(kwargs) or tmp_path / "published",
    )

    result = cli.main(
        [
            "run",
            "--non-interactive",
            "--project",
            "Explicit inputs",
            "--captures-dir",
            str(captures),
            "--output-root",
            str(tmp_path / "runs"),
            "--aps-csv",
            str(samples / "Sample_APs.csv"),
            "--classrooms-csv",
            str(samples / "Sample_classrooms.csv"),
            "--schedule-csv",
            str(samples / "Sample_schedule.csv"),
            "--start-date",
            "2026-09-28",
            "--end-date",
            "2026-09-28",
            "--timezone",
            "America/Denver",
            "--tshark",
            "fake-tshark",
            "--env-file",
            str(tmp_path / "missing.env"),
        ]
    )

    assert result == 0
    assert received["project_name"] == "Explicit inputs"


def test_capture_workers_flag_overrides_env_file(
    tmp_path: Path,
    monkeypatch,
):
    captures, samples = _write_inputs(tmp_path)
    env_file = tmp_path / "config.env"
    env_file.write_text(
        "PROJECT_NAME=Worker test\n"
        f"CAPTURES_FOLDER={captures}\n"
        f"OUTPUT_FOLDER={tmp_path / 'Output'}\n"
        f"SAMPLE_FOLDER={samples}\n"
        "START_DATE=2026-09-28\n"
        "END_DATE=2026-09-28\n"
        "TIMEZONE=America/Denver\n"
        "TSHARK_PATH=fake-tshark\n"
        "CAPTURE_WORKERS=1\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "_STARTUP_ENV", {})
    monkeypatch.setattr(
        cli,
        "_resolve_tshark",
        lambda setting: cli.ResolvedSetting("C:/fake/tshark.exe", setting.source),
    )
    received = {}
    monkeypatch.setattr(
        cli,
        "run_corrected_capture_project",
        lambda **kwargs: received.update(kwargs) or tmp_path / "published",
    )

    result = cli.main(
        [
            "run",
            "--env-file",
            str(env_file),
            "--capture-workers",
            "8",
            "--yes",
        ]
    )

    assert result == 0
    assert received["capture_workers"] == 8


def test_declining_confirmation_does_not_start_analysis(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    captures, samples = _write_inputs(tmp_path)
    env_file = tmp_path / "config.env"
    env_file.write_text(
        "PROJECT_NAME=Review project\n"
        f"CAPTURES_FOLDER={captures}\n"
        f"OUTPUT_FOLDER={tmp_path / 'Output'}\n"
        f"SAMPLE_FOLDER={samples}\n"
        "START_DATE=2026-09-28\n"
        "END_DATE=2026-09-28\n"
        "TIMEZONE=America/Denver\n"
        "TSHARK_PATH=fake-tshark\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "_STARTUP_ENV", {})
    monkeypatch.setattr(
        cli,
        "_resolve_tshark",
        lambda setting: cli.ResolvedSetting("C:/fake/tshark.exe", setting.source),
    )
    monkeypatch.setattr("builtins.input", lambda _prompt: "no")

    def unexpected_run(**_kwargs):
        raise AssertionError("analysis must not start after declined confirmation")

    monkeypatch.setattr(cli, "run_corrected_capture_project", unexpected_run)

    result = cli.main(["run", "--env-file", str(env_file)])

    assert result == 0
    assert "no analysis outputs were created" in capsys.readouterr().out
