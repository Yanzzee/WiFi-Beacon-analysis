import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_samples_script_handles_short_schedule(tmp_path):
    sample_dir = tmp_path

    (sample_dir / "Sample_APs.csv").write_text(
        "Building-Room\nRoom1\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_classrooms.csv").write_text(
        "Location,Capacity\nRoom1,50\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_schedule.csv").write_text(
        "Location,Course,Sec,LQ,H,S,Cr Hr,Sec Type,Class Period,Days,Instructor,Status,Assign To,Begin Date,End Date,sample,Size,Enrl\n"
        "Room1,Test Course,1,A,B,C,1,Regular,not-a-time,M,Prof,Active,Room1,2025-01-01,2025-01-31,1,10,10\n",
        encoding="utf-8",
    )
    (sample_dir / "config.env").write_text(
        "TIMEZONE=Etc/GMT+7\n"
        "SAMPLE_FOLDER=.\n"
        "START_DATE=2025-01-01\n"
        "END_DATE=2025-01-01\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "Samples.py")],
        cwd=sample_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (sample_dir / "sample_classes.parquet").exists()
