from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[0]
sys.path.insert(0, str(SCRIPT_DIR))

from pipeline_harness import (  # noqa: E402
    build_fingerprints,
    missing_expected_outputs,
    missing_expected_plots,
    missing_required_columns,
    prepare_analysis_workspace,
    run_analysis_pipeline,
    write_fingerprints,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build golden fingerprints for the analysis-forward pytest harness."
    )
    parser.add_argument(
        "--fixture-dir",
        type=Path,
        default=Path(
            os.environ.get(
                "BEACON_ANALYSIS_FIXTURE_DIR",
                REPO_ROOT / "tests" / "fixtures" / "dataset",
            )
        ),
        help=(
            "Fixture dataset root. Must contain Sample/*.parquet and "
            "Data/grouped_data/*.parquet."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            os.environ.get(
                "BEACON_ANALYSIS_GOLDEN_FILE",
                REPO_ROOT / "tests" / "golden" / "fingerprints.json",
            )
        ),
        help="Golden fingerprint JSON to write.",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=None,
        help="Optional workspace to use instead of a temporary directory.",
    )
    parser.add_argument(
        "--keep-workspace",
        action="store_true",
        help="Keep the temporary workspace after the run.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Per-notebook timeout in seconds.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    temp_dir = None

    if args.workspace is None:
        temp_dir = tempfile.mkdtemp(prefix="beacon_analysis_golden_")
        workspace = Path(temp_dir) / "workspace"
    else:
        workspace = args.workspace
        if workspace.exists():
            shutil.rmtree(workspace)

    try:
        prepare_analysis_workspace(args.fixture_dir, workspace, REPO_ROOT)
        run_analysis_pipeline(workspace, timeout=args.timeout)

        missing_outputs = missing_expected_outputs(workspace)
        missing_plots = missing_expected_plots(workspace)
        missing_columns = missing_required_columns(workspace)

        if missing_outputs or missing_plots or missing_columns:
            print("Golden build produced incomplete outputs.", file=sys.stderr)
            if missing_outputs:
                print(
                    "Missing output files: " + ", ".join(missing_outputs),
                    file=sys.stderr,
                )
            if missing_plots:
                print(
                    "Missing plot files: " + ", ".join(missing_plots),
                    file=sys.stderr,
                )
            if missing_columns:
                print(f"Missing columns: {missing_columns}", file=sys.stderr)
            return 1

        fingerprints = build_fingerprints(workspace)
        write_fingerprints(args.output, fingerprints)
        print(f"Wrote golden fingerprints to {args.output}")
        print(f"Workspace: {workspace}")
        return 0
    finally:
        if temp_dir and not args.keep_workspace:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
