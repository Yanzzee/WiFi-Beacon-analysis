from __future__ import annotations

import textwrap
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def script_cell_source(script_name: str, cell_number: int) -> tuple[str, Path]:
    script_path = REPO_ROOT / script_name
    lines = script_path.read_text(encoding="utf-8").splitlines(keepends=True)
    marker = f"# Cell {cell_number} -"

    try:
        title_index = next(
            index
            for index, line in enumerate(lines)
            if line.lstrip().startswith(marker)
        )
    except StopIteration as exc:
        raise AssertionError(f"{marker} was not found in {script_name}") from exc

    start_index = title_index + 2
    end_index = len(lines)
    for index in range(start_index, len(lines)):
        stripped = lines[index].lstrip()
        if stripped.startswith("# Cell ") or stripped.startswith(
            'if __name__ == "__main__":'
        ):
            end_index = index
            break

    return textwrap.dedent("".join(lines[start_index:end_index])), script_path
