"""Reusable grouped-parquet cleanup workflow."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import os

import pandas as pd


def fix_one(full_path: str, plot_timezone: str) -> str:
    try:
        df = pd.read_parquet(full_path, engine="pyarrow")

        # 1) Ensure time is a column, not index
        if df.index.name == "aruba_erm.time":
            df = df.reset_index()

        if "aruba_erm.time" not in df.columns:
            return (
                "[skip] No 'aruba_erm.time' column in "
                f"{os.path.basename(full_path)}"
            )

        ts = df["aruba_erm.time"]

        # 2) Normalize to datetime
        if not pd.api.types.is_datetime64_any_dtype(ts):
            ts = pd.to_datetime(ts, errors="coerce", utc=True)

        # 3) Localize or convert
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(
                plot_timezone,
                ambiguous="NaT",
                nonexistent="NaT",
            )
        else:
            ts = ts.dt.tz_convert(plot_timezone)

        df["aruba_erm.time"] = ts

        # 4) Save back
        df.to_parquet(full_path, index=False, engine="pyarrow")
        return f"[ok] {os.path.basename(full_path)}"
    except Exception as error:
        return f"[err] {os.path.basename(full_path)}: {error}"


def cleanup_grouped_data(
    folder_path: str,
    plot_timezone: str,
    thread_count: int,
) -> None:
    # Collect files first
    files = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if filename.endswith(".parquet")
    ]

    print(
        f"Processing {len(files)} files with "
        f"{thread_count} threads…"
    )

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [
            executor.submit(fix_one, path, plot_timezone)
            for path in files
        ]
        for future in as_completed(futures):
            print(future.result())
