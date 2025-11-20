# This code is for cleaning up the grouped_data parquet files that do not have a separate index, and have the aruba_erm.time as the index
# also makes sure the time is in the right format
# group_data_cleanup.py (multithreaded)
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv("config.env")

PLOT_TIMEZONE = os.getenv("PLOT_TIMEZONE")
GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")
THREAD_COUNT = int(os.getenv("THREAD_COUNT", "4"))

if not PLOT_TIMEZONE:
    raise ValueError("PLOT_TIMEZONE is not set in config.env")
if not GROUPED_DATA_FOLDER:
    raise ValueError("GROUPED_DATA_FOLDER is not set in config.env")

folder_path = GROUPED_DATA_FOLDER

def fix_one(full_path: str) -> str:
    try:
        df = pd.read_parquet(full_path, engine="pyarrow")

        # 1) Ensure time is a column, not index
        if df.index.name == "aruba_erm.time":
            df = df.reset_index()

        if "aruba_erm.time" not in df.columns:
            return f"[skip] No 'aruba_erm.time' column in {os.path.basename(full_path)}"

        ts = df["aruba_erm.time"]

        # 2) Normalize to datetime
        if not pd.api.types.is_datetime64_any_dtype(ts):
            ts = pd.to_datetime(ts, errors="coerce", utc=True)

        # 3) Localize or convert
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(PLOT_TIMEZONE, ambiguous="NaT", nonexistent="NaT")
        else:
            ts = ts.dt.tz_convert(PLOT_TIMEZONE)

        df["aruba_erm.time"] = ts

        # 4) Save back
        df.to_parquet(full_path, index=False, engine="pyarrow")
        return f"[ok] {os.path.basename(full_path)}"
    except Exception as e:
        return f"[err] {os.path.basename(full_path)}: {e}"

# collect files first
files = [
    os.path.join(folder_path, fn)
    for fn in os.listdir(folder_path)
    if fn.endswith(".parquet")
]

print(f"Processing {len(files)} files with {THREAD_COUNT} threads…")

with ThreadPoolExecutor(max_workers=THREAD_COUNT) as ex:
    futures = [ex.submit(fix_one, p) for p in files]
    for fut in as_completed(futures):
        print(fut.result())
