import dask.dataframe as dd
from tqdm import tqdm
from dotenv import load_dotenv
import os
import re
import pandas as pd  # for pd.isna

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
if PROCESSED_DATA_FOLDER is None:
    raise ValueError("PROCESSED_DATA_FOLDER is not set in config.env")

GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")
if GROUPED_DATA_FOLDER is None:
    raise ValueError("GROUPED_DATA_FOLDER is not set in config.env")

INPUT_DIR = PROCESSED_DATA_FOLDER
OUTPUT_DIR = GROUPED_DATA_FOLDER
os.makedirs(OUTPUT_DIR, exist_ok=True)


def sanitize_filename(name: str) -> str:
    """
    Convert an arbitrary AP name into a safe, cross-OS filename.

    - Removes/ replaces characters illegal on Windows: <>:\"/\\|?* and control chars 0x00–0x1F
    - Replaces path separators for safety (/ and \\)
    - Strips trailing space/dot which Windows disallows
    - Limits length to something reasonable
    """
    s = str(name or "").strip()

    # Replace any path separators, regardless of OS
    s = s.replace("/", "_").replace("\\", "_")

    # Replace Windows-illegal chars and control chars
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s)

    # Windows doesn't like names ending with a space or dot
    s = s.rstrip(" .")

    # Limit length so we don't hit crazy long filenames
    s = s[:100]

    # Fallback if empty
    return s or "UNKNOWN"


df = dd.read_parquet(INPUT_DIR, engine="pyarrow")

# NOTE: still Aruba-specific; later you can generalize for multi-vendor
if "wlan.vs.aruba.ap_name" not in df.columns:
    raise ValueError(
        "Input data does not contain 'wlan.vs.aruba.ap_name'. "
        "Did you run the preprocessing pipeline with Aruba fields enabled?"
    )

unique_ap_names = df["wlan.vs.aruba.ap_name"].unique().compute()

for ap_name in tqdm(unique_ap_names, desc="Processing AP Groups"):
    # Handle NaN / None / empty AP names
    if pd.isna(ap_name) or str(ap_name).strip() == "":
        safe_name = "UNKNOWN"
    else:
        safe_name = sanitize_filename(ap_name)

    # Filter rows for this AP and compute to pandas
    pandas_df = df[df["wlan.vs.aruba.ap_name"] == ap_name].compute()

    if pandas_df.empty:
        continue

    output_path = os.path.join(OUTPUT_DIR, f"{safe_name}.parquet")

    # Optional: debug if something still goes wrong
    # print("Writing", repr(ap_name), "->", repr(output_path))

    pandas_df.to_parquet(output_path, engine="pyarrow")
