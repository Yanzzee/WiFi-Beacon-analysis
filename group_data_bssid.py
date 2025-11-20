import dask.dataframe as dd
from tqdm import tqdm
from dotenv import load_dotenv
import os, re
import pandas as pd

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")
if not PROCESSED_DATA_FOLDER or not GROUPED_DATA_FOLDER:
    raise ValueError("Set PROCESSED_DATA_FOLDER and GROUPED_DATA_FOLDER in config.env")

INPUT_DIR = PROCESSED_DATA_FOLDER
OUTPUT_DIR = GROUPED_DATA_FOLDER
os.makedirs(OUTPUT_DIR, exist_ok=True)

df = dd.read_parquet(INPUT_DIR, engine="pyarrow")

AP_COL = "wlan.vs.aruba.ap_name"
BSSID_COL = "wlan.bssid"

if BSSID_COL not in df.columns:
    raise ValueError("Input data must include wlan.bssid column")

def sanitize(name: str) -> str:
    s = str(name)
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s).strip().rstrip(".")
    return s[:100] or "UNKNOWN"

# --- 1) Write one file per AP NAME (combines all BSSIDs with that name) ---
# Consider non-empty strings as valid AP names
ap_names = (
    df[AP_COL]
    .dropna()
    .map_partitions(lambda s: s.astype(str).str.strip())
    .replace({"": pd.NA})
    .dropna()
    .unique()
    .compute()
)

# --- 2) Rows with NO AP NAME -> fallback to per-BSSID files ---
mask_no_ap = (df[AP_COL].isna()) | (df[AP_COL].astype(str).str.strip() == "")
unknown_bssids = (
    df[mask_no_ap][BSSID_COL]
    .dropna()
    .map_partitions(lambda s: s.astype(str).str.strip())
    .replace({"": pd.NA})
    .dropna()
    .unique()
    .compute()
)

total_groups = len(ap_names) + len(unknown_bssids)

with tqdm(total=total_groups, desc="Writing grouped Parquet") as pbar:
    # Per-AP files (combine all BSSIDs for that AP)
    for ap_name in ap_names:
        part = df[df[AP_COL] == ap_name].compute()
        if part.empty:
            pbar.update(1)
            continue
        out_name = sanitize(ap_name)
        output_path = os.path.join(OUTPUT_DIR, f"{out_name}.parquet")
        part.to_parquet(output_path, engine="pyarrow")
        pbar.update(1)

    # Fallback: per-BSSID files for rows with no AP name
    for bssid in unknown_bssids:
        if not bssid or str(bssid).lower() in {"nan", "<na>", "none"}:
            print("[WARN] Encountered row with missing BSSID and no AP name; skipping.")
            pbar.update(1)
            continue
        part = df[mask_no_ap & (df[BSSID_COL] == bssid)].compute()
        if part.empty:
            pbar.update(1)
            continue
        out_name = sanitize(bssid)
        output_path = os.path.join(OUTPUT_DIR, f"{out_name}.parquet")
        part.to_parquet(output_path, engine="pyarrow")
        pbar.update(1)

print("Finished grouping: one file per AP name, fallback to BSSID when AP name is missing.")