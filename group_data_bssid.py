import dask.dataframe as dd
from tqdm import tqdm
from dotenv import load_dotenv
import os
import re

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")

if not PROCESSED_DATA_FOLDER:
    raise ValueError("PROCESSED_DATA_FOLDER is not set in config.env")
if not GROUPED_DATA_FOLDER:
    raise ValueError("GROUPED_DATA_FOLDER is not set in config.env")

INPUT_DIR = PROCESSED_DATA_FOLDER
OUTPUT_DIR = GROUPED_DATA_FOLDER
os.makedirs(OUTPUT_DIR, exist_ok=True)

df = dd.read_parquet(INPUT_DIR, engine="pyarrow")

# Ensure both columns exist
if "wlan.bssid" not in df.columns:
    raise ValueError("Input data must include wlan.bssid column")

# Identify unique BSSIDs (all beacons should have this)
unique_bssids = df["wlan.bssid"].unique().compute()

def sanitize(name: str) -> str:
    """Remove filesystem-unsafe characters."""
    s = str(name)
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s).strip().rstrip(".")
    return s[:100] or "UNKNOWN"

for bssid in tqdm(unique_bssids, desc="Processing AP Groups"):
    if not bssid or str(bssid).lower() in {"nan", "<na>", "none"}:
        print(f"[WARN] Skipping record with missing BSSID")
        continue

    # Filter dataframe by BSSID
    pandas_df = df[df["wlan.bssid"] == bssid].compute()

    if pandas_df.empty:
        continue

    # Determine output file name: prefer Aruba AP name, fallback to BSSID
    ap_name = pandas_df["wlan.vs.aruba.ap_name"].dropna().unique()
    if len(ap_name) > 0 and str(ap_name[0]).strip():
        out_name = sanitize(ap_name[0])
    else:
        out_name = sanitize(bssid)

    output_path = os.path.join(OUTPUT_DIR, f"{out_name}.parquet")
    pandas_df.to_parquet(output_path, engine="pyarrow")

print("Finished grouping all APs.")
