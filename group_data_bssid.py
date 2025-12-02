#regroup parquet files by AP name or BSSID if name doesn't exist
#processed captures must include BSSID column for this version to run
#this script may require analyze files to be updated before they will work

import dask.dataframe as dd
from tqdm import tqdm
from dotenv import load_dotenv
import os, re
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")
if not PROCESSED_DATA_FOLDER or not GROUPED_DATA_FOLDER:
    raise ValueError("Set PROCESSED_DATA_FOLDER and GROUPED_DATA_FOLDER in config.env")

INPUT_DIR = PROCESSED_DATA_FOLDER
OUTPUT_DIR = GROUPED_DATA_FOLDER
os.makedirs(OUTPUT_DIR, exist_ok=True)

df = dd.read_parquet(INPUT_DIR, engine="pyarrow")

if "wlan.bssid" not in df.columns:
    raise ValueError("Input data must include wlan.bssid")

def sanitize(name: str) -> str:
    s = str(name or "").strip()
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s).rstrip(".")
    return s[:100] or "UNKNOWN"

# One pass over partitions; keep open Parquet writers per output file
writers: dict[str, pq.ParquetWriter] = {}
schemas: dict[str, pa.Schema] = {}

try:
    n_parts = df.npartitions
    for i in tqdm(range(n_parts), desc="Processing partitions"):
        pdf = df.get_partition(i).compute()  # small, localized compute
        if pdf.empty:
            continue

        # Group within the partition by BSSID
        for bssid, g in pdf.groupby("wlan.bssid", dropna=False):
            if not isinstance(bssid, str) or not bssid or bssid.lower() in {"nan", "<na>", "none"}:
                print("[WARN] Missing BSSID in some rows; skipping those rows.")
                continue

            # Choose filename: AP name (first non-null) else BSSID
            ap_name = next((x for x in g["wlan.vs.aruba.ap_name"].dropna().unique() if str(x).strip()), None) \
                      if "wlan.vs.aruba.ap_name" in g.columns else None
            out_name = sanitize(ap_name if ap_name else bssid)
            out_path = os.path.join(OUTPUT_DIR, f"{out_name}.parquet")

            # Write/append with PyArrow (single file per BSSID/AP)
            table = pa.Table.from_pandas(g, preserve_index=True)
            if out_path not in writers:
                writers[out_path] = pq.ParquetWriter(out_path, table.schema)
                schemas[out_path] = table.schema
            else:
                # If schema drift occurs, align columns (rare but safer)
                if table.schema != schemas[out_path]:
                    table = table.combine_chunks()
                    table = table.cast(schemas[out_path], safe=False)
            writers[out_path].write_table(table)
finally:
    for w in writers.values():
        w.close()

print("Finished grouping all APs.")
