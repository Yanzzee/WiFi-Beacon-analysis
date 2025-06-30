import dask.dataframe as dd
from tqdm import tqdm
from dotenv import load_dotenv
import os

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")

if PROCESSED_DATA_FOLDER is None:
    raise ValueError("PROCESSED_DATA_FOLDER is not set in config.env")

GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")

if GROUPED_DATA_FOLDER is None:
    raise ValueError("GROUPED_DATA_FOLDER is not set in config.env")

INPUT_DIR = PROCESSED_DATA_FOLDER
OUTPUT_DIR = GROUPED_DATA_FOLDER

df = dd.read_parquet(INPUT_DIR, engine="pyarrow")

unique_ap_names = df["wlan.vs.aruba.ap_name"].unique().compute()    # this needs to be modified to account for multi-vendor

for ap_name in tqdm(unique_ap_names, desc="Processing AP Groups"):
    pandas_df = df[df["wlan.vs.aruba.ap_name"] == ap_name].compute()

    output_path = f"{OUTPUT_DIR}/{ap_name}.parquet"
    pandas_df.to_parquet(output_path, engine="pyarrow")
