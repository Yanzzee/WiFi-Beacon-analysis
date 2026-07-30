# Regroup parquet files by AP name or BSSID if name doesn't exist.

import os

from dotenv import load_dotenv

from grouping_bssid import group_data_by_bssid


load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")
if not PROCESSED_DATA_FOLDER or not GROUPED_DATA_FOLDER:
    raise ValueError(
        "Set PROCESSED_DATA_FOLDER and GROUPED_DATA_FOLDER in config.env"
    )

INPUT_DIR = PROCESSED_DATA_FOLDER
OUTPUT_DIR = GROUPED_DATA_FOLDER
os.makedirs(OUTPUT_DIR, exist_ok=True)

group_data_by_bssid(INPUT_DIR, OUTPUT_DIR)
