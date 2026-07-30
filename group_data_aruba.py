import os

from dotenv import load_dotenv

from grouping_aruba import group_data_by_aruba_name


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

group_data_by_aruba_name(INPUT_DIR, OUTPUT_DIR)
