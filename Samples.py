###
# Input sample information for classrooms, APs and class periods
# See example files for input format
# clean up the data and export .parquet files that can be easily imported for analysis later
# creates sample_classes_df and saves it for analysis - includes every class session
###

import os

from dotenv import load_dotenv

from analysis_config import ensure_directory
from sample_processing import process_samples

load_dotenv(dotenv_path="config.env")

TIMEZONE = os.getenv("TIMEZONE")
SAMPLE_FOLDER = os.getenv("SAMPLE_FOLDER")

if TIMEZONE is None:
    raise ValueError("TIMEZONE is not set in config.env")
if SAMPLE_FOLDER is None:
    raise ValueError("SAMPLE_FOLDER is not set in config.env")

ensure_directory(SAMPLE_FOLDER)

load_dotenv(dotenv_path="config.env")

start_date_str = os.getenv("START_DATE")
end_date_str = os.getenv("END_DATE")

process_samples(
    SAMPLE_FOLDER,
    TIMEZONE,
    start_date_str,
    end_date_str,
)
