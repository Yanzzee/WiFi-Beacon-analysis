import os

from dotenv import load_dotenv

from grouping_cleanup import cleanup_grouped_data


load_dotenv("config.env")

PLOT_TIMEZONE = os.getenv("PLOT_TIMEZONE")
GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")
THREAD_COUNT = int(os.getenv("THREAD_COUNT", "4"))

if not PLOT_TIMEZONE:
    raise ValueError("PLOT_TIMEZONE is not set in config.env")
if not GROUPED_DATA_FOLDER:
    raise ValueError("GROUPED_DATA_FOLDER is not set in config.env")

folder_path = GROUPED_DATA_FOLDER

cleanup_grouped_data(folder_path, PLOT_TIMEZONE, THREAD_COUNT)
