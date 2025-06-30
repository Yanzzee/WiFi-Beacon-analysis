# This code is for cleaning up the grouped_data parquet files that do not have a separate index, and have the aruba_erm.time as the index

import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv("config.env")

PLOT_TIMEZONE = os.getenv("PLOT_TIMEZONE")

if PLOT_TIMEZONE is None:
    raise ValueError("PLOT_TIMEZONE is not set in config.env")

GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")

if GROUPED_DATA_FOLDER is None:
    raise ValueError("GROUPED_DATA_FOLDER is not set in config.env")

# Set the folder containing your parquet files
folder_path = GROUPED_DATA_FOLDER

# Process each .parquet file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".parquet"):
        full_path = os.path.join(folder_path, filename)
        print(f"Processing {filename}...")

        try:
            df = pd.read_parquet(full_path)

            # Reset index if it's aruba_erm.time
            if df.index.name == "aruba_erm.time":
                df.reset_index(inplace=True)

            # Ensure aruba_erm.time is a datetime column
            if "aruba_erm.time" in df.columns:
                df["aruba_erm.time"] = (
                    df["aruba_erm.time"]
                    .astype(str)
                    .str.replace("MDT", "", regex=False)
                    .str.strip()
                )
                df["aruba_erm.time"] = pd.to_datetime(
                    df["aruba_erm.time"], errors="coerce"
                )
                df["aruba_erm.time"] = df["aruba_erm.time"].dt.tz_localize(
                    PLOT_TIMEZONE, ambiguous="NaT", nonexistent="NaT"
                )



            # Save it back
            df.to_parquet(full_path, index=False)
            print(f"✔️ Fixed and saved: {filename}")

        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")