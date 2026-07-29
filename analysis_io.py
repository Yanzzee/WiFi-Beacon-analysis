"""Parquet persistence helpers shared by the analysis workflow."""

import glob
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def append_to_parquet(
    AP_name,
    all_class_beacons_df,
    sampled_folder,
    log_and_print,
):
    # Ensure all_class_beacons_df is not empty before appending
    if not all_class_beacons_df.empty:
        ap_beacons_file = os.path.join(
            sampled_folder,
            f"{AP_name}_class_beacons.parquet",
        )

        # Define schema based on the DataFrame
        schema = pa.Schema.from_pandas(all_class_beacons_df)

        # Check if the file exists
        if os.path.exists(ap_beacons_file):
            # If file exists, open it and append using ParquetWriter
            existing_table = pq.read_table(ap_beacons_file)
            with pq.ParquetWriter(
                ap_beacons_file,
                schema,
                compression="snappy",
            ) as writer:
                # Append the existing data first
                writer.write_table(existing_table)
                # Append the new data
                table = pa.Table.from_pandas(
                    all_class_beacons_df,
                    schema=schema,
                )
                writer.write_table(table)
            log_and_print(f"Adding to {ap_beacons_file}")
        else:
            # If the file doesn't exist, create it and write data
            all_class_beacons_df.to_parquet(
                ap_beacons_file,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
            log_and_print(f"Creating {ap_beacons_file}")

    # else:
    #     log_and_print("DataFrame is empty, nothing to append.")


def combine_all_class_beacons(sampled_folder, data_folder):
    all_class_beacons_df = pd.DataFrame()

    # Directory containing AP .parquet files
    directory = sampled_folder

    # Get a list of all .parquet files
    parquet_files = glob.glob(os.path.join(directory, "*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            "No sampled class beacon parquet files found in "
            f"SAMPLED_FOLDER ({directory}). Check grouped AP/sample matches "
            "before combining all class beacons."
        )

    all_class_beacons_df = all_class_beacons_df.iloc[0:0]
    # Read and concatenate all Parquet files (faster than .pkl)
    all_class_beacons_df = pd.concat(
        [pd.read_parquet(file) for file in parquet_files],
        ignore_index=True,
    )

    all_class_beacons_df.to_parquet(
        f"{data_folder}/all_class_beacons.parquet"
    )
