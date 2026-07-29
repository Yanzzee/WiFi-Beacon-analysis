"""Reusable Aruba AP-name grouping workflow."""

import os
import re

import dask.dataframe as dd
import pandas as pd
from tqdm import tqdm


def sanitize_filename(name: str) -> str:
    """
    Convert an arbitrary AP name into a safe, cross-OS filename.

    - Removes/replaces characters illegal on Windows
    - Replaces path separators for safety
    - Strips trailing space/dot which Windows disallows
    - Limits length to something reasonable
    """
    s = str(name or "").strip()

    # Replace any path separators, regardless of OS
    s = s.replace("/", "_").replace("\\", "_")

    # Replace Windows-illegal chars and control chars
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s)

    # Windows doesn't like names ending with a space or dot
    s = s.rstrip(" .")

    # Limit length so we don't hit crazy long filenames
    s = s[:100]

    # Fallback if empty
    return s or "UNKNOWN"


def group_data_by_aruba_name(input_dir: str, output_dir: str) -> None:
    df = dd.read_parquet(input_dir, engine="pyarrow")

    # NOTE: still Aruba-specific; later you can generalize for multi-vendor
    if "wlan.vs.aruba.ap_name" not in df.columns:
        raise ValueError(
            "Input data does not contain 'wlan.vs.aruba.ap_name'. "
            "Did you run the preprocessing pipeline with Aruba fields "
            "enabled?"
        )

    unique_ap_names = df["wlan.vs.aruba.ap_name"].unique().compute()

    for ap_name in tqdm(unique_ap_names, desc="Processing AP Groups"):
        # Handle NaN / None / empty AP names
        if pd.isna(ap_name) or str(ap_name).strip() == "":
            safe_name = "UNKNOWN"
        else:
            safe_name = sanitize_filename(ap_name)

        # Filter rows for this AP and compute to pandas
        pandas_df = df[df["wlan.vs.aruba.ap_name"] == ap_name].compute()

        if pandas_df.empty:
            continue

        output_path = os.path.join(
            output_dir,
            f"{safe_name}.parquet",
        )
        pandas_df.to_parquet(output_path, engine="pyarrow")
