"""Reusable BSSID-based grouping workflow."""

import os
import re

import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


def sanitize(name: str) -> str:
    s = str(name or "").strip()
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s).rstrip(".")
    return s[:100] or "UNKNOWN"


def group_data_by_bssid(input_dir: str, output_dir: str) -> None:
    df = dd.read_parquet(input_dir, engine="pyarrow")

    if "wlan.bssid" not in df.columns:
        raise ValueError("Input data must include wlan.bssid")

    # One pass over partitions; keep open Parquet writers per output file
    writers: dict[str, pq.ParquetWriter] = {}
    schemas: dict[str, pa.Schema] = {}

    try:
        n_parts = df.npartitions
        for i in tqdm(range(n_parts), desc="Processing partitions"):
            pdf = df.get_partition(i).compute()
            if pdf.empty:
                continue

            # Group within the partition by BSSID
            for bssid, group in pdf.groupby("wlan.bssid", dropna=False):
                if (
                    not isinstance(bssid, str)
                    or not bssid
                    or bssid.lower() in {"nan", "<na>", "none"}
                ):
                    print(
                        "[WARN] Missing BSSID in some rows; "
                        "skipping those rows."
                    )
                    continue

                # Choose filename: AP name (first non-null) else BSSID
                ap_name = (
                    next(
                        (
                            value
                            for value in group[
                                "wlan.vs.aruba.ap_name"
                            ]
                            .dropna()
                            .unique()
                            if str(value).strip()
                        ),
                        None,
                    )
                    if "wlan.vs.aruba.ap_name" in group.columns
                    else None
                )
                out_name = sanitize(ap_name if ap_name else bssid)
                out_path = os.path.join(
                    output_dir,
                    f"{out_name}.parquet",
                )

                # Write/append with PyArrow (single file per BSSID/AP)
                table = pa.Table.from_pandas(group, preserve_index=True)
                if out_path not in writers:
                    writers[out_path] = pq.ParquetWriter(
                        out_path,
                        table.schema,
                    )
                    schemas[out_path] = table.schema
                else:
                    # If schema drift occurs, align columns
                    if table.schema != schemas[out_path]:
                        table = table.combine_chunks()
                        table = table.cast(
                            schemas[out_path],
                            safe=False,
                        )
                writers[out_path].write_table(table)
    finally:
        for writer in writers.values():
            writer.close()

    print("Finished grouping all APs.")
