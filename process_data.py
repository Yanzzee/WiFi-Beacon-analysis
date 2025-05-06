import argparse
from concurrent.futures import ProcessPoolExecutor
import subprocess
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
CAPTURES_FOLDER = os.getenv("CAPTURES_FOLDER")

CHUNK_SIZE = 10000
TSHARK_FIELDS = [
    "aruba_erm.time",
    "wlan.ta",
    "wlan.vs.aruba.ap_name",
    "wlan.ssid",
    "wlan.qbss.scount",
    "wlan.qbss.cu",
    "wlan.qbss.adc",
#    "wlan.ds.current_channel", # add for channel number in output # add this in later
]


# Function to parse and save pcapng file
def process_pcapng(file_path, output_dir):
    output_file = os.path.join(output_dir, f"{os.path.basename(file_path)}.parquet")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Processing {file_path}...")

    tshark_command = [
        "tshark",
        "-r",
        file_path,
        "-M",
        "100000",
        "-T",
        "fields",
        "-E",
        "header=y",
        "-E",
        "separator=,",
        "-E",
        "quote=d",
    ]

    for field in TSHARK_FIELDS:
        tshark_command.extend(["-e", field])

    # Start the subprocess and stream the output line-by-line
    with subprocess.Popen(tshark_command, stdout=subprocess.PIPE, text=True) as proc:
        df = pd.read_csv(
            proc.stdout,
            sep=",",
            quotechar='"',
            index_col="aruba_erm.time",
            parse_dates=True,
            date_format="%b %e, %Y %H:%M:%S.%f %Z",
        )

    print("Finished reading pcapng file. Saving to Parquet...")
    
    # Try to force convert specific columns to numeric, coercing errors to NaN
    numeric_columns = ["wlan.qbss.scount", "wlan.qbss.cu", "wlan.qbss.adc"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_parquet(output_file, engine="pyarrow")
    print(f"Finished processing {file_path}")
    return output_file


def process_all_pcapngs(pcapng_dir, output_dir, num_processes):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pcapng_files = [
        os.path.join(pcapng_dir, f)
        for f in os.listdir(pcapng_dir)
        if f.endswith(".pcapng")
    ]
    print(f"Found {len(pcapng_files)} pcapng files. Starting processing...")

    # Process files in parallel
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [
            executor.submit(process_pcapng, file, output_dir) for file in pcapng_files
        ]
        for future in futures:
            future.result()

    print("All files processed.")


# Main Execution with argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process pcapng files and save as Parquet."
    )
    parser.add_argument(
        "-i",
        "--pcapng_dir",
        type=str,
        default=CAPTURES_FOLDER,
        help="Directory containing pcapng files.",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default=PROCESSED_DATA_FOLDER,
        help="Directory to save processed Parquet files.",
    )
    parser.add_argument(
        "-n",
        "--num_processes",
        type=int,
        default=1,
        help="Number of processes to use for parallel processing.",
    )

    args = parser.parse_args()

    print("Starting pcapng processing...")
    process_all_pcapngs(args.pcapng_dir, args.output_dir, args.num_processes)
