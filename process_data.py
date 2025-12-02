import argparse
from concurrent.futures import ProcessPoolExecutor
import subprocess
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
CAPTURES_FOLDER = os.getenv("CAPTURES_FOLDER")
THREAD_COUNT = os.getenv("THREAD_COUNT")

ERM_UDP_PORT = int(os.getenv("ERM_UDP_PORT", "5555"))
ERM_TYPE = os.getenv("ERM_TYPE", "aruba_erm_type0")

CHUNK_SIZE = 10000

# Aruba ERM captures
TSHARK_FIELDS_ERM = [
    "aruba_erm.time",
    "wlan.ta",
    "wlan.bssid",                # keep BSSID
    "wlan.vs.aruba.ap_name",
    "wlan.ssid",
    "wlan.qbss.scount",
    "wlan.qbss.cu",
    "wlan.qbss.adc",
    # frequency sources (band-agnostic)
    "radiotap.channel.freq",
    "wlan_radio.frequency",
]

# Native 802.11 captures
TSHARK_FIELDS_NATIVE = [
    "frame.time_epoch",
    "wlan.ta",
    "wlan.bssid",
    "wlan.ssid",
    "wlan.qbss.scount",
    "wlan.qbss.cu",
    "wlan.qbss.adc",
    "radiotap.channel.freq",
    "wlan_radio.frequency",
]

def process_pcapng(file_path, output_dir, erm=True):
    output_file = os.path.join(output_dir, f"{os.path.basename(file_path)}.parquet")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Processing {file_path}...")

    tshark_command = ["tshark", "-r", file_path, "-M", "100000", "-T", "fields"]

    if erm:
        tshark_command += [
            "-d", f"udp.port=={ERM_UDP_PORT},aruba_erm",
            "-d", f"aruba_erm.type,{ERM_TYPE}",
        ]
        fields = TSHARK_FIELDS_ERM
    else:
        fields = TSHARK_FIELDS_NATIVE

    tshark_command += [
        "-E", "header=y",
        "-E", "separator=,",
        "-E", "quote=d",
    ]
    for f in fields:
        tshark_command.extend(["-e", f])

    # Read tshark CSV directly into pandas
    with subprocess.Popen(tshark_command, stdout=subprocess.PIPE, text=True) as proc:
        index_col = "aruba_erm.time" if erm else "frame.time_epoch"
        df = pd.read_csv(
            proc.stdout,
            sep=",",
            quotechar='"',
            index_col=index_col,
            parse_dates=True,
            date_format="%b %e, %Y %H:%M:%S.%f %Z" if erm else None,
        )

    # Build a single frequency_mhz column (numeric) from available sources
    freq = pd.to_numeric(df.get("radiotap.channel.freq"), errors="coerce")
    if "wlan_radio.frequency" in df.columns:
        freq = freq.fillna(pd.to_numeric(df["wlan_radio.frequency"], errors="coerce"))
    df["frequency_mhz"] = freq  # leave NaN if not available

    # --- force SSID to pandas string dtype ---
    if "wlan.ssid" in df.columns:
        df["wlan.ssid"] = df["wlan.ssid"].astype("string")
    if "wlan.vs.aruba.ap_name" in df.columns:
        df["wlan.vs.aruba.ap_name"] = df["wlan.vs.aruba.ap_name"].astype("string")

    # Convert QBSS metrics to numeric (int)
    for col in ["wlan.qbss.scount", "wlan.qbss.cu", "wlan.qbss.adc"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Convert frequency fields to int
    for col in ["radiotap.channel.freq", "wlan_radio.frequency", "frequency_mhz"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Convert QBSS metrics to numeric
    for col in ["wlan.qbss.scount", "wlan.qbss.cu", "wlan.qbss.adc"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_parquet(output_file, engine="pyarrow")
    print(f"Finished processing {file_path}")
    return output_file

def process_all_pcapngs(pcapng_dir, output_dir, num_processes, erm=True):
    os.makedirs(output_dir, exist_ok=True)
    pcapng_files = [
        os.path.join(pcapng_dir, f)
        for f in os.listdir(pcapng_dir)
        if f.endswith(".pcapng")
    ]
    print(f"Found {len(pcapng_files)} pcapng files. Starting processing...")

    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(process_pcapng, file, output_dir, erm) for file in pcapng_files]
        for fut in futures:
            fut.result()
    print("All files processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process pcapng files and save as Parquet.")
    parser.add_argument("-i", "--pcapng_dir", type=str, default=CAPTURES_FOLDER)
    parser.add_argument("-o", "--output_dir", type=str, default=PROCESSED_DATA_FOLDER)
    parser.add_argument("-n", "--num_processes", type=int, default=THREAD_COUNT)
    parser.add_argument("--no-erm", action="store_true", help="Process non-Aruba ERM captures.")
    args = parser.parse_args()

    process_all_pcapngs(args.pcapng_dir, args.output_dir, args.num_processes, erm=not args.no_erm)
