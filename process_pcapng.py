# take capture files and create parquet files
# need to add logic for whether capture is Aruba ERM or wireless, if ERM, which type based on env file

import argparse
from concurrent.futures import ProcessPoolExecutor
import subprocess
import os
from dotenv import load_dotenv
import pandas as pd
import re
import shutil
import io
import warnings

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
CAPTURES_FOLDER = os.getenv("CAPTURES_FOLDER")
THREAD_COUNT = os.getenv("THREAD_COUNT")
TSHARK_EXECUTABLE = os.getenv("TSHARK_PATH", "tshark")

ERM_UDP_PORT = int(os.getenv("ERM_UDP_PORT", "5555"))
ERM_TYPE = os.getenv("ERM_TYPE", "aruba_erm_type0")

CHUNK_SIZE = 10000
PCAPNG_MAGIC = b"\x0a\x0d\x0d\x0a"
PCAP_MAGIC_BYTES = {
    b"\xa1\xb2\xc3\xd4",
    b"\xd4\xc3\xb2\xa1",
    b"\xa1\xb2\x3c\x4d",
    b"\x4d\x3c\xb2\xa1",
}
CAPTURE_FILE_EXTENSIONS = {".pcapng", ".pcap"}


def resolve_tshark_executable() -> str:
    """Return the tshark executable path or raise if not available."""
    resolved = shutil.which(TSHARK_EXECUTABLE)
    if resolved:
        return resolved

    if os.path.isabs(TSHARK_EXECUTABLE) and os.path.exists(TSHARK_EXECUTABLE):
        return TSHARK_EXECUTABLE

    raise FileNotFoundError(
        "The tshark executable was not found. "
        f"Tried '{TSHARK_EXECUTABLE}'. Ensure tshark is installed and on PATH, "
        "or set the TSHARK_PATH environment variable to the tshark executable."
    )


def validate_pcapng_capture(file_path, warn_on_legacy_extension=True) -> None:
    """Validate that a capture file is pcapng, even if it has a legacy .pcap name."""
    file_path = os.fspath(file_path)
    extension = os.path.splitext(file_path)[1].lower()

    with open(file_path, "rb") as capture_file:
        magic = capture_file.read(4)

    if magic == PCAPNG_MAGIC:
        if extension == ".pcap" and warn_on_legacy_extension:
            warnings.warn(
                f"{file_path} has a .pcap extension but pcapng magic bytes. "
                "Processing will continue, but rename the file to .pcapng when possible.",
                UserWarning,
                stacklevel=2,
            )
        return

    if magic in PCAP_MAGIC_BYTES:
        raise ValueError(
            f"{file_path} is a pcap file based on its magic bytes. "
            "This pipeline currently requires pcapng captures. Convert or recapture as .pcapng."
        )

    magic_hex = magic.hex(" ") if magic else "empty file"
    raise ValueError(
        f"{file_path} is not a pcapng capture based on its magic bytes ({magic_hex}). "
        "This pipeline currently requires pcapng captures."
    )

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

# --- helper to decode hex-encoded SSIDs ---
HEX_RE = re.compile(r"^[0-9a-fA-F]+$")

def _looks_like_hex(s: str) -> bool:
    s = s.strip()
    return len(s) > 0 and len(s) % 2 == 0 and HEX_RE.fullmatch(s) is not None

def _decode_hex_ssid(s: str) -> str:
    """Decode a hex string to UTF-8; fall back to original on error."""
    s = s.strip()
    try:
        return bytes.fromhex(s).decode("utf-8", errors="replace")
    except Exception:
        return s

def normalize_ssid_column(df: pd.DataFrame) -> None:
    """
    Inspect wlan.ssid values to see if tshark is outputting hex.
    If most non-empty values look like hex, decode them. Then
    cast to pandas string dtype.
    """
    if "wlan.ssid" not in df.columns:
        return

    s = df["wlan.ssid"].astype("string")

    # Drop NaNs/empties for detection
    non_empty = s.dropna().str.strip()
    non_empty = non_empty[non_empty != ""]
    if non_empty.empty:
        df["wlan.ssid"] = s
        return

    sample = non_empty.head(100)
    hex_count = sum(_looks_like_hex(v) for v in sample)
    frac_hex = hex_count / len(sample)

    # Heuristic: if majority look like hex, assume tshark is in hex mode
    if frac_hex > 0.7:
        decoded = s.map(
            lambda v: _decode_hex_ssid(v) if v is not None and _looks_like_hex(str(v)) else v
        )
        df["wlan.ssid"] = decoded.astype("string")
    else:
        # Already human-readable text; just enforce string dtype
        df["wlan.ssid"] = s.astype("string")


def process_pcapng(file_path, output_dir, erm=True):
    file_path = os.fspath(file_path)
    output_dir = os.fspath(output_dir)
    validate_pcapng_capture(file_path)

    output_file = os.path.join(output_dir, f"{os.path.basename(file_path)}.parquet")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Processing {file_path}...")

    tshark_executable = resolve_tshark_executable()
    tshark_command = [tshark_executable, "-r", file_path, "-M", "100000", "-T", "fields"]

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
    with subprocess.Popen(
        tshark_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        index_col = "aruba_erm.time" if erm else "frame.time_epoch"
        try:
            df = pd.read_csv(
                proc.stdout,
                sep=",",
                quotechar='"',
                index_col=index_col,
                parse_dates=True,
                date_format="%b %e, %Y %H:%M:%S.%f %Z" if erm else None,
            )
        except Exception as exc:
            proc.kill()
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(
                f"Failed to parse tshark output for {file_path}: {exc}. "
                f"tshark stderr:\n{stderr}"
            ) from exc

        proc.stdout.close()
        proc.wait()
        if proc.returncode != 0:
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(
                f"tshark failed processing {file_path} with exit code {proc.returncode}. "
                f"Command: {' '.join(tshark_command)}\n"
                f"stderr:\n{stderr}"
            )

    # Build a single frequency_mhz column (numeric) from available sources
    freq = pd.to_numeric(df.get("radiotap.channel.freq"), errors="coerce")
    if "wlan_radio.frequency" in df.columns:
        freq = freq.fillna(pd.to_numeric(df["wlan_radio.frequency"], errors="coerce"))
    df["frequency_mhz"] = freq  # leave NaN if not available

    # --- normalize SSID output from tshark (hex vs text) ---
    normalize_ssid_column(df)

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


def discover_pcapng_files(pcapng_dir):
    pcapng_dir = os.fspath(pcapng_dir)
    pcapng_files = [
        os.path.join(pcapng_dir, f)
        for f in os.listdir(pcapng_dir)
        if os.path.splitext(f)[1].lower() in CAPTURE_FILE_EXTENSIONS
    ]
    for file in pcapng_files:
        validate_pcapng_capture(file, warn_on_legacy_extension=False)

    pcapng_files.sort()
    return pcapng_files


def process_all_pcapngs(pcapng_dir, output_dir, num_processes, erm=True):
    os.makedirs(output_dir, exist_ok=True)
    pcapng_files = discover_pcapng_files(pcapng_dir)
    print(f"Found {len(pcapng_files)} pcapng-format files. Starting processing...")

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
    parser.add_argument(
        "--no-erm",
        action="store_true",
        help=(
            "Convert native/non-Aruba captures without Aruba ERM fields. "
            "The current analysis pipeline still requires Aruba-shaped grouped data."
        ),
    )
    args = parser.parse_args()

    process_all_pcapngs(args.pcapng_dir, args.output_dir, args.num_processes, erm=not args.no_erm)
