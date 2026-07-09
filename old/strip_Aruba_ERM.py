# Using Scapy to strip Aruba ERM frames from pcapng files
# This shouldn't be needed, redundant to process_data.py

import os
from pathlib import Path
from scapy.all import rdpcap, wrpcap, UDP, RadioTap, Raw
from concurrent.futures import ProcessPoolExecutor
from functools import partial

def strip_aruba_erm(input_path: Path, output_dir: Path):
    try:
        packets = rdpcap(str(input_path))
        cleaned = []

        for pkt in packets:
            if UDP in pkt and pkt[UDP].dport == 5555 and len(pkt[UDP].payload) > 8:
                payload = bytes(pkt[UDP].payload)[8:]  # Strip 8-byte timestamp
                cleaned.append(RadioTap()/Raw(load=payload))

        if cleaned:
            output_path = output_dir / (input_path.stem + "_cleaned.pcapng")
            wrpcap(str(output_path), cleaned)
            print(f"✔ Saved {len(cleaned)} packets to {output_path.name}")
        else:
            print(f"⚠ No Aruba ERM frames found in {input_path.name}")
    except Exception as e:
        print(f"❌ Failed to process {input_path.name}: {e}")

def process_folder(input_folder: str, output_folder: str, workers: int = 4):
    input_dir = Path(input_folder)
    output_dir = Path(output_folder)
    output_dir.mkdir(parents=True, exist_ok=True)

    pcapng_files = list(input_dir.glob("*.pcapng"))

    with ProcessPoolExecutor(max_workers=workers) as executor:
        executor.map(partial(strip_aruba_erm, output_dir=output_dir), pcapng_files)

# Example usage
if __name__ == "__main__":
    process_folder("Data/aruba_captures", "Data/captures", workers=6)
