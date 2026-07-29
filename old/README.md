# Archived Files

Files in this folder are obsolete or historical references. They are not part of the normal Beacon-analysis pipeline and should not be used as runnable stages without a deliberate review.

- `analyze_data.py`: outdated script version of `../analyze_data.ipynb`; comments warn of a fatal bug.
- `group_data_old.py`: earlier grouping reference superseded by `../group_data_bssid.py` and `../group_data_aruba.py`.
- `strip_Aruba_ERM.py`: old Scapy-based Aruba ERM stripping helper; current conversion uses `../process_pcapng.py`.
- `aruba_custom.lua` and `aruba_custom_old.lua`: Wireshark Lua dissector experiments/helpers for Aruba ERM traffic.
- `requirements.old.txt`: historical dependency reference, not the active dependency file.
