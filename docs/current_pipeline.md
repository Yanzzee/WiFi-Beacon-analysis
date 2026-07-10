# Current Beacon Analysis Pipeline

This document describes the current repository pipeline from capture files to final plots and reports. It documents existing behavior only; it does not propose code or analysis logic changes.

## High-Level Flow

1. Capture beacon frames into .pcapng files.
2. Convert .pcapng captures to parquet with selected Wireshark/tshark fields.
3. Group converted parquet rows by AP name or BSSID.
4. Convert sample classroom/AP/schedule CSV files to sample parquet files.
5. Run `analyze_data.ipynb` to extract class-session beacon data, per-radio summaries, and per-session plots.
6. Run `analyze_all_class_beacons.ipynb` to enrich and analyze all sampled class beacons.
7. Run `analyze_radio_summary.ipynb` to analyze one row per radio per class session.
8. Run `analyze_class_sessions.ipynb` to analyze one row per class session/location.

The active pipeline is notebook-centered. The archived `old/analyze_data.py` file is an outdated script version of `analyze_data.ipynb`; it is kept only as a historical reference and should not be treated as a runnable pipeline stage.

## Configuration

Most paths and scope values come from `config.env`.

Important current settings:

- `CAPTURES_FOLDER=Data/captures`
- `PROCESSED_DATA_FOLDER=Data/processed_data`
- `GROUPED_DATA_FOLDER=Data/grouped_data`
- `SAMPLED_FOLDER=Data/sampled`
- `DATA_FOLDER=Data`
- `OUTPUT_FOLDER=Output`
- `SAMPLE_FOLDER=Sample`
- `PLOTS_FOLDER=Output/plots`
- `ALL_CLASS_BEACONS_FOLDER=Output/all_class_beacons`
- `RADIO_SUMMARY_FOLDER=Output/radio_summary`
- `CLASS_SESSIONS_FOLDER=Output/class_sessions`
- `TIMEZONE=Etc/GMT+7`
- `PLOT_TIMEZONE=America/Denver`
- `START_DATE=2025-11-10`
- `END_DATE=2025-11-14`
- `BEACON_RATE=1.024`
- `CAPTURE_MODE=aruba_erm`
- `AP_PLATFORM=aruba`
- `ERM_UDP_PORT=5555`
- `ERM_TYPE=aruba_erm_type0`

Beacon interval note: IEEE 802.11 commonly defaults to a 0.1024 second beacon interval. This pipeline's `BEACON_RATE` is the effective metadata-bearing beacon interval used for analysis, currently 1.024 seconds. That longer interval reflects common vendor behavior where beacon metadata is updated every 10 intervals; Aruba ERM captures/forwards those updates at the longer interval even if over-the-air beacons are transmitted more frequently.

## Execution Order

### 1. Capture Files

Scripts:

- `beacon_capture.sh`
- `beacon_capture_direct.sh`
- `beacon_capture_band.sh`
- `capture_direct.sh`

Inputs:

- Wireless interface named `wlan0`.
- Linux wireless tools such as `ip`, `iw`, `iwconfig`, and `iwlist`.
- `tshark`.
- User-provided location, band, channel, duration, or hardcoded BSSID depending on script.

Outputs:

- .pcapng capture files, usually written outside the repo first, such as `/var/tmp/*.pcapng` or `~/captures/*.pcapng`.
- Files are expected to be moved or placed under `Data/captures` before processing.

Downstream required content:

- Beacon frames from classroom APs.
- 802.11e QBSS fields when available:
  - `wlan.qbss.scount`
  - `wlan.qbss.cu`
  - `wlan.qbss.adc`
- Transmitter/BSSID identifiers.
- Aruba AP name information if using the current Aruba-centered analysis path.

Notes:

- These capture scripts are Linux-specific because they use Linux wireless monitor-mode tools. They are not expected to run unchanged on Windows or macOS.
- `Wireshark-profile-Beacon-QBSS-load.zip` is a Wireshark profile for visual beacon/QBSS inspection, not a required processing step.
- `old/aruba_custom.lua` and `old/aruba_custom_old.lua` are archived Wireshark Lua dissector experiments/helpers for Aruba ERM traffic. The current `process_pcapng.py` uses tshark decode options directly.

### 2. Convert Captures to Processed Parquet

Script:

- `process_pcapng.py`

Typical command:

```powershell
py process_pcapng.py
```

For non-Aruba/native captures:

```powershell
py process_pcapng.py --no-erm
```

This native path is currently conversion-only. Its parquet output does not
feed the analysis notebooks without an additional timestamp and AP-name
mapping/adaptation layer. Full native/non-Aruba analysis support is important
future work, but it should wait until after the current bug fixes and
refactoring.

Inputs:

- `Data/captures/*.pcapng` by default.
- Legacy `*.pcap` filenames are discovered only so `process_pcapng.py` can inspect their magic bytes. If the bytes are pcapng, processing continues with a warning; if the bytes are true pcap, processing fails with a clear unsupported-format error.
- `config.env` values:
  - `CAPTURES_FOLDER`
  - `PROCESSED_DATA_FOLDER`
  - `THREAD_COUNT`
  - `ERM_UDP_PORT`
  - `ERM_TYPE`
- External `tshark` executable.

Fields requested for Aruba ERM captures:

- `aruba_erm.time`
- `wlan.ta`
- `wlan.bssid`
- `wlan.vs.aruba.ap_name`
- `wlan.ssid`
- `wlan.qbss.scount`
- `wlan.qbss.cu`
- `wlan.qbss.adc`
- `radiotap.channel.freq`
- `wlan_radio.frequency`

Fields requested for native 802.11 captures:

- `frame.time_epoch`
- `wlan.ta`
- `wlan.bssid`
- `wlan.ssid`
- `wlan.qbss.scount`
- `wlan.qbss.cu`
- `wlan.qbss.adc`
- `radiotap.channel.freq`
- `wlan_radio.frequency`

Outputs:

- `Data/processed_data/<capture_filename>.pcapng.parquet`
- Adds a normalized `frequency_mhz` column from `radiotap.channel.freq` and/or `wlan_radio.frequency`.
- Attempts to normalize hex-encoded SSIDs into text.

Key downstream columns:

- `aruba_erm.time` for Aruba path, usually as the parquet index immediately after conversion.
- `wlan.ta`
- `wlan.bssid`
- `wlan.vs.aruba.ap_name`
- `wlan.ssid`
- `wlan.qbss.scount`
- `wlan.qbss.cu`
- `wlan.qbss.adc`
- `frequency_mhz`

Fragility:

- `THREAD_COUNT` is read from the environment as a string before being used as the argparse default for an integer argument.
- True pcap input is not currently supported. TShark can read pcap files, but this pipeline now enforces pcapng at the conversion boundary so file naming, discovery, and fixture expectations stay consistent. Adding true pcap support would require separate schema and link-layer validation.
- Native capture mode writes `frame.time_epoch`, but downstream notebooks currently expect `aruba_erm.time` and Aruba AP names.
- If tshark field names or Aruba ERM decode settings change, downstream schemas can break.

### 3. Group Processed Data

Current generic grouping script:

- `group_data_bssid.py`

Aruba-specific grouping script:

- `group_data_aruba.py`

Older reference script:

- `old/group_data_old.py`

Inputs:

- `Data/processed_data/*.parquet`
- `config.env` values:
  - `PROCESSED_DATA_FOLDER`
  - `GROUPED_DATA_FOLDER`

`group_data_bssid.py` behavior:

- Reads all processed parquet data with Dask.
- Requires `wlan.bssid`.
- Groups rows by `wlan.bssid`.
- Uses the first non-empty `wlan.vs.aruba.ap_name` in a group as the output filename when available.
- Falls back to the BSSID as the output filename when AP name is unavailable.
- Writes with `pyarrow.parquet.ParquetWriter`.

This is intended as the generic grouping path for captures where BSSID is the stable grouping key.

Outputs:

- `Data/grouped_data/<AP_name>.parquet` when Aruba AP names are present.
- `Data/grouped_data/<BSSID>.parquet` when AP names are not present.

`group_data_aruba.py` behavior:

- Reads all processed parquet data with Dask.
- Requires `wlan.vs.aruba.ap_name`.
- Groups rows by Aruba AP name.
- Writes one parquet file per AP name to `GROUPED_DATA_FOLDER`.

This is used for Aruba-specific captures that include Aruba AP names.

Current direction:

- `group_data_bssid.py`, `group_data_aruba.py`, and `group_data_cleanup.py` remain separate for now.
- Eventually these should be combined into one script or process that can detect vendor-specific fields and apply the correct grouping and cleanup behavior.

Key downstream columns:

- `aruba_erm.time`
- `wlan.ta`
- `wlan.bssid`
- `wlan.vs.aruba.ap_name`
- `wlan.ssid`
- `wlan.qbss.scount`
- `wlan.qbss.cu`
- `wlan.qbss.adc`

Fragility:

- `analyze_data.ipynb` later constructs filenames from `Sample/sample_aps.parquet` `Hostname`, so grouped output names must match sample AP hostnames.
- If output files are named by BSSID fallback, the current sample-driven notebook path will not find them unless sample hostnames also use those BSSID filenames.
- `group_data_aruba.py` requires `wlan.vs.aruba.ap_name` and is not vendor-neutral.

### 4. Optional Grouped Data Cleanup

Script:

- `group_data_cleanup.py`

Inputs:

- `Data/grouped_data/*.parquet`
- `config.env` values:
  - `GROUPED_DATA_FOLDER`
  - `PLOT_TIMEZONE`
  - `THREAD_COUNT`

Behavior:

- Reads each grouped parquet file.
- If the dataframe index is named `aruba_erm.time`, resets it into a regular column.
- Converts or localizes `aruba_erm.time` to `PLOT_TIMEZONE`.
- Current cleanup timestamp handling is format-dependent: non-datetime values are parsed with `utc=True`, while timezone-naive datetime columns are localized to `PLOT_TIMEZONE`.
- Writes the parquet file back in place with `index=False`.

Outputs:

- Updated `Data/grouped_data/*.parquet` files.

Key downstream columns:

- `aruba_erm.time` as a datetime column rather than only as an index.

Fragility:

- This step mutates grouped parquet files in place.
- It skips files without `aruba_erm.time`, which means native/non-Aruba captures remain incompatible with the notebooks unless adapted elsewhere.
- `analyze_data.ipynb` also resets the dataframe index, so the pipeline can be ambiguous about whether `aruba_erm.time` should be an index or a column at each point.
- This step does not establish a final canonical timestamp contract. Full normalization of varied beacon timestamp formats should be revisited after notebook conversion.

### 5. Build Sample Metadata

Script:

- `samples.py`

Inputs:

- `Sample/Sample_APs.csv`
- `Sample/Sample_classrooms.csv`
- `Sample/Sample_schedule.csv`
- `config.env` values:
  - `SAMPLE_FOLDER`
  - `TIMEZONE`
  - `START_DATE`
  - `END_DATE`

Expected input columns:

- `Sample_APs.csv`:
  - `Hostname`
  - `Building`
  - `Room`
  - `Number`
  - `Building-Room`
  - `Model`
  - `Group`
  - `Notes`
- `Sample_classrooms.csv`:
  - `Location`
  - `Building`
  - `Room`
  - `Capacity`
  - `Group`
  - `Schedule`
- `Sample_schedule.csv`:
  - `Location`
  - `Course`
  - `Sec`
  - `LQ`
  - `H`
  - `S`
  - `Cr Hr`
  - `Sec Type`
  - `Class Period`
  - `Days`
  - `Instructor`
  - `Size`
  - `Enrl`
  - `Status`
  - `Assign To`
  - `Begin Date`
  - `End Date`
  - `sample`

Outputs:

- `Sample/sample_aps.parquet`
- `Sample/sample_classrooms.parquet`
- `Sample/sample_classes.parquet`

Key downstream columns:

- `sample_aps.parquet`:
  - `Hostname`
  - `Building-Room`
  - `Group`
- `sample_classrooms.parquet`:
  - `Location`
  - `Capacity`
  - `Group`
  - `ap_count`
- `sample_classes.parquet`:
  - `location`
  - `course`
  - `start_time`
  - `end_time`
  - `enrolled`
  - `capacity`

Assumptions:

- `Days` uses `M T W R F`, with `R` for Thursday.
- `Class Period` is formatted like `8:00a - 9:50a`.
- `START_DATE` and `END_DATE` define the inclusive date range for expanding recurring class sessions.
- Generated `start_time` and `end_time` are localized using `TIMEZONE`.
- Sections in the same class/location should already be combined before input.

Fragility:

- Use the exact script name `samples.py`; casing matters on case-sensitive filesystems.
- Input schedule CSV includes non-breaking spaces in headers/values; `samples.py` cleans cell values and strips column names, but schema changes can still break the explicit `astype` call.
- Course schedule time parsing currently expects the known `Class Period` shape. Broader normalization of varied class schedule time formats should be revisited after notebook conversion.
- `TIMEZONE=Etc/GMT+7` is a fixed offset, not DST-aware. `PLOT_TIMEZONE=America/Denver` is DST-aware, so these can disagree for data outside the current date range.

### 6. Analyze Sampled Class Beacon Data

Notebook:

- `analyze_data.ipynb`

Inputs:

- `Sample/sample_classes.parquet`
- `Sample/sample_aps.parquet`
- `Sample/sample_classrooms.parquet`
- `Data/grouped_data/<Hostname>.parquet`
- `config.env` values:
  - `DATA_FOLDER`
  - `OUTPUT_FOLDER`
  - `PLOTS_FOLDER`
  - `TIMEZONE`
  - `SAMPLED_FOLDER`
  - `SAMPLE_FOLDER`
  - `GROUPED_DATA_FOLDER`
  - `BEACON_RATE`

Behavior:

- Iterates over each class session from `sample_classes.parquet`.
- Finds APs whose `Building-Room` matches the class `location`.
- Loads `Data/grouped_data/<Hostname>.parquet` for each AP.
- Validates grouped AP files before analysis. They must contain required Aruba analysis columns and `aruba_erm.time` must already be a timezone-aware datetime dtype.
- Filters beacon rows to the class start/end time.
- Converts `aruba_erm.time` to `TIMEZONE`.
- Removes near-duplicate beacons by `wlan.ta` using `0.95 * BEACON_RATE`.
- Validates that every retained, deduplicated in-scope row has a non-null QBSS channel-utilization value. If CU is missing, analysis fails before integer conversion with the AP, class interval, affected count/percentage, and example source rows.
- Replaces missing QBSS station-count and admission-capacity values with zero, then converts the QBSS columns to non-null integers. The existing CU `fillna(0)` remains in the notebook but is now guarded: missing CU cannot reach it through the supported analysis path.
- Requires `AP_PLATFORM=aruba` before analysis because the current radio inference is Aruba-specific.
- Infers `radio_number` from sorted transmitter MAC integer gaps.
- Calculates `total_scount` by combining station counts across transmitters for the inferred radio.
- Calculates per-radio metrics and plots channel utilization/station counts.

Outputs:

- `Data/sampled/<AP_name>_class_beacons.parquet`
- `Data/all_class_beacons.parquet`
- `Data/radio_summary.parquet`
- `Output/plots/<AP_name>_<YYYY-MM-DD_HH-MM>_R<radio_number>.png`
- `Output/analyze_data_log_<timestamp>.txt`

Key downstream columns in `Data/all_class_beacons.parquet`:

- `aruba_erm.time`
- `wlan.ta`
- `wlan.vs.aruba.ap_name`
- `wlan.ssid`
- `wlan.qbss.scount`
- `wlan.qbss.cu`
- `wlan.qbss.adc`
- `wlan.ta_int`
- `radio_number`
- `total_scount`

Key downstream columns in `Data/radio_summary.parquet`:

- `subject`
- `location`
- `AP_hostname`
- `radio_number`
- `start_time`
- `end_time`
- `capacity`
- `enrolled`
- `AP_count`
- `radio_count`
- `bssid_count`
- `ssid_count`
- `total_beacons`
- `high_cu_beacons`
- `percent_high_cu`
- `highest_cu`
- `median_cu`
- `lowest_cu`
- `high_scount`
- `median_scount`
- `low_scount`
- `longest_duration_high_cu`
- `graph_filename`

Fragility:

- This notebook is the current source of truth; the `.py` export is marked outdated.
- It assumes grouped filenames are AP hostnames from `Sample_APs.csv`.
- It assumes `wlan.vs.aruba.ap_name` is present and has one unique value per AP/session slice.
- It now fails fast when grouped `aruba_erm.time` is missing, timezone-naive, or stored as strings/objects. This is validation only; it does not normalize varied input timestamp formats.
- Radio numbering is inferred from transmitter MAC integer gaps. The notebook now fails before analysis unless `AP_PLATFORM=aruba`; this validates the supported platform but does not prove that every observed Aruba MAC allocation is complete or correctly grouped. Channel-based or vendor-neutral inference remains deferred until after notebook conversion.
- `BEACON_RATE` is used for deduplication and duration math. The configured value is the effective metadata interval, not the raw IEEE beacon interval.
- Some SSID-specific plotting lines are commented or temporarily disabled for performance.

### 7. Analyze All Class Beacons

Notebook:

- `analyze_all_class_beacons.ipynb`

Inputs:

- `Data/all_class_beacons.parquet`
- `Sample/sample_aps.parquet`
- `Data/sampled/*.parquet` indirectly referenced in comments/lookup context
- `config.env` values:
  - `DATA_FOLDER`
  - `OUTPUT_FOLDER`
  - `SAMPLE_FOLDER`
  - `ALL_CLASS_BEACONS_FOLDER`
  - `SSID1`
  - `SSID2`

Behavior:

- Maps `wlan.vs.aruba.ap_name` to classroom `location` using `sample_aps.parquet`.
- Computes `room_scount` by looking across APs in the same room within a plus/minus 5 second search window.
- Filters `all_class_beacons_df` to the configured `SSID1` and `SSID2` values.
- Computes aggregate channel utilization and station count statistics.
- Generates histograms, PDF/CDF plots, hexbins, and regression values.

Outputs:

- `Data/all_class_beacons_processed.parquet`
- `Output/analyze_all_class_beacons_log_<timestamp>.txt`
- `Output/all_class_beacons/Channel_use_histogram.png`
- `Output/all_class_beacons/Channel_use_pdf.png`
- `Output/all_class_beacons/Channel_use_cdf.png`
- `Output/all_class_beacons/Scount_histogram.png`
- `Output/all_class_beacons/Scount_channel_use_hexbin.png`
- `Output/all_class_beacons/Scount_adc_hexbin.png`
- `Output/all_class_beacons/adc_channel_use_hexbin.png`

Key downstream columns:

- Existing columns from `Data/all_class_beacons.parquet`.
- Added/required:
  - `location`
  - `room_scount`

Fragility:

- The SSID filter uses `SSID1` and `SSID2` from `config.env`, with current defaults matching `eduroam` and `BYU-WiFi`.
- Room-level station count depends on hostname-to-location mapping from `Sample_APs.csv`.
- The plus/minus 5 second lookup window is embedded in notebook logic.
- This notebook must run before `analyze_class_sessions.ipynb`.

### 8. Analyze Radio Summary

Notebook:

- `analyze_radio_summary.ipynb`

Inputs:

- `Data/radio_summary.parquet`
- `config.env` values:
  - `DATA_FOLDER`
  - `OUTPUT_FOLDER`
  - `RADIO_SUMMARY_FOLDER`

Behavior:

- Treats each row as one radio for one class session.
- Computes class duration from `start_time` and `end_time`.
- Filters out rows with fewer than 300 beacons and non-positive class duration.
- Computes `beacons_per_second`.
- Plots beacon rate per SSID against highest channel utilization.
- Splits plots by `radio_count`.
- Exports CSV reports.

Outputs:

- `Output/radio_summary.csv`
- `Output/filtered_radio_summary.csv`
- `Output/analyze_radio_summary_log_<timestamp>.txt`
- `Output/radio_summary/Beacons_per_second_class_cu.png`
- `Output/radio_summary/Beacons_per_second_class_split.png`
- `Output/radio_summary/Beacons_per_second_class_linear_regression.png`
- `Output/radio_summary/Beacons_per_second_class_histogram.png`
- `Output/radio_summary/Beacons_per_second_stacked.png`
- `Output/radio_summary/Beacons_per_second_class_PDF.png`
- `Output/radio_summary/Beacons_per_second_class_CDF.png`

Key columns:

- `start_time`
- `end_time`
- `total_beacons`
- `ssid_count`
- `radio_count`
- `highest_cu`
- `median_cu`
- `high_scount`
- `longest_duration_high_cu`

Fragility:

- The filter threshold of 300 beacons is embedded in the notebook.
- Several plots divide by `ssid_count`; zero or missing `ssid_count` would break or distort outputs.
- It expects `highest_cu` and related CU values to already be percentages, as produced by `analyze_data.ipynb`.

### 9. Analyze Class Sessions

Notebook:

- `analyze_class_sessions.ipynb`

Inputs:

- `Sample/sample_classes.parquet`
- `Sample/sample_aps.parquet`
- `Sample/sample_classrooms.parquet`
- `Data/all_class_beacons_processed.parquet`
- `config.env` values:
  - `DATA_FOLDER`
  - `OUTPUT_FOLDER`
  - `SAMPLE_FOLDER`
  - `CLASS_SESSIONS_FOLDER`
  - `BEACON_RATE`
  - `SSID1`

Behavior:

- Starts from sample class sessions.
- Adds class-level fields such as:
  - `group`
  - `number_aps`
  - `number_radios`
  - `number_beacons`
  - `max_stations`
  - `max_cu`
  - `average_cu`
  - `median_cu`
  - `stations_enrolled`
  - `class_duration`
  - `duration_75`
  - `duration_50`
  - `max_streak_75`
  - `max_streak_50`
  - `sum_scount_75`
  - `sum_scount_50`
- Filters `Data/all_class_beacons_processed.parquet` by session time and `location`.
- Uses `room_scount` for maximum station count.
- Uses `wlan.qbss.cu` for CU threshold analysis.
- Creates `BEACON_RATE`-wide bins anchored at the class start time. Only complete bins produced by `pd.date_range(start, end, freq=...)` are evaluated; a trailing partial interval is not evaluated.
- Calculates high-CU duration and streaks across all beacons in the location, not independently per radio. A nonempty bin is high for a threshold when any classroom beacon in that bin exceeds the threshold; it does not require every radio to be high.
- Adds one `BEACON_RATE` interval to duration and the current streak for each high bin. A nonempty bin with no value above the threshold resets that threshold's streak.
- Skips empty bins without adding duration and without resetting the current streak. High bins on opposite sides of one or more empty bins can therefore be part of the same reported streak, but the empty-bin time is not included in the reported streak seconds.
- The reported maximum streak is consequently a count of observed high bins separated by no observed non-high bin, rather than a guarantee of uninterrupted high CU in wall-clock time.
- Counts unique radios with `wlan.ta` for the configured primary SSID, `SSID1`.

Outputs:

- `Output/analyze_class_sessions_log_<timestamp>.txt`
- `Output/class_sessions/High_CU_streaks.png`
- `Output/class_sessions/High_CU_streaks_zoomed.png`
- `Output/class_sessions/stations_enrolled_histogram.png`
- `Output/class_sessions/stations_enrolled_plot.png`

Key columns:

- From `Sample/sample_classes.parquet`:
  - `location`
  - `course`
  - `start_time`
  - `end_time`
  - `enrolled`
  - `capacity`
- From `Sample/sample_aps.parquet`:
  - `Building-Room`
- From `Sample/sample_classrooms.parquet`:
  - `Location`
  - `Group`
- From `Data/all_class_beacons_processed.parquet`:
  - `aruba_erm.time`
  - `location`
  - `room_scount`
  - `wlan.qbss.cu`
  - `wlan.qbss.scount`
  - `wlan.ssid`
  - `wlan.ta`

Fragility:

- Requires `Data/all_class_beacons_processed.parquet`, so `analyze_all_class_beacons.ipynb` must run first.
- Counts radios only for the configured primary SSID, `SSID1`.
- Uses `BEACON_RATE` as the bin/window duration for high-CU duration and streak math.
- The current location-wide streak can join high observations from different radios or be reset by a non-high observation from a different radio. It is not the longest independently calculated radio streak. The post-refactor target is to calculate streaks per stable radio identity and report the longest single-radio streak for the class location.
- Newly generated supported-path data cannot silently convert missing CU to zero because `analyze_data.ipynb` now fails first. Historical outputs created before the guard, or data produced outside the supported path, may still conflate imputed zero with measured zero because they do not carry missing-value provenance.
- If `room_scount` is missing, class-level station metrics cannot be computed.

## Scripts and Notebooks Not Normally Run as Pipeline Stages

- `old/analyze_data.py`: archived stale/outdated script version of `analyze_data.ipynb`; comments warn of a fatal bug.
- `test.ipynb`: scratch notebook for installs and parquet inspection.
- `old/strip_Aruba_ERM.py`: archived Scapy-based Aruba ERM stripping helper; comments say it should not be needed and is redundant to current processing.
- `old/group_data_old.py`: archived original AP-name grouping reference; comments recommend `group_data_bssid.py` for data with BSSID.
- `old/aruba_custom.lua`: archived custom Aruba dissector experiment/helper.
- `old/aruba_custom_old.lua`: archived older custom Aruba dissector.
- `old/requirements.old.txt`: archived environment/dependency reference, not the current dependency file.

## Core Downstream Schema

The notebooks rely on these beacon columns being available by the time data reaches `analyze_data.ipynb`:

- `aruba_erm.time`
- `wlan.ta`
- `wlan.bssid`
- `wlan.vs.aruba.ap_name`
- `wlan.ssid`
- `wlan.qbss.scount`
- `wlan.qbss.cu`
- `wlan.qbss.adc`

Additional columns introduced by the pipeline:

- `frequency_mhz`: added in `process_pcapng.py`.
- `wlan.ta_int`: added in `analyze_data.ipynb`.
- `radio_number`: inferred in `analyze_data.ipynb`.
- `total_scount`: calculated in `analyze_data.ipynb`.
- `location`: added in `analyze_all_class_beacons.ipynb`.
- `room_scount`: calculated in `analyze_all_class_beacons.ipynb`.

## Key Assumptions

### Aruba Fields

- The main analysis path assumes Aruba ERM fields are available.
- `wlan.vs.aruba.ap_name` is used as the AP hostname and must map to sample AP `Hostname`.
- `aruba_erm.time` is the canonical timestamp in analysis notebooks.
- Non-Aruba/native captures may be converted by `process_pcapng.py --no-erm`, but downstream notebooks are not currently generalized for `frame.time_epoch` or missing Aruba AP names.

### BSSID and AP Naming

- Processed data includes both `wlan.ta` and `wlan.bssid`.
- `group_data_bssid.py` groups by `wlan.bssid`, but output filenames prefer `wlan.vs.aruba.ap_name`.
- `analyze_data.ipynb` loads grouped files by `Sample_APs.csv` `Hostname`.
- Therefore, AP hostname strings in sample metadata must match grouped parquet filenames exactly.

### Timestamps and Timezone

- Capture conversion uses `aruba_erm.time` as the index for Aruba ERM data.
- Cleanup and notebooks expect or create an `aruba_erm.time` datetime column.
- `samples.py` localizes class schedule timestamps to `TIMEZONE`.
- `analyze_data.ipynb` converts beacon timestamps to `TIMEZONE`.
- `group_data_cleanup.py` uses `PLOT_TIMEZONE`, which may not match `TIMEZONE`.
- Current `TIMEZONE=Etc/GMT+7` is a fixed offset and does not handle DST changes.
- Until notebook conversion, grouped parquet input to `analyze_data.ipynb` must already store `aruba_erm.time` as a timezone-aware datetime dtype. Varying beacon timestamp strings, naive datetimes, and varying class schedule time formats need a dedicated normalization design after conversion to native Python.

### Beacon Interval

- IEEE 802.11 commonly defaults to a 0.1024 second beacon interval.
- This application's default `BEACON_RATE=1.024` represents the effective interval for metadata-bearing beacon updates used by the analysis.
- Most vendors update beacon metadata every 10 beacon intervals, and Aruba ERM captures/forwards those metadata updates at this longer interval even when beacons are transmitted over the air every 0.1024 seconds.
- `analyze_data.ipynb` keeps beacons spaced at least `0.95 * BEACON_RATE` apart by transmitter.
- `analyze_data.ipynb` adds `BEACON_RATE` to high-CU sequence duration calculations.
- `analyze_class_sessions.ipynb` uses `BEACON_RATE` as `BEACON_WINDOW_SEC` for high-CU duration and streak bins.
- If the capture source or vendor behavior changes the effective metadata interval, review `BEACON_RATE` and the golden fingerprint together before changing analysis behavior.

## Known Fragility and Ambiguity

Analysis and architecture changes intentionally deferred until after notebook conversion are tracked in `docs/post_refactor_design.md`.

- Grouping and grouped-data cleanup remain split across `group_data_bssid.py`, `group_data_aruba.py`, and `group_data_cleanup.py`; future work should merge vendor detection, grouping, and cleanup into one process.
- The current operational source is the notebooks, not archived `old/analyze_data.py`.
- Notebooks have no markdown guidance and no saved outputs, so execution order is mostly external knowledge.
- `Data/`, `Output/`, and `Sample/` are ignored in `.gitignore`, but some historical sample files are tracked.
- `requirements.txt` appears environment-derived and includes standard-library names such as `glob`, `pathlib`, and `datetime`.
- Current local `venv` may not be reproducible as-is; `venv/pyvenv.cfg` points `home` and `executable` back into the venv.
- Several thresholds are embedded in notebooks:
  - CU greater than 191 means greater than 75 percent.
  - CU greater than 127 means greater than 50 percent.
  - radio summary filters rows with fewer than 300 beacons.
  - room station count lookup uses a plus/minus 5 second window.
- SSID filters now read `SSID1` and `SSID2` from `config.env`; `analyze_class_sessions.ipynb` uses `SSID1` as the primary radio-count SSID.
- AP radio inference is Aruba-specific and based on MAC integer gaps, not explicit channel/radio metadata.
- Some stages write in place or append to existing parquet files, so reruns may require manual cleanup to avoid stale or duplicated outputs.
