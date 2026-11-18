# Alpha 0.1

Release type: Published public alpha release

Alpha 0.1 is the first public preview of the corrected Beacon Analysis
workflow. It replaces the former multi-script process with one supported
command that reads packet captures and project CSVs, performs the complete
analysis, and publishes an isolated, versioned run.

The earlier [Original refactored release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original)
remains available for reproducing the former calculations. Alpha 0.1 uses new
timing, identity, missing-data, coverage, and streak semantics; its results
should not be assumed to match that historical release.

## Highlights

- One supported command: `beacon_analysis.py run`.
- Direct input from AP, classroom, and class-schedule CSV files.
- Automatic Aruba ERM type 0, 3, and 6 plus native 802.11 capture detection.
- Reviewed Aruba and Extreme AP-name adapters.
- Early filtering to configured AP hostnames.
- BSSID to radio to AP to classroom identity using AP name, band, and channel.
- 2.4, 5, and 6 GHz classification when the capture provides sufficient
  evidence.
- Per-BSSID/session TSF-assisted ordering, duplicate recognition, and bin
  placement, with observation-time fallback.
- One selected beacon per BSSID/bin using deterministic first-observed
  selection.
- Corrected channel-utilization coverage, missing-CU/no-beacon, high and very
  high duration, and actual/possible streak metrics.
- Bounded multi-worker extraction and bounded per-session processing.
- Atomic publication: failed runs do not publish partial results.

## Outputs

Each successful run publishes beneath:

```text
output/runs/<project>/<run-id>/
```

The five durable logical artifacts are:

- `project_identity_map.parquet`;
- partitioned `selected_observations/` Parquet data;
- `radio_class_summary.parquet`;
- `classroom_class_summary.parquet`; and
- `manifest.json`.

Compact CSV review exports, a text run summary and methodology guide, and the
current CU/coverage/streak report package are included as derived outputs.

## Supported environment

- Windows
- 64-bit Python 3.13
- Wireshark/TShark 4.6.6
- Source checkout with dependencies installed from `requirements.txt`

This alpha is intended for technical users. It is not distributed as an
installer or Python package.

## Validation

The release boundary passed:

- 221 current unit and integration tests;
- 16 real-capture/TShark tests;
- the dedicated corrected semantic golden fixture; and
- an end-to-end four-worker smoke run covering 10 captures, 21 configured
  APs, 373 class sessions, and 441,566 selected observations.

## Known limitations

- Station-count rollups are not yet authoritative summary artifacts.
- APs without a supported hostname information element require future explicit
  BSSID mapping.
- Aruba ERM may contain periodic controller/capture-path delivery gaps.
- Vendor support is currently limited to the reviewed Aruba and Extreme
  adapters.
- No runtime service-level guarantee is claimed.
- A future pandas release will change dtype inference when concatenating
  empty/all-null summary frames; current pinned dependencies produce correct
  output but emit a non-fatal warning in the limited-capture smoke case.

## Feedback requested

Alpha review should focus on input usability, AP/radio identity, 5/6 GHz
classification, selected-observation cadence, summary correctness, report
clarity, runtime, and memory use on representative projects.
