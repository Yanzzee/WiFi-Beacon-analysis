# Corrected capture-worker benchmark — 2026-09-29

> Historical boundary: this measurement predates configured-hostname TShark
> filtering and worker-local dispatch/filter/class staging. It is the baseline
> that motivated the newer 1-16-capable bounded design; it does not predict
> 4/8/16 scaling for that design.

This benchmark compares the production corrected pipeline with one and two
capture-extraction workers. It is operational evidence for the current
machine and workload, not a general scalability guarantee.

The machine-readable report is
[`capture_workers_2026-09-29.json`](capture_workers_2026-09-29.json). The
reproducible harness is `benchmark_capture_workers.py`.

## Workload

- Three owner production Aruba ERM type-0 captures copied under
  `data/captures_test`, totaling 1,291,458,744 bytes (about 1.20 GiB).
- Complete raw-CSV-to-publication corrected pipeline.
- 2,870,617 supported beacon rows, 265,766 configured-project rows, and
  184,281 class-window rows.
- 183,045 selected observations, 34 identity rows, 228 radio/class rows, and
  61 classroom/class rows.
- TShark 4.6.6 on Windows.

## Results

| Measurement | One worker | Two workers | Change |
|---|---:|---:|---:|
| Complete wall time | 709.8 s | 664.1 s | 45.7 s faster; 1.069x speedup |
| Sampled process-tree CPU | 736.4 s | 760.0 s | 3.2% higher |
| Peak process-tree RSS | 2.74 GiB | 3.28 GiB | about 20% higher |
| Peak temporary workspace | 10.22 MiB | 10.22 MiB | unchanged |

Logical fingerprints of all four authoritative table artifacts were identical
after excluding the intentionally unique `run_id`. Extraction, project-scope,
class-window, selected-row, identity, and summary counts also matched.

## Interpretation

Two workers are safe for this reviewed workload and provide a modest speedup.
The gain is limited because concurrent TShark processes share CPU and storage,
and normalization/filtering/staging remain deliberately serialized. The
approximately 6.4% wall-time reduction does not justify changing the default
yet, especially with the approximately 20% higher peak memory.

Keep one worker as the default and retain two workers as an explicit
`--capture-workers 2` / `CAPTURE_WORKERS=2` option. Before changing the
default, run the complete 80-capture acceptance project with two workers and
compare its logical artifacts, peak memory, temporary disk, and total runtime
against the completed one-worker run.
