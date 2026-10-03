# Dynamic one-extraction baseline — 2026-09-23

## Scope

This is the first observed baseline for the PR-0032 dynamic TShark field
planner. It measures the exact full-extraction command after bounded capture-
profile detection. Each full extraction was repeated three times. It is not a
release budget, scalability claim, comparison to the legacy pipeline, or
measurement of the complete Python normalization/selection runner.

The harness is `benchmark_tshark_extraction.py`. It records the source hash,
detected profile, probe/full-pass counts, planned fields and adapter versions,
rows, output bytes, wall time, TShark CPU time, and TShark-process peak RSS.
Peak RSS is optional when `psutil` is unavailable.

## Environment

| Item | Observed value |
|---|---|
| Operating system | Windows 11, build 10.0.26200, 64-bit |
| Python | 3.13.14 |
| Logical CPUs | 24 |
| Visible memory | 68,401,475,584 bytes (approximately 63.7 GiB) |
| TShark | Wireshark 4.6.6, `v4.6.6-0-g3a22c3ef473d` |
| Aruba/Extreme adapter versions | 3 / 3 |
| Full-extraction display filter | `wlan.fc.type_subtype == 8` |
| Repetitions | 3 per capture |

## Results

Wall time is the median of three full extractions. Peak RSS is the maximum
observed TShark-process RSS across those runs. Profile detection is measured
once and reported separately; the current detector uses one bounded probe for
native input and four for ERM input.

| Capture | Profile | Capture bytes | Rows | Fields | Output bytes | Detection s | Full extraction median s | CPU median s | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `Aruba_ERM_Capture_635_type0_2026-09-14.pcapng` | ERM type 0 | 98,140 | 184 | 20 | 40,619 | 0.828 | 0.216 | 0.219 | 114.8 |
| `Aruba_ERM_Capture_635_type3_2026-09-14.pcapng` | ERM type 3 | 157,584 | 298 | 20 | 68,493 | 0.825 | 0.233 | 0.234 | 115.3 |
| `Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng` | ERM type 6 | 871,816 | 1,523 | 20 | 267,310 | 1.020 | 0.289 | 0.297 | 116.3 |
| `Beacons_LVES_fr5220_00004_20260905195309.pcapng` | Native 802.11 | 17,621,824 | 36,969 | 17 | 6,742,691 | 0.282 | 2.065 | 2.016 | 129.0 |
| `Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng` | Native 802.11 | 1,669,384 | 4,148 | 17 | 799,383 | 0.268 | 0.405 | 0.438 | 118.2 |

Capture SHA-256 values:

| Capture | SHA-256 |
|---|---|
| `Aruba_ERM_Capture_635_type0_2026-09-14.pcapng` | `8e1d17f9d2b924a1b5d62914e30328db53be4c7a61e5ee200ed5442e9d3ae43f` |
| `Aruba_ERM_Capture_635_type3_2026-09-14.pcapng` | `0784bae9244c3583776111794e540656c9e7dcca35a306360d3787add55e7177` |
| `Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng` | `6d524e4c5a527cf12697500747ac38f5a9129d8e9368aeeb184a2932ab96a272` |
| `Beacons_LVES_fr5220_00004_20260905195309.pcapng` | `566ea91a2ac19c192e530e2161b84e51087a2c37d90219685cde5976d7cd8116` |
| `Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng` | `b2b549e99e2b4de047be31e8a8b0b8f1899b4d42bc01684d9d63ebc0e69321e1` |

## Interpretation

- The reviewed extraction workloads do not demonstrate a need for chunking or
  temporary spill storage. Do not add either mechanism at this stage.
- The 17.6 MB native capture is the largest reviewed input in this baseline;
  these results do not define supported maximum capture size.
- Peak RSS covers the TShark process, not the complete Python DataFrame,
  normalization, selection, or durable-output pipeline. Corrected-runner
  integration needs its own later end-to-end measurement before a full memory
  limit or 102.4 ms-bin scalability claim is accepted.
- ERM profile detection is currently more expensive than the small-file full
  extraction because it performs four bounded decoder probes. No optimization
  is justified from these small captures alone.
- No performance pass/fail threshold is established by this baseline.

## Reproduction

Run from the repository root with the desired captures:

```powershell
.\.venv\Scripts\python.exe benchmark_tshark_extraction.py `
  --repeats 3 `
  Data\captures_test\Aruba_ERM_Capture_635_type0_2026-09-14.pcapng `
  Data\captures_test\Aruba_ERM_Capture_635_type3_2026-09-14.pcapng `
  Data\captures_test\Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng `
  Data\captures_test\Beacons_LVES_fr5220_00004_20260905195309.pcapng `
  Data\captures_test\Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng
```

JSON is written to standard output. `--output <path>` may be used for an
explicit run-scoped result file.
