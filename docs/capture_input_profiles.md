# Capture input profiles

This document records the tested capture containers, Aruba ERM payload types,
and native 802.11 input boundary used by `process_pcapng.py`. Detection is
performed independently for every file from decoded packet content. A filename,
GUI label, or one project-wide ERM setting is not treated as evidence of the
payload shape.

## Current recommendation

For the current channel-utilization workflow, Aruba ERM **type 3 (PCAP plus
radio header)** is preferred when the capture source offers it. It retains
`aruba_erm.time`, which is the preferred observation timestamp, and adds the
ERM radio channel. Beacon IEs still supply TSF, QBSS, AP name, SSID, and
band/channel evidence. Although some capture profiles expose rate or signal
metadata, the current extraction plan does not request it because no accepted
metric consumes it.

There is no format that contains every useful field:

- Type 0 (PCAP) retains Aruba time and the required beacon IEs, but has no ERM
  radio-header channel field.
- Type 3 retains Aruba time and adds ERM radio metadata. It is the preferred
  default for the present analysis.
- Type 6 (radiotap) supplies frequency metadata, but does not expose
  `aruba_erm.time` in the tested capture. It therefore uses `frame.time_epoch`
  for class admission and TSF anchoring.
- A native over-the-air radiotap capture supplies ordinary 802.11/radiotap
  fields directly and also uses `frame.time_epoch`. AP-name availability then
  depends on a supported vendor IE or a later explicit BSSID mapping.

Choose type 6 instead of type 3 when frequency inspection is the main purpose
and loss of the source-stamped Aruba time is acceptable. RSSI inspection can
still be performed directly in the source capture, but it is not part of the
current application extraction plan.

## Owner-supplied GUI capture evidence — 2026-09-14

All three Aruba files were captured from the Wireshark GUI on Ethernet with UDP
port 5555 and decoded with portable TShark 4.6.6. Each capture sequentially
covered the 2.4, 5, and 6 GHz radios of the same Aruba AP. Counts below are
non-empty values among rows decoded as beacons; a small number of malformed or
truncated beacon bodies account for most missing IE values. A few malformed
bodies also produced nonsensical decoded identifiers or intervals; these are
not evidence of another AP/radio, and the configured-hostname and identity
safety checks prevent them from being silently promoted into project scope.

| Field/evidence | Type 0 (184 rows) | Type 3 (298 rows) | Type 6 (1,523 rows) |
|---|---:|---:|---:|
| `frame.time_epoch` | 184 | 298 | 1,523 |
| raw/rendered `aruba_erm.time` | 184 | 298 | 0 |
| `wlan.fixed.timestamp` (TSF) | 181 | 292 | 1,516 |
| `wlan.fixed.beacon` | 181 | 292 | 1,516 |
| Aruba AP name | 180 | 288 | 1,495 |
| QBSS CU/stations/admission capacity | 181 | 290 | 1,509 |
| DS current channel | 163 | 121 | 1,384 |
| HE 6 GHz primary channel | 18 | 169 | 128 |
| `aruba_erm.channel` / `wlan_radio.channel` | 0 / 0 | 298 / 298 | 0 / 1,523 |
| ERM data rate / signal percent | 0 / 0 | 298 / 298 | 0 / 0 |
| radiotap or WLAN-radio frequency | 0 | 0 | 1,523 |
| radiotap or WLAN-radio dBm | 0 | 0 | 1,523 |

Type 3 channel values were 11, 104, and 165. Type 6 additionally exposed
2,462, 5,520, and 6,775 MHz, which unambiguously distinguish bands even when
channel numbers overlap across bands. Type 0 still provides channel/band
evidence through the DS and HE operation IEs, even though it has no radio
header.

Conversion retains the tested source-native channel, rate, signal-percentage,
frequency, and dBm fields when present. It does not convert ERM signal
percentage into dBm or otherwise claim those values are directly comparable.

Capture identity:

| File | SHA-256 |
|---|---|
| `Aruba_ERM_Capture_635_type0_2026-09-14.pcapng` | `8e1d17f9d2b924a1b5d62914e30328db53be4c7a61e5ee200ed5442e9d3ae43f` |
| `Aruba_ERM_Capture_635_type3_2026-09-14.pcapng` | `0784bae9244c3583776111794e540656c9e7dcca35a306360d3787add55e7177` |
| `Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng` | `6d524e4c5a527cf12697500747ac38f5a9129d8e9368aeeb184a2932ab96a272` |

The representative existing CLI capture remains type 0. Its first 500 decoded
beacons show the same field family as the GUI type-0 sample: Aruba and frame
times, TSF, beacon interval, AP name, QBSS, and DS/HE channel IEs are present;
ERM/radiotap radio metadata is absent. This supports one type-0 decoder contract
for both capture methods, but does not assume all future GUI and CLI exports are
identical.

## Automatic profile detection

The converter uses this bounded per-file procedure:

1. Probe up to the first 2,000 packets without an ERM Decode-As rule. If valid
   802.11 beacon/BSSID rows are directly visible, select `native_80211`.
2. Otherwise probe Aruba ERM types 0, 3, and 6 independently on UDP port 5555.
3. Count syntactically valid beacon/BSSID rows for every decoder and select the
   unique highest count.
4. Reject the file with a clear diagnostic if every count is zero or the best
   result is tied. Do not guess.

This is deliberately a score, not a first-match test. In the three GUI files,
the correct decoders produced 184, 298, and 1,523 plausible rows respectively;
wrong decoders produced zero except for one false-positive type-3 row from the
type-6 capture.

The selected profile is retained on every converted row as `capture_profile`,
with `capture_profile_detection=content_probe` and nullable `erm_type`. The
command-line `--erm-type` and `--no-erm` switches remain explicit overrides for
diagnosis and controlled replay.

Aruba ERM types 1, 2, 4, and 5 are not claimed as supported because no project
fixtures establish their field contracts. They fail safely instead of being
decoded speculatively. More vendor profiles can be added only with a capture,
field comparison, and focused profile test.

## Native 802.11 boundary

Both pcapng and true pcap containers are accepted by magic bytes. The local
`Beacons_LVES_fr5220_00004_20260905195309.pcapng` uses a native radiotap link
layer and is correctly detected as `native_80211`. Native conversion retains
frame time, frame lineage, TSF, beacon interval, BSSID, QBSS,
channel/frequency evidence, and the reviewed Aruba and Extreme AP-name fields
when those IEs are available.

The Extreme adapter recognizes `wlan.vs.extreme.ap_name` and supports
configured-hostname scope, corrected timing/QBSS selection, and shared
AP+band+channel radio identity.

| Extreme reference | Beacon/name evidence | Frequency/channel | SHA-256 |
|---|---|---|---|
| `Beacons_LVES_fr5220_00004_20260905195309.pcapng` | 36,969 beacons; 33,977 named rows; 14 named APs; 39 named BSSIDs | 5220 MHz / 5 GHz channel 44 | `566ea91a2ac19c192e530e2161b84e51087a2c37d90219685cde5976d7cd8116` |
| `Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng` | 4,148 beacons, all Extreme-named; 15 APs; 15 BSSIDs | 6355 MHz / 6 GHz channel 81 | `b2b549e99e2b4de047be31e8a8b0b8f1899b4d42bc01684d9d63ebc0e69321e1` |

All named rows in both files contain TSF and QBSS CU. The common AP
`P-LVES-B105R22S30-AP-01-WN` contributes three channel-44 BSSIDs in the 5 GHz
capture and one channel-81 BSSID in the 6 GHz capture. The shared resolver
groups the three 5 GHz BSSIDs together and keeps the 6 GHz BSSID on a separate
radio. No Extreme-specific MAC pattern is required.

## Future vendor AP-name references — 2026-09-15

Two additional native-radiotap captures contain the named vendor plus mixed
neighboring beacons. They establish only decoded AP-name evidence for future
adapters; each capture observes one 2.4 GHz channel and therefore does not yet
prove same-AP multi-radio grouping or broader vendor support.

| Vendor reference | Beacon rows | Decoded AP-name evidence | Frequency/channel | SHA-256 |
|---|---:|---|---|---|
| `Beacons_ITB-arista_fr2437_00001_20260915110242.pcapng` | 5,363 total; 1,752 Arista-name rows | `wlan.vs.arista.ap_name=ITB-Test` | 2437 MHz / channel 6 | `96e8bbef655e7b6f06d01c59ba31fcc622b8eb3d8bf54dbafad37c5f5f825cce` |
| `Beacons_ITB-mist_fr2462_00001_20260915105927.pcapng` | 7,203 total; 702 Mist-name rows | `wlan.vs.mist.apname=ITB-AP-47-LAB` | 2462 MHz / channel 11 | `dacf791e6af8bc1ef6a3c2efd1af0f9764406bb6048faed71542447da1ad54c` |

TShark 4.6.6 decodes both vendor fields correctly. The current native export
does not yet retain them because Mist/Arista identity support has not been
reviewed or implemented.
