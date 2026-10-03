# Retained-capture TSF and duplicate characterization

- Status: Evidence complete for current-Aruba TSF progression, duplicate, and
  fallback behavior; lower-envelope binning oracle accepted separately
- Initial characterization: 2026-09-01
- Controlled TShark 4.6.6 replay: 2026-09-02
- Related decisions:
  [ADR-0008](decisions/0008-aruba-capture-timestamp-authority-and-fallback.md)
  and
  [ADR-0009](decisions/0009-tbtt-tsf-beacon-slot-and-bin-semantics.md)

## Purpose and current conclusion

This report records what the retained Aruba ERM captures show about beacon
TSF, sequence progression, and duplicate capture views. It changes no runtime
behavior, schema, fixture, output, or golden.

The owner subsequently refined the timing requirement:

- valid Aruba observation time, otherwise frame time, admits beacons to
  classes;
- reliable TSF is evaluated separately per BSSID/class session, anchored by
  the earliest valid `O-F` candidate, and used for bin placement, ordering,
  duplicate recognition, and skipped-interval diagnostics;
- missing, frozen, or clearly reversing TSF is ignored without dropping
  beacons by falling back to observation time for that BSSID/session; and
- no cross-BSSID timing pool, clock-drift analysis, segmentation, correction,
  or class rejection is required.

The replay evidence supports that simpler contract. In particular, the actual
LSB-2102 AP population has overwhelmingly frozen TSF and therefore should be
processed normally using Aruba observation time.

## Evidence scope

| Capture | Size | SHA-256 |
|---|---:|---|
| `Beacon_Capture_20251110090000_00012.pcapng` | 439.4 MiB | `BC458E33D4C5BDE71CC46F3ABFEC678A158CDB5975D8B4361488D4B26E3AB03E` |
| `Beacon_Capture_20251110100000_00013.pcapng` | 421.0 MiB | `3CA41EE06573A985395F01CC9F941903200E2FC2F277DC882F75D794D0AD58B3` |
| `Beacon_Capture_20251110110000_00014.pcapng` | 371.2 MiB | `8B10D6101A7C2428C2B7E169CABA60B6E00608095B6D41D011ADE04659514578` |

The controlled replay used portable TShark/Wireshark `4.6.6`
(`v4.6.6-0-g3a22c3ef473d`) with isolated preferences and the required Aruba
ERM type-0 decode settings. The replay produced no preferences warning or
TShark stderr output.

Each capture contributed its half-open 50-minute top-of-hour class period:

- `00012`: 09:00–09:50 MST;
- `00013`: 10:00–10:50 MST; and
- `00014`: 11:00–11:50 MST.

The replay extracted exact Aruba and frame observation times, capture/frame
lineage, BSSID, AP name, TSF, advertised beacon interval, and QBSS values. It
evaluated all decoded beacon rows and all forward TSF transitions.

Native over-the-air captures, other vendors, reset/reboot examples, and the
sparse 300-TU population are not supported by this evidence. Those are not
needed for the simplified current-Aruba result.

## Controlled replay results

| Capture | Beacon rows | 100 / 300 TU rows | Frozen/provisional TSF rows | Distinct-progressing eligible rows |
|---|---:|---:|---:|---:|
| `00012` | 890,637 | 890,421 / 216 | 209,638 | 680,999 |
| `00013` | 841,824 | 841,458 / 366 | 211,172 | 630,652 |
| `00014` | 722,715 | 720,252 / 2,463 | 200,011 | 522,704 |
| **Total** | **2,455,176** | **2,452,131 / 3,045** | **620,821** | **1,834,355** |

All provisional rows used exact TSF `0` or `56`, and an affected BSSID kept
one sentinel value through the observed class period. The beacon rows remain
valid observations; only TSF assistance is unavailable.

The broader population also showed strong normal TSF progression when TSF was
usable. This supports using distinct TSF values for sequence order and using
positive interval-sized jumps as evidence that beacon opportunities were
skipped. The simplified decision does not need a universal residual threshold
or an accepted numeric profile.

## Actual LSB-2102 production classes

The retained production fixture maps these class periods to LSB-2102 and
three classroom APs:

| Capture | Production AP beacon rows | Frozen `F=56` rows | Other TSF rows |
|---|---:|---:|---:|
| `00012` | 52,943 | 52,542 | 401 |
| `00013` | 50,410 | 50,026 | 384 |
| `00014` | 58,662 | 58,223 | 439 |
| **Total** | **162,015** | **160,791 (99.2445%)** | **1,224** |

No production stream had sustained usable TSF progression. Under revised
ADR-0009 this is not a class-level failure. It means TSF-derived placement is
unavailable for those BSSID/class sessions. The processor keeps the beacons,
uses Aruba observation time for bin placement and ordering, and selects the
first observed beacon per BSSID/bin.

## Cross-endpoint duplicate evidence

The replay found 22 `(BSSID, advertised interval, TSF)` keys visible through
more than one outer capture endpoint: 16, 2, and 4 by capture. Their Aruba
observation times differed by as much as 8.100 ms, and none had conflicting
QBSS payload values.

This supports the revised duplicate rule:

1. the same BSSID and TSF can identify duplicate views of one beacon;
2. apply source precedence before timestamp comparison;
3. keep the earliest observation within that precedence; and
4. use durable capture/frame lineage for a remaining tie.

Do not require duplicate views to have identical observation timestamps.

## Retired analyses and retained lower envelope

The replay also explored `O-F` lower envelopes, compatible-BSSID timing pools,
and changes between Aruba and frame clocks. The owner later accepted only the
simple per-BSSID/session lower envelope `B=min(O-F)` for reliable TSF-derived
bin placement.

The owner clarified that clock drift is not a project concern and that TSF is
used solely to address out-of-order, delayed, missing, or duplicated beacons.
The following more complex elements are retired and are not readiness gates:

- lower-envelope support-count thresholds or a claimed literal AP boot time;
- `R/20` progression or clock-change thresholds;
- compatible-BSSID timing pools;
- one-minute change screens and before/after persistence windows;
- diagnostic dual-clock corroboration;
- local linear detrending;
- clock-domain segmentation or normalization; and
- whole-class rejection or fallback caused by clock behavior.

Historical details remain available in version control if a confirmed future
requirement makes them relevant.

## Readiness and next step

The retained current-Aruba evidence step is complete for progression,
duplicate, and frozen-TSF fallback behavior. The accepted lower-envelope
binning calculation is defined by the small oracle specification; no numeric
timing-profile approval remains.

Before implementation, create small fixture oracles for:

- observation-time source precedence;
- observation-time class admission and wrong-day/hour exclusion;
- per-BSSID/session `min(O-F)` anchoring and a delayed first beacon;
- reliable TSF moving a row to a different bin than observation time;
- same-BSSID/same-TSF duplicate views;
- out-of-order distinct TSF values;
- skipped intervals without synthesized rows;
- missing/frozen/reversing/incompatible TSF using whole-BSSID/session
  observation-time fallback; and
- first-observed per-BSSID/class-start-bin selection.

ADR-0004 now defines the separate missing-beacon/CU, coverage, and streak
semantics. Its fixture oracles and metric-version migration remain future
implementation work.
