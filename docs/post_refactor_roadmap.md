# Beacon Analysis Roadmap

The post-refactor corrected pipeline is now the supported workflow. This file
tracks only remaining work; accepted semantics are recorded in the ADRs under
[`decisions/`](decisions/README.md).

## Current release boundary

Status: accepted for source-checkout use and release validation.

Implemented and owner-reviewed:

- `beacon_analysis.py run` as the one-command workflow;
- direct AP/classroom/schedule CSV loading and physical-session expansion;
- Aruba ERM type 0/3/6 and native 802.11 capture-profile detection;
- reviewed Aruba and Extreme AP-name adapters;
- configured-hostname scope filtering;
- BSSID to radio to AP to classroom identity using AP name plus band/channel;
- observation-time admission with per-BSSID/session TSF-assisted ordering,
  duplicate handling, and bin placement;
- first-observed BSSID/bin selection with terminal partial bins;
- corrected CU coverage, missing-CU/no-beacon, high/very-high duration, and
  actual/possible streak semantics;
- five durable artifacts, compact CSVs, project report package, run summary,
  manifest, atomic publication, failure cleanup, and create-only runs;
- bounded concurrent extraction and bounded per-session selection; and
- dedicated corrected semantic golden plus optional real-capture replay.

The verified environment is Windows with Python 3.13 and TShark 4.6.6. Other
platforms may work but are not yet claimed by release evidence.

## Immediate — release validation

1. Keep the ordinary corrected unit/integration suite green.
2. Run the real Aruba/Extreme TShark smoke test when owner captures are
   available.
3. Run a small end-to-end CLI smoke project in the supported environment.
4. Review current README, configuration, output guide, and support limitations.
5. Publish a source-checkout release candidate. An installable package is not
   required for this niche technical-user workflow.

Ctrl+C with cleanup and restart-from-beginning is the accepted cancellation
model. Resume is not planned. Exact within-capture percentage remains deferred
because it requires another decode pass or a larger streaming redesign.

## Next functional decision — station-count semantics

Selected observations already retain one station count per BSSID/bin. The next
analysis decision should define:

- missing/invalid BSSID member behavior;
- whether partial radio/SSID/classroom totals are emitted and how they are
  labeled;
- staleness rules when a member BSSID is not observed in a bin; and
- the durable/report placement of radio/bin, SSID/classroom/bin, and
  band/classroom/bin totals.

After acceptance, implement station summaries and the useful corrected
all-observation, class-session, and radio-summary report views. No historical
calculation pipeline should be restored.

## 6 GHz reporting

The current package includes band-separated CU histogram/CDF panels and an
all-5-GHz versus paired-AP-5-GHz versus 6-GHz population table. Later work may
add:

- quality/comparability warnings;
- richer paired-radio/AP statistics;
- station-count comparisons after station semantics are accepted; and
- broader same-session 5/6 GHz evidence.

Band plus channel identifies radios only within an AP. Different APs on the
same band/channel remain separate, and channel number is never interpreted
without band.

## Performance and scalability

The 80-capture, 49.37-GiB project has completed successfully with bounded
memory, but observed wall time varies substantially between otherwise
identical four-worker runs. Future performance work should first collect:

- CPU, disk throughput/latency, and memory over complete runs;
- per-capture extraction/normalization/staging timings;
- session-selection and selected-Parquet write throughput; and
- repeat runs under controlled system load.

Extraction is currently the dominant stage, followed by selection and selected
observation writing. Optimize only after the variability is characterized, and
require the corrected semantic golden to remain unchanged.

## Additional vendor support

The adapter/field planner is modular, but a vendor is supported only after a
representative capture proves its AP-name field, timing, QBSS, BSSID, and
band/channel behavior. Candidate future adapters include Arista, Mist, and
Cisco. MAC-pattern AP inference without a hostname remains separate future
work and is not required by the current workflow.

## Optional product work

The following are not required for the current technical-user release:

- installable wheel or packaged console command;
- graphical interface;
- resume/retry UI;
- hosted deployment; and
- consumer-oriented setup automation.

They may be reconsidered if the user base or deployment model expands.

## Known limitations

- Station-count rollups are not yet authoritative summary artifacts.
- APs without a supported hostname IE require a future explicit BSSID mapping.
- Aruba ERM can show periodic controller/capture-path delivery pauses even
  while the active per-BSSID cadence remains about 1.024 seconds.
- No runtime SLA is claimed.
- Support is evidence-based and currently narrower than every theoretically
  decodable capture/vendor combination.

## Historical reproduction

The former multi-command workflow and original golden fingerprint are not
maintained in this repository. Git commit
`4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb` is the recovery point for that
version. `tests/golden/legacy_corrected_comparison_v1.json` retains one small
same-input semantic comparison without executing historical code.
