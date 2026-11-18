# ADR-0019: Observation-time bins and native TSF diagnostics

- Status: Accepted by project owner; implemented
- Date: 2026-11-15
- Supersedes ADR-0008/0009's TSF anchoring, placement, trust and duplicate rules,
  and ADR-0014's TSF-derived native slot placement. Class boundaries and
  aggregation rules stay unchanged.

## Evidence and purpose

An HBLL native capture contained forward jumps in the beacon TSF field while
capture timestamps continued normally. The old whole-session tolerance accepted
the stream. Its minimum observation-minus-TSF origin moved measurements earlier,
excluded 16,195 observations and introduced an artificial gap. Adding ERM caused
the mixed selection to reject TSF, masking the CU/station issue while the
independent native cadence still used the bad clock.

For November 9, 2026, 11:00–12:15 America/Denver, native CU coverage was 63.11%
and native empty-slot fraction 38.47%. A capture-time diagnostic recovered 100%
CU coverage and 1.55% empty slots from the same packets. Empty slots still do
not establish over-the-air packet loss.

## Decision

1. Preferred observation time (valid Aruba timestamp, otherwise frame epoch)
   controls class membership, sequence order, fixed bins and cadence slots.
   Native packets use their capture timestamps. TSF never moves measurements
   or excludes a row for a mapped timestamp outside the class.
2. Diagnose native TSF independently per BSSID/class session. ERM and unknown
   profiles do not supply trusted TSF. Compare successive clock increments
   and the range of their cumulative difference. The tolerance is one
   advertised interval, defaulting to 100 TU. Missing/frozen/reversing values,
   excessive forward steps or inconsistent offsets disable TSF-based dedup.
3. A repeated TSF is a duplicate candidate only in a trusted native stream.
   Require matching retained beacon fields, distinct source files (one view
   per file) and observation timestamps within 102.4 ms. Retain conflicting,
   incomplete or ambiguous evidence rather than infer a duplicate.
4. Keep TSF values and source capture/frame lineage. Persist per-stream jump
   counts, maximum step error and offset range, plus an anomalous frame's
   source/time/TSF in `beacon_cadence_sessions.csv`. Report discontinuities in
   the terminal, structured run log, manifest warning counts and HTML report.

The native timing assessment is reused by cadence even in mixed-family runs.
Checks are linear after the existing observation-time sort; only repeated TSF
candidates need duplicate-content comparisons. No extra capture decode, clock
fitting, segmented origins, interpolation or user configuration is introduced.

## Compatibility and validation

The analysis-results table shape stays v1. Core metric identifiers become
`beacon.metric/corrected-cu/v2`. New selections use `timing_method=observation_time`;
the historical `tsf_origin` and `tsf_derived_time` columns remain null. Old
timing representations can still be read. Capture-quality metric and report
schema identifiers become v2 for the changed timing and additive CSV fields.

Historical v1 fixtures and runs remain evidence of the former behavior, not a
requirement to retain TSF retiming. New regression tests cover forward jumps,
bin edges, source-family independence, conflicting duplicate candidates and
saved anomaly lineage. A fresh analysis is needed to repair affected v1 data;
regenerating HTML alone cannot reconstruct excluded observations.

Validation passed all 533 automated tests: 531 in the full suite and the two
external capture replays rerun with the validated TShark 4.6.6 and retained
Parquet references. Replay of the affected original HBLL native session
retained all 43,868 admitted observations, filled 4,395 of 4,395 CU bins, and
flagged four TSF jumps with zero timing exclusions. Native empty slots were
1.5474%. Existing saved runs were not modified; full-project v2 validation
remains a publication prerequisite.
