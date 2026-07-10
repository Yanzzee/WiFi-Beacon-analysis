# Post-Refactor Design Backlog

This document tracks analysis and architecture changes intentionally deferred until after the current notebooks are converted into testable Python modules. It is not a description of current behavior and does not authorize changing analysis math without a separate reviewed implementation decision.

`docs/current_pipeline.md` remains the source for how the pipeline works today. `step5_bug_backlog.md` remains the source for pre-refactor bugs, fragilities, and behavior risks.

## Analysis Semantics

### Per-radio and classroom high-CU streaks

- Establish a stable physical-radio identity. Under the current Aruba path this requires at least AP identity plus inferred radio identity; `radio_number` alone is not globally unique.
- Collapse multiple SSID/BSSID observations from the same physical radio into one documented radio/bin state so duplicate advertisements do not change streak semantics.
- Calculate streaks independently for each radio before producing a classroom result.
- Make each classroom streak the longest streak produced by any one radio in that location and retain the responsible radio identity in the output for traceability.
- Keep channel-level analysis separate from physical-radio analysis. Multiple AP radios may share a channel, so grouping by channel answers a different question.

### Missing-data streak bounds

- Preserve distinct states for observed high CU, observed non-high CU, a received beacon with missing CU, and an expected interval with no beacon.
- Produce a lower/minimum streak measure that treats missing expected intervals as non-high and therefore breaks continuity.
- Produce an upper/maximum streak measure with a precisely documented high-adjacent rule. Decide whether high adjacency means a preceding high observation, high observations on both sides of a bounded gap, or another constrained interpolation rule.
- Define limits for upper-bound imputation so long capture outages, leading gaps, or trailing gaps cannot silently become long high-CU streaks.
- Report coverage with the bounds so an upper estimate based on sparse data is visibly different from a well-observed streak.

### Beacon-rate and coverage analysis

- Measure expected and observed metadata-bearing beacon rates per stable radio and, where needed, per BSSID/SSID.
- Report missing-bin count, coverage percentage, gap-length distribution, longest gap, and leading/trailing coverage gaps for every class/radio series.
- Separate the raw over-the-air beacon interval from the effective QBSS metadata update interval. Allow the effective rate to be validated from observed data rather than assumed globally for every vendor and capture source.
- Account for bin alignment and timestamp jitter so an empty analysis bin is not automatically labeled a lost beacon without supporting evidence.

### Missing QBSS values

- Preserve nullable `wlan.qbss.cu`, `wlan.qbss.scount`, and `wlan.qbss.adc` values instead of silently converting missing values to measured zero.
- Add explicit provenance or quality fields when a downstream algorithm imputes a value.
- Define metric-specific handling for missing CU rather than applying one global rule to averages, thresholds, streaks, and coverage.
- Include missing-value counts in outputs and retain enough identifiers to trace summaries back to source rows.

### Station-count semantics

- Review the current nearest-observation room station-count calculation and its plus/minus five-second lookup window.
- Define whether station counts should be aligned per radio, AP, channel, or room and how stale observations and missing transmitters affect totals.
- Prevent multiple SSIDs/BSSIDs from double-counting the same physical radio or associated station population.

## Capture and Vendor Abstraction

### Multi-vendor normalization

- Introduce a canonical internal schema for timestamp, AP identity, physical-radio identity, BSSID, SSID, channel/frequency, CU, station count, and admission capacity.
- Implement vendor adapters for Aruba, Mist, Extreme, Cisco, Meraki, and vendor-neutral/native 802.11 captures as evidence and fixtures become available.
- Replace Aruba AP-name dependence and MAC-gap radio inference with explicit metadata where available and documented fallback strategies where it is not.
- Preserve the raw vendor fields and record which adapter or fallback produced each normalized identity.

### Unified grouping and cleanup

- Combine vendor detection, AP/BSSID grouping, schema normalization, and index cleanup into one deterministic stage.
- Make native and Aruba ERM capture outputs enter the same downstream contract after normalization.
- Define an explicit policy for captures that lack reliable AP or radio identity rather than silently grouping them as if those identities were known.

## Time and Interval Model

### Timezone standardization

- Define one canonical internal timestamp representation, preferably timezone-aware UTC, for capture data and class schedules.
- Convert to a named IANA display timezone only at reporting and plotting boundaries.
- Normalize strings, naive datetimes, fixed-offset datetimes, and daylight-saving transitions through one tested path.
- Replace the current split use of `TIMEZONE` and `PLOT_TIMEZONE` with an explicit storage, comparison, and display contract.
- Add fixtures covering DST boundaries, multiple input formats, and schedule/capture inclusion at exact class boundaries.

### Interval boundaries

- Define whether class end times are inclusive or exclusive and how trailing partial beacon windows contribute to metrics.
- Define bin anchoring independently from incidental class start offsets when comparing expected beacon cadence.
- Use one interval implementation across per-radio summaries and class-session summaries so missing gaps have consistent semantics.

## Outputs, Configuration, and Validation

### Traceable summary outputs

- Store stable AP/radio identifiers, the source time range, threshold, missing-data assumption, coverage measures, and the source dataset/version with every summary row.
- Preserve enough row identifiers or lineage metadata to reproduce a reported maximum streak from source observations.
- Version output schemas and metric definitions when analysis semantics change.

### Configurable analysis definitions

- Move CU thresholds, beacon-rate assumptions, gap-imputation limits, and station-count lookup tolerances into validated configuration.
- Record effective configuration values in generated reports and regression fingerprints.
- Fail clearly when required metadata for a selected analysis mode is unavailable, such as channel analysis without channel/frequency data.

### Focused regression fixtures

- Add small fixtures for multiple APs and radios, multiple SSIDs per radio, alternating high radios, missing beacons, missing CU, measured zero CU, long capture gaps, and timezone boundaries.
- Test lower and upper streak contracts independently, then test that the classroom result is the maximum of single-radio results.
- Keep golden fingerprints for stable tabular outputs while avoiding PNG byte comparisons.
