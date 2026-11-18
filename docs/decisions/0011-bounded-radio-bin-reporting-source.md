# ADR-0011: Bounded radio/bin reporting source

- Status: Accepted
- Date proposed: 2026-10-03
- Date decided: 2026-10-03
- Owners/reviewers: Project owner, also serving as analysis-correctness reviewer
- Roadmap items: Alpha 0.2 reusable CU radio/bin source gate
- Supersedes: None
- Superseded by: None
- Related decisions:
  [ADR-0003](0003-ap-and-physical-radio-identity-contract.md),
  [ADR-0004](0004-missing-cu-and-missing-beacon-semantics.md),
  [ADR-0005](0005-class-interval-boundary-semantics.md),
  [ADR-0006](0006-output-and-metric-versioning.md), and
  [ADR-0010](0010-station-count-rollup-and-staleness-semantics.md)

## Context

`selected_observations/` retains at most one row per BSSID/class-session/bin.
That is the authoritative beacon evidence and intentionally preserves every
BSSID needed for station-count totals. Corrected CU summaries then select one
CU value per physical radio/bin, but previously retained only class-level
metrics and a 256-value histogram. They did not persist the ordered radio/bin
state needed for detailed radio/class reports or time-series plots.

An absent row in `selected_observations/` also cannot by itself distinguish an
expected radio/bin with no beacon from a radio outside the relevant identity
and session grid. Reconstructing this interpretation independently in each
report would risk divergence from ADR-0004.

## Decision

### Placement and grain

Publish a manifest-registered bounded derived view at:

```text
reports/radio_class_detail/radio_bin_detail.parquet
```

Its grain is one row per:

```text
(class_session_id, classroom, radio_group_id, bin_number)
```

The five existing durable logical artifacts remain authoritative. The view is
generated one physical session at a time and streamed to the report package;
it is not a sixth durable artifact.

### CU source

The project may configure one preferred CU SSID. The supported default is
`eduroam`, configurable with `CU_PRIMARY_SSID` or `--cu-primary-ssid`.

For each radio/class session:

1. If the configured SSID supplies any valid selected CU, its first such BSSID
   is the stable preferred source for the session.
2. Otherwise, the first member BSSID with valid selected CU is preferred.
3. In each bin, use the preferred BSSID when it supplies valid CU.
4. Otherwise, use the earliest valid selected observation from another member
   BSSID on the same radio/bin.
5. Emit only one row and one CU value per physical radio/bin. A fallback SSID
   therefore improves coverage but never duplicates the radio reading.
6. When member beacons exist but none has valid CU, record `missing_cu`. When
   no member beacon exists, record `no_beacon`. Neither state means zero.

The primary-SSID preference adds no additional scan: it is resolved from the
same already ordered per-session selected observations used by the corrected
summary calculation. The CU histogram, class summaries, and radio/bin view use
the same selection pass.

### Columns and lineage

The view retains run/schema/metric IDs; session, classroom, radio and bin keys;
bin timestamps, duration and width status; AP/band/channel identity; CU state
and nullable raw value; selected source SSID/BSSID, selection method and source
observation time; observed-member and valid-candidate counts; and the accepted
radio/bin station total with availability and stale-assistance flags.

`source_bssid` plus run/session/bin identifies the authoritative row in
`selected_observations/`. Capture/frame/TSF fields are not duplicated because
they remain recoverable through that unique join. Percent values, threshold
flags, averages and streaks are also not stored because they are derived from
raw CU under existing versioned semantics.

The station columns are joined from the ADR-0010 radio/bin rollup. They are not
recalculated. Unavailable totals remain null and measured zero remains zero.

### Optional plots

Radio/class-session CU and station-count plots are disabled by default. They
may be enabled with `--radio-class-plots` or `RADIO_CLASS_PLOTS=true`; the
resolved setting is recorded in the manifest. Enabling the option intentionally
creates one plot per radio/class session plus a CSV index.

Measured CU uses solid line segments without per-bin point markers and a
lighter solid fill beneath measured values. Unknown CU bins remain blank. When
a gap has a valid observation on both sides, a dashed blue line connects those
values as a visual best-guess path only; it is not stored, included in
calculations, or presented as measured CU. Station count uses a secondary
orange 0-100 axis unless a larger observed value requires expansion, with
stale-assisted values visibly marked.
Unavailable station-count gaps bounded by available measured or stale-assisted
counts receive a dashed orange visual bridge. This guide does not replace
nulls or enter metric calculations. Horizontal padding is 2.5% of the data span.

Plot titles and axes convert canonical UTC instants to the explicitly
configured named schedule timezone. Titles show AP, radio, local date, and
local class interval; x-axis ticks
show local 24-hour time. A named IANA zone
applies the correct standard/daylight offset for each plotted date regardless
of the machine timezone or when the analysis runs.
The upper-left text box shows building-room, schedule subject, seats, combined
enrollment, configured distinct room AP count, AP model, and radio band.
Missing or conflicting metadata is labeled Unknown. Schedule/AP context is
looked up once for plotting rather than duplicated in every radio/bin row.

## Consequences

- Detailed reports reuse one reviewed radio/bin interpretation.
- BSSID-grain source evidence and station-count completeness remain intact.
- The default run adds one bounded Parquet report but no per-radio plot flood.
- Opt-in plotting can create many files and additional runtime by explicit
  user choice.
- Existing immutable runs remain valid and are not rewritten.

## Addendum — 2026-10-08

[ADR-0013](0013-radio-admission-capacity-reporting.md) extends this bounded view
with separately versioned admission-capacity fields and lineage. The report
schema becomes `beacon.schema/radio-bin-detail/v2` in Parquet metadata and the
manifest; the CU selection, CU metric version, station totals, and opt-in
radio/session plots are unchanged. Capacity reporting is placed separately in
`reports/admission_capacity/`. CU describes channel conditions and admission
capacity describes the physical radio's QoS admission allowance.

## Required verification

- Exactly one derived row exists per expected radio/bin.
- Multiple member BSSIDs never duplicate CU values.
- Configured-primary selection and same-radio fallback are deterministic.
- `missing_cu`, `no_beacon`, and measured zero remain distinct.
- Station values match ADR-0010 radio/bin rows, including null and stale flags.
- Default runs create no radio/class plots.
- Opt-in runs create an indexed plot collection whose unknown-CU treatment is
  documented as non-imputing.
