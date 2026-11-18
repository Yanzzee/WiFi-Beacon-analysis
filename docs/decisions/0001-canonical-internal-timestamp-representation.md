# ADR-0001: Canonical internal timestamp representation

- Status: Accepted
- Decision: Accepted within the timestamp-representation scope below
- Date proposed: 2026-08-03
- Date decided: 2026-08-03
- Owners/reviewers: Project owner
- Roadmap items: [PR-0019, PR-0020, and the timestamp-contract and
  DST-fixture portions of PR-0023](../post_refactor_roadmap.md)
- Related decisions: [ADR-0002](0002-canonical-beacon-schema.md),
  [ADR-0005](0005-class-interval-boundary-semantics.md), and
  [ADR-0006](0006-output-and-metric-versioning.md)
- Supersedes: None
- Superseded by: None

> Current implementation status (2026-10-03): this decision is implemented
> by the supported `beacon_analysis.py run` workflow. References below to
> migration, later activation, or removed scripts describe the evidence and
> alternatives available when the decision was made, not a second supported
> runtime. The 2026-10-03 project-timezone amendment records the subsequent
> owner-approved move from a fixed UTC-07 offset to `America/Denver`.

## Scope and decision state

This ADR accepts the canonical timestamp storage and normalization contract
within the scope below. Acceptance records architecture policy only; it does
not implement the policy or change current analysis behavior.

The accepted contract is limited to representing and normalizing unambiguous
instants. It deliberately does not decide:

- whether a class end is inclusive or exclusive;
- bin anchoring, expected cadence, or timestamp-jitter assignment;
- empty expected intervals or missing-beacon semantics;
- trailing partial bins; or
- any metric formula, grouping rule, threshold, or class-boundary behavior.

Those interval questions belong to ADR-0005, PR-0021, and PR-0022. Missing-CU
and missing-beacon interpretation belongs to ADR-0004 and the relevant P07
roadmap items. This separation is required because changing a timestamp's
representation without changing its instant can be behavior-preserving, while
changing interval membership or bin assignment is an intentional analysis
change.

## Context and current behavior

[`current_pipeline.md`](../current_pipeline.md) remains the source of truth for
runtime behavior. The active pipeline has no single storage, comparison, and
display timezone contract. Its behavior depends on the source field, pandas
dtype, whether grouped cleanup ran, and which configuration value a stage
uses.

The two data-bearing timestamp sources are:

1. capture timestamps exported by tshark as `aruba_erm.time` for Aruba ERM or
   `frame.time_epoch` for native conversion; and
2. class-session endpoints synthesized from the configured date range,
   schedule `Days`, and schedule `Class Period` values.

Operational log timestamps also exist. They use machine-local
`datetime.now()` for log filenames and start/completion messages. They do not
participate in beacon filtering or metrics and are outside this canonical
observation-time decision. A future run-manifest contract should separately
standardize them.

### Evidence labels

The inventory below distinguishes four evidence strengths:

- **Tracked fixture:** directly observed in the committed analysis-forward
  Parquet fixture or its T3 golden fingerprint.
- **Focused regression:** established by a small committed test with synthetic
  data.
- **Retained original-input checkpoint:** observed in local T4/T5/T6 inputs or
  outputs whose role and prior validation are documented, but which are not a
  portable tracked fixture.
- **Code/documentation only:** active code or current documentation states the
  behavior, but no representative committed fixture proves the source shape.

### Stage-by-stage timestamp inventory

| Stage and source | Input representation and awareness | Active transformation | Output representation | Failure or ambiguity behavior | Evidence |
|---|---|---|---|---|---|
| Aruba capture export | tshark field `aruba_erm.time`; CSV text; source may contain an explicit offset | `process_pcapng.py` selects it as the pandas index and calls `read_csv(parse_dates=True, date_format="%b %e, %Y %H:%M:%S.%f %Z")` | Parsed datetime index when the token matches; otherwise an object/string index can survive to Parquet | The read fails only on a raised CSV/parser exception. A nonmatching timestamp token can remain an object value rather than fail | Code plus retained T4 checkpoint. The retained three-capture Parquets store ISO-like offset-bearing strings such as `2025-11-10T09:00:00.212540000-0700` in a string `aruba_erm.time` index |
| Native capture export | tshark field `frame.time_epoch`; decimal Unix-epoch seconds in the documented tshark field contract | `process_pcapng.py` selects it as the index and uses `parse_dates=True` without an explicit unit or format | pandas decides the dtype; the pipeline does not explicitly convert seconds with `unit="s"` and `utc=True` | Numeric epoch text may remain numeric rather than become a timestamp. Downstream Aruba analysis rejects this path | Code and focused rejection test only; there is no representative native downstream fixture |
| Aruba AP-name grouping | Processed Parquet with `aruba_erm.time` normally as an index, potentially still strings | Dask filters rows by AP name and writes with pandas/PyArrow; no timestamp parsing or timezone conversion is requested | Timestamp placement and dtype are carried through grouping | No timestamp-specific validation; schema problems can surface through Dask/PyArrow writes | Code; short grouping checkpoints compare known rows, but the committed fixture begins after cleanup |
| BSSID grouping | Processed Parquet with the source timestamp index | PyArrow writes each group with `preserve_index=True`; no timestamp conversion | Source timestamp remains an index in the written schema | No timestamp-specific validation; schema drift may be cast to the first writer schema | Code and grouping tests; no native timestamp fixture |
| Grouped cleanup: index placement | Grouped Parquet with index named `aruba_erm.time` or a column of that name | A named timestamp index is reset to a column | `aruba_erm.time` becomes a regular column; the output is written with `index=False` | A file without the column is skipped unchanged | Focused regression proves index reset and unchanged skip behavior |
| Grouped cleanup: strings or other non-datetime values | Column is not a pandas datetime dtype; awareness is encoded, if at all, in each token | `pd.to_datetime(errors="coerce", utc=True)`, then `tz_convert(PLOT_TIMEZONE)` | Timezone-aware datetime column in `PLOT_TIMEZONE` | Invalid tokens silently become `NaT`; the file can still report `[ok]`. Mixed explicit offsets are collapsed to UTC before display-zone conversion | Focused regression proves an explicit-UTC string converts to Denver time and an invalid string becomes `NaT`; retained T4 Aruba strings contain explicit `-0700` offsets |
| Grouped cleanup: naive datetime dtype | `datetime64` column or index with no timezone | `tz_localize(PLOT_TIMEZONE, ambiguous="NaT", nonexistent="NaT")` | Aware datetime column labeled with `PLOT_TIMEZONE` | Repeated and nonexistent local values become `NaT` without source evidence or a row-level diagnostic | Focused regression proves ordinary naive localization; no DST fixture covers the gap/fold behavior |
| Grouped cleanup: aware datetime dtype | Aware pandas datetime series in any supported timezone | `tz_convert(PLOT_TIMEZONE)` | Same instants represented in `PLOT_TIMEZONE` | Conversion errors are caught per file and returned as `[err]`; the source zone/offset token is not retained separately | Focused regression proves UTC-to-`America/Denver` conversion |
| Schedule date range | `START_DATE` and `END_DATE` strings | Strict `datetime.strptime(..., "%Y-%m-%d")`; an inclusive list of dates is built | Naive Python datetimes at midnight used only to generate candidate dates | Missing or malformed configuration fails. Per-row schedule `Begin Date` and `End Date` are cleaned and cast to strings but are not used to limit expansion | Code, documentation, and focused inclusive-range regression |
| Schedule weekday and period | `Days` codes and `Class Period` text such as `8:00a - 9:50a` | Period endpoints are parsed with `%I:%M%p`; each matching global date is combined with each endpoint | Naive Python datetimes; start and end are placed on the same generated date | A malformed period prints a message and skips that schedule row. Unsupported day codes produce no sessions. Overnight periods receive no special treatment | Focused regressions prove accepted formats, rejected formats, skip behavior, and row/weekday order |
| Schedule parsing and localization | Naive generated `start_time` and `end_time`, or an already-aware series if supplied programmatically | `pd.to_datetime(errors="coerce")`; naive series use `tz_localize(TIMEZONE, ambiguous="NaT")`; aware series use `tz_convert(TIMEZONE)` | Aware schedule columns in `TIMEZONE` | Invalid values can become `NaT`. Repeated local times become `NaT`. `nonexistent` is not specified, so pandas' default raises for a spring gap. No row-level timezone provenance is stored | Code and focused ordinary-time regression; no DST fixture |
| Schedule Parquet | Aware `start_time` and `end_time` columns | pandas `to_parquet` | Arrow `timestamp[ns, tz=Etc/GMT+7]` in the tracked fixture | Parquet preserves the configured zone label, but not the original schedule token or localization method | Tracked fixture: 61 sessions; focused round-trip regression proves the same dtype and wall times |
| Grouped analysis validation | `aruba_erm.time` may be an index or column | Checks for at least one non-null value, pandas datetime dtype, and a non-null timezone | Input is accepted unchanged or analysis stops | A naive dtype is rejected. A string dtype is rejected even if parseable; `pd.to_datetime(..., utc=True)` is used only to count invalid values for the diagnostic. `frame.time_epoch` is explicitly reported as conversion-only | Focused regression covers aware acceptance, naive rejection, string rejection, and native rejection; tracked fixture satisfies the aware invariant |
| Primary class filter | Validated grouped `aruba_erm.time`, usually a named index; schedule endpoints are aware in `TIMEZONE` | Resets the index, contains a defensive naive-to-UTC branch, converts beacon timestamps to `TIMEZONE`, then applies `timestamp >= start_time` and `timestamp <= end_time` | In-scope beacon rows have `aruba_erm.time` as an aware column in `TIMEZONE` | The naive-to-UTC branch is unreachable through the supported validated path. Empty selections are logged and skipped. Both class endpoints are currently inclusive | Code and focused boundary tests; T3 protects current selected outputs. Inclusivity remains an ADR-0005 decision, not an ADR-0001 decision |
| Deduplication and radio analysis | Aware in-scope beacon column | Sorts by timestamp; compares elapsed differences against `0.95 * BEACON_RATE`; later streak duration subtracts timestamps | Same timestamp values and timezone; order changes only where the current explicit sorts already require it | Empty input to the low-level deduplicator is not independently guarded; timestamp type failures surface from pandas arithmetic | Focused tests pin sort order and exact dedup boundary; T3 pins aggregate results |
| Sampled and combined beacon Parquet | Filtered timestamps represented in `TIMEZONE` | pandas/PyArrow writes and later concatenates tables | Current generated `all_class_beacons.parquet` stores `timestamp[ns, tz=Etc/GMT+7]` | Appending requires compatible Arrow schemas. Original timestamp tokens and source-zone evidence are absent | Retained generated output matches the T3 row count; T3 fingerprints timestamp instants after converting them to UTC |
| Radio-summary Parquet | Schedule `start_time` and `end_time` values | Values are copied into summary rows; duration is computed by endpoint subtraction in the later radio-summary stage | Current generated summary stores both fields as `timestamp[ns, tz=Etc/GMT+7]` | No additional timezone validation at this stage | Retained generated output and T3 fingerprint; focused tests cover duration calculations but not zone mixing |
| Room and class-session analysis | Aware beacon and schedule values | Room lookup converts aware timestamps to signed nanoseconds for `searchsorted`. Class analysis uses current inclusive filtering and `pd.date_range` bins in the schedule timezone | Beacon timestamps remain aware columns; no class-session table is persisted | Incompatible/naive types fail through pandas operations. Current bin and empty-bin behavior is intentionally unchanged and delegated to ADR-0005/ADR-0004 | Focused tests cover exact class boundaries and current bin behavior; T3 protects current metrics |
| Time-series plotting | Beacon x-values are already in `TIMEZONE`; `start_time` and `end_time` are schedule values in `TIMEZONE` | Matplotlib receives the aware beacon values. Only the x-axis formatter is given `PLOT_TIMEZONE` | Tick labels display in `PLOT_TIMEZONE`; title text and output filename use `start_time.strftime(...)` in `TIMEZONE` | Invalid `PLOT_TIMEZONE` fails through `pytz.timezone`. A different `TIMEZONE` can make title/filename wall time disagree with tick labels | Focused plotting regression proves formatter timezone, title, filename, and source x-values. PNG bytes are deliberately not a regression oracle |
| Golden fingerprinting | Any known timestamp column in an output DataFrame | Test harness calls `pd.to_datetime(errors="coerce", utc=True)` before min/max reporting and row hashing | UTC ISO-8601 min/max values and timezone-neutral instant hashes | Invalid values are dropped from min/max. Row hashes are sorted, so T3 does not prove row order. Dtypes and Arrow timezone metadata are not fingerprinted | Committed harness and T3 golden fingerprint |
| Operational logs | Machine-local wall clock from `datetime.now()` | Formats minute-resolution filenames and second-resolution log messages without timezone/offset | Unzoned strings | Machine timezone is implicit; these values are not analysis data | Code only; outside the decision scope |

### Fixture-proven Parquet representations

The committed fixture and retained checkpoints demonstrate that current
storage is stage-dependent:

| Artifact boundary | Observed placement and Arrow type | Evidence limit |
|---|---|---|
| Retained three-capture processed checkpoint | `aruba_erm.time` is a string-backed pandas index with explicit `-0700` tokens | Original-input evidence is local, not committed; no native equivalent exists |
| Tracked grouped fixture | `aruba_erm.time` is a regular `timestamp[ns, tz=America/Denver]` column with no pandas index metadata | Proves the current Aruba grouped/analysis-forward path for November 10-14, 2025 only |
| Tracked sample classes | `start_time` and `end_time` are regular `timestamp[ns, tz=Etc/GMT+7]` columns | The fixed offset equals Denver's observed offset for the tracked November dates; it does not exercise a DST transition |
| Retained generated `all_class_beacons` and `radio_summary` | Beacon and schedule timestamps are `timestamp[ns, tz=Etc/GMT+7]` | These are generated outputs and not a new canonical schema contract |

The tracked grouped files contain 8,403,745 rows. Their first observed values
carry nanosecond Arrow types and a Denver zone label. The tracked schedules
contain 61 rows and a fixed-offset `Etc/GMT+7` label. The T3 golden converts
timestamp values to UTC before fingerprinting, so it protects instant values
but not timezone labels or Parquet bytes.

### Consequences of current behavior

1. `PLOT_TIMEZONE` currently affects persisted grouped data, so its name is
   broader than its actual role.
2. The same naive wall value is treated as Denver time by cleanup but the
   defensive analysis branch would treat it as UTC. Supported analysis now
   rejects the value before that branch, but the conflicting code remains.
3. A string containing an explicit offset is interpreted as an instant, while
   the same characters already stored as a naive datetime dtype are interpreted
   in `PLOT_TIMEZONE`. Dtype therefore changes meaning.
4. Current Aruba processing discards the original source timestamp token and
   source offset after cleanup.
5. Fixed-offset `TIMEZONE=Etc/GMT+7` and named
   `PLOT_TIMEZONE=America/Denver` agree for the tracked November fixture but
   can disagree when Denver observes a different offset.
6. Arrow/Parquet round-trips the current timezone-aware pandas columns, but a
   field-level timezone label is not a substitute for per-row source timezone,
   offset, raw-token, or normalization evidence.

## Decision drivers

1. An unambiguous instant must compare, sort, filter, and round-trip the same
   way regardless of source or display timezone.
2. The representation must work predictably in pandas 2.2 and Arrow/Parquet,
   the current implementation boundary.
3. DST gaps and folds must not be repaired silently.
4. Multi-location and future multi-vendor data must coexist without converting
   timestamp columns to object dtype or relying on one machine timezone.
5. Source timezone evidence and a report's display timezone are distinct from
   canonical storage semantics.
6. Current aware Aruba rows must preserve their instants, order, class
   inclusion, deduplication, metrics, and plot meaning.
7. Normalization decisions must be auditable under ADR-0002's value-state and
   transformation model.
8. Timestamp precision must not be reduced below the current nanosecond Arrow
   representation.

## Timestamp models considered

### Comparison

| Model | Correctness and pandas behavior | Arrow/Parquet interoperability | DST and multiple locations | Readability and provenance | Compatibility assessment |
|---|---|---|---|---|---|
| **A. Timezone-aware UTC instant** | One ordered timeline; pandas comparison, subtraction, sorting, and `datetime64[ns, UTC]` operations are direct and vectorized | Native Arrow `timestamp[ns, tz=UTC]`; stable filter/predicate semantics; avoids mixed-zone object columns | DST ambiguity must be resolved before conversion. Once resolved, every instant is unique and locations can share one column | UTC is readable but not the source wall context; raw token, source offset/zone, and display zone must be separate evidence | Current aware Aruba and schedule values can convert without changing epoch nanoseconds. Best fit for canonical internal use |
| **B. Timezone-aware named local timestamp** | Correct for one zone and retains useful wall context, but concatenating different local zones can yield mixed/object behavior or force normalization during queries | Arrow carries one timezone label per timestamp field; a single column cannot honestly use different named zones as its field metadata | A named IANA zone expresses DST rules, but ambiguous naive inputs still require fold/offset evidence. Multi-location tables need partitioning or an additional conversion rule | Most human-readable for one location; may falsely appear to retain the original source zone after conversion | Matches the tracked grouped fixture but makes `PLOT_TIMEZONE` look like storage semantics and scales poorly to multi-location data |
| **C. Integer Unix epoch plus timezone metadata** | Exact and sortable only if unit, range, signedness, and null contract are never lost; pandas does not enforce that an integer column is time | Very portable as `int64`, but weaker logical typing and predicate tooling; unit mistakes can be silent | Epoch values avoid DST after parsing. Separate zone metadata is still required for schedule interpretation and display | Least readable; provenance can be explicit, but the integer alone cannot show whether seconds, milliseconds, microseconds, or nanoseconds were intended | `frame.time_epoch` points in this direction, but current code's implicit pandas parsing demonstrates the unit/type risk |
| **D. Offset-bearing ISO-8601 strings** | Can preserve source text and parse varied offsets, but repeated parsing is slower; lexical sort is not chronological across differing offsets unless normalized | Interoperable as strings but loses timestamp logical typing, timezone-aware predicates, and efficient time statistics | Explicit offsets identify instants, but named-zone/DST-rule context can still be absent. Mixed offsets remain strings until parsed | Excellent raw evidence and diagnostics; unsuitable as the only normalized value | Matches retained processed Aruba evidence and is valuable in the raw layer, but current analysis correctly rejects strings as its final input dtype |

### Selected model

Model A is selected for the canonical instant. Model D or an equivalent
lossless value is retained as source evidence where the source supplied text.

The accepted logical contract is:

- every valid canonical observation timestamp and normalized schedule endpoint
  is a timezone-aware UTC instant;
- pandas represents canonical timestamp columns as `datetime64[ns, UTC]`;
- Arrow/Parquet represents them as `timestamp[ns, tz=UTC]`;
- nanosecond precision is retained unless original-input evidence proves a
  wider requirement that Arrow/Pandas cannot represent safely;
- source timezone, source offset, schedule-interpretation timezone,
  dataset/location timezone, and display timezone remain separate metadata or
  provenance concepts; and
- conversion to a report/display timezone occurs only at a reporting,
  labeling, filename, or plotting boundary.

UTC was selected because it provides one typed comparison domain. The
selection does not make source or local timezone evidence unimportant. A UTC
value alone is insufficient provenance.

## Decision

Accepted within this timestamp-representation scope:

1. **Canonical internal instant.** Every valid canonical observation timestamp
   and normalized schedule endpoint is a timezone-aware UTC instant. pandas
   uses `datetime64[ns, UTC]`; Arrow/Parquet uses
   `timestamp[ns, tz=UTC]`.
2. **Separate roles.** Source timestamp evidence, canonical storage time,
   schedule interpretation timezone, dataset/location timezone, and display
   timezone are distinct. Display timezone never determines storage or
   comparison semantics.
3. **Source evidence and provenance.** Preserve or reference, where available,
   the original token, source offset or named-zone evidence, evidence source,
   normalization method/version, and invalid or ambiguous diagnostic state.
   Exact physical placement remains deferred.
4. **Naive timestamps.** Normalize a naive value only with explicit trusted
   evidence, in this order: explicit source/record metadata; accepted
   adapter-provided evidence; then an explicitly configured dataset or
   schedule timezone. Conflicting evidence fails. The executing computer's
   timezone is never implicit evidence.
5. **DST.** Reject nonexistent local times unless source evidence identifies a
   real instant. Reject repeated/ambiguous local times unless an explicit
   offset, fold, or equivalent trusted evidence resolves the instant. Never
   silently select the first or second occurrence.
6. **Fixed offsets and named zones.** An explicit fixed-offset timestamp is a
   valid instant and may normalize to UTC. Preserve whether evidence was a
   fixed offset or named zone. Do not silently replace a configured fixed
   offset with a daylight-saving-aware zone.
7. **Schedule interpretation.** Schedule dates and local times require an
   explicitly configured or dataset-defined named timezone. Existing
   configured schedule behavior remains available during behavior-preserving
   rollout. The project owner subsequently approved `America/Denver` for this
   project's schedule timezone on 2026-10-03 after compatibility evidence
   showed that it preserves every represented instant in the November 2025
   sample while supplying the correct daylight-saving rules for other dates.
8. **Display behavior.** Convert only at reporting, labeling, filename, and
   plotting boundaries, and make the effective display timezone identifiable.
   Initial implementation must preserve current plot titles, labels,
   filenames, and included rows.
9. **Current Aruba compatibility.** Preserve the represented instants of all
   current aware Aruba observations. Initial normalization changes no row
   inclusion, significant row ordering, deduplication, class filtering,
   aggregation, plot, filename, published timestamp value, or T3 fingerprint.
10. **Native epoch timestamps.** This ADR does not authorize downstream
    `frame.time_epoch` support. Its grammar, epoch unit, extraction behavior,
    precision, and round trip require original-input evidence.
11. **Deferred interval semantics.** ADR-0005, ADR-0004, and their roadmap
    items retain class-boundary inclusion, bin anchoring, cadence, jitter,
    expected missing intervals, trailing partial bins, and missing-beacon
    behavior.

## Source, storage, schedule, dataset, and display timezone roles

| Role | Accepted contract | Current mapping and migration concern |
|---|---|---|
| Source timezone or offset | Evidence attached to the source token, decoded field, capture metadata, adapter, or file manifest. Preserve both a named zone and observed offset when both exist | Current Aruba tokens can carry `-0700`; cleanup later replaces that presentation with `America/Denver` and does not retain the token separately |
| Canonical storage timezone | UTC only for valid normalized instants | The corrected supported path normalizes observations and schedules to UTC. Retained Original-release and tracked fixture files can still carry their legacy timezone labels |
| Schedule interpretation timezone | The named IANA zone whose civil rules apply to naive schedule dates/times, selected from explicit trusted evidence | The project configuration now uses `TIMEZONE=America/Denver`; localization is evaluated for each schedule date, then converted to canonical UTC |
| Dataset/location timezone | Named IANA metadata for the physical location or dataset, potentially different by location | The global `TIMEZONE` setting is the explicit project/location evidence. A future multi-location dataset will require narrower-scoped metadata |
| Report and plot display timezone | Explicit report configuration or location choice applied after canonical filtering and calculation | Corrected reports convert canonical timestamps back to the resolved schedule timezone. The radio/bin manifest records the effective display timezone |

Display timezone is presentation policy, not storage semantics. A report can be
rendered in Denver time, UTC, or another requested zone without rewriting the
canonical instant or changing row inclusion.

## Naive timestamp policy

### Policies considered

| Policy | Benefit | Risk | Assessment |
|---|---|---|---|
| Always reject | Simple and safest for capture data | Would reject intrinsically naive civil schedule inputs even when an authoritative location zone is configured | Too strict as a universal rule; appropriate when no trusted timezone evidence exists |
| Require an explicitly configured named timezone | Deterministic and testable | One global setting may be wrong for a multi-location dataset; configuration can conflict with source evidence | Viable for schedules if scope and provenance are explicit |
| Allow adapter-supplied evidence | Lets a vendor/source adapter use capture semantics that are not encoded in the value | Adapter claims can be wrong or version-dependent | Viable only with named adapter/version and retained evidence |
| Allow file/dataset metadata | Scales to sources whose timestamps inherit a documented file or deployment zone | Metadata may be missing, too broad, or conflict with row values | Viable with scope, authority, and conflict checks |
| Controlled hierarchy | Supports schedules and varied sources without guessing | Requires explicit precedence, diagnostics, and provenance | Selected |

### Accepted controlled hierarchy

For a naive timestamp, use the first applicable trusted evidence below. If
multiple applicable sources disagree, fail instead of choosing by precedence:

1. explicit source/record metadata, including an offset, fold, named zone, or
   file/capture metadata tied to that record;
2. accepted adapter-provided timezone semantics tied to a named adapter
   version and source field; and
3. an explicitly configured dataset or schedule timezone whose scope is tied
   deterministically to the value.

If none applies, reject the value for canonical timestamp-dependent
processing. Never attach the machine timezone, infer a zone from the current
date, reuse `PLOT_TIMEZONE` merely because it exists, or silently attach a
fixed offset.

An explicit source fixed offset is valid evidence for an instant. It is not
equivalent to a named IANA zone and must not be used to infer past or future
DST rules. An epoch field with a documented unit and epoch, such as a validated
`frame.time_epoch` adapter mapping, already expresses an instant and must be
parsed explicitly by unit rather than treated as a naive local value. This
principle does not authorize downstream native support without the required
original-input evidence.

### Provenance for localization

Every accepted localization must be auditable through these conceptual
facts, with physical placement deferred:

- raw source field name and original token/value;
- original precision and epoch unit when applicable;
- observed source offset and named source zone, if present;
- timezone-evidence kind, value, scope, and source reference;
- adapter or schedule-normalizer name and version;
- normalization method and contract version;
- canonical UTC result;
- diagnostic/status state; and
- explicit fold/offset evidence when a repeated local time is resolved.

Configuration values alone are not sufficient provenance unless the effective
configuration and its applicable dataset/location scope are retained.

## DST and offset behavior

### Accepted rules

- A nonexistent local time in a spring transition fails normalization. Do not
  shift it forward/backward or convert it to `NaT` without a diagnostic.
- A repeated local time in a fall transition requires an explicit UTC offset,
  fold indicator, or equivalent source evidence. A named zone plus wall value
  alone is insufficient. Do not silently select the first or second
  occurrence.
- Conflicting named-zone and explicit-offset evidence fails normalization.
- An explicit fixed-offset timestamp is an unambiguous instant and may be
  converted to UTC, while retaining the fixed offset as source evidence.
- A named IANA timezone supplies civil transition rules, but does not by itself
  resolve a repeated wall value.
- A schedule interval that crosses a DST transition must normalize each
  endpoint independently using the same evidence rules and retain the original
  civil endpoints and zone. Whether reports use elapsed or civil duration, and
  how the interval is binned, remains an ADR-0005 metric/interval decision.

### Diagnostic outcome

At minimum, normalization must distinguish:

- valid unambiguous instant;
- invalid syntax or out-of-range value;
- missing timezone evidence;
- conflicting timezone evidence;
- nonexistent local time; and
- ambiguous repeated local time.

Invalid or ambiguous rows must not enter timestamp-dependent canonical
analysis as if they were valid observations. Whether they remain in a raw
table, a quarantine/diagnostic table, or a nullable canonical staging artifact
is a physical placement decision. They must not be silently dropped, repaired,
or converted to an undifferentiated `NaT`.

## Raw timestamp preservation

The conceptual contract must retain or reference:

1. the parsed canonical UTC timestamp for every valid normalized value;
2. the original source timestamp token or lossless source value;
3. original source offset and named timezone evidence, when available;
4. the source field and source-record/capture reference;
5. normalization method and version; and
6. a diagnostic state for invalid, ambiguous, nonexistent, conflicting, or
   evidence-missing values.

ADR-0002's hybrid-provenance direction permits row-specific facts inline and
repeated adapter/capture/configuration facts in a referenced manifest. This ADR
does not choose exact field names, nullable layouts, sidecar tables, or manifest
schemas. Legacy grouped data cannot recover discarded source tokens or capture
lineage and must report that evidence as unavailable rather than invent it.

## Compatibility and behavior preservation

### Why current aware Aruba data can be normalized without changing results

For an aware timestamp `t`, `t.tz_convert("UTC")` preserves the underlying
instant and epoch nanoseconds. Applied element by element without reindexing,
that transformation preserves:

- input row order;
- total ordering and equality of instants;
- elapsed differences used by deduplication;
- current inclusive class membership when schedule endpoints are converted to
  the same canonical domain;
- grouping keys and non-time values;
- aggregation results and metric formulas; and
- all current threshold calculations.

The tracked November schedule and grouped beacon values both represent an
observed UTC-07:00 offset. Converting both to UTC therefore preserves their
relative and boundary relationships. This observation does not prove behavior
for DST transitions or authorize interpreting naive/ambiguous values.

### Plot compatibility conditions

Plot compatibility requires more than UTC storage:

- x-axis values must be converted or formatted with the selected display zone;
- title and filename timestamps must also be converted to their explicitly
  selected presentation zone before `strftime`;
- current `PLOT_TIMEZONE=America/Denver` labels must remain unchanged for the
  tracked fixture; and
- plot series, point order, filenames, titles, and tick-label semantics must
  be asserted without comparing PNG bytes.

If canonical UTC schedule endpoints were passed directly to the current title
and filename formatting, wall-clock text and filenames would shift. That is a
compatibility risk to prevent, not an intended consequence of UTC storage.

### Legacy fixture and T3 conditions

The tracked grouped and schedule fixture Parquets must not be rewritten for
PR-0019 or this ADR. A future adapter can normalize them in memory or write a
separate canonical artifact while the test-only legacy profile remains
unchanged under ADR-0006.

T3 must retain its current fingerprint exactly. T3 already converts timestamp
values to UTC for min/max and row hashing, which is useful for proving instant
equivalence. It is not sufficient by itself because:

- its row hashes are sorted and therefore do not prove row order;
- it does not fingerprint pandas or Arrow timezone dtypes;
- it does not compare PNG bytes or all visual labels; and
- it drops invalid timestamps from timestamp min/max summaries.

Focused tests must therefore prove row order, dtype, Parquet metadata,
diagnostic counts, and display labels in addition to unchanged T3 evidence.

### No support broadening

Normalization must not admit a currently rejected string, naive value,
ambiguous time, invalid value, native capture, or future vendor adapter merely
to broaden support. Each newly accepted source shape needs an explicit adapter
or schedule contract and the evidence required by its roadmap item.

## Analysis-semantic impact

behavior-preserving for unambiguous timestamp normalization

Interval inclusion and binning remain deferred.

This accepted contract changes only representation when the source already
identifies one unambiguous instant. Any rule that changes class membership,
deduplication, bins, cadence, missing-interval state, or metric output is an
intentional change owned by another ADR and metric version.

## Data, provenance, output, and compatibility impact

When later implemented, the canonical internal timestamp dtype is
`datetime64[ns, UTC]` in pandas and
`timestamp[ns, tz=UTC]` in Arrow/Parquet. Source and display timezone metadata
remain separate. Provenance is additive in a new canonical or manifest
contract when physical placement is approved; it is not retrofitted into
current published files under this ADR.

The following are not authorized by this decision:

- rewriting tracked fixtures or existing grouped/output Parquets;
- changing current public filenames or schemas;
- changing the active metric version;
- changing `TIMEZONE` or `PLOT_TIMEZONE` configuration values without a
  separately approved project/location policy and compatibility evidence;
- making grouped Aruba Parquet a permanent supported input;
- claiming native or another vendor adapter is supported; or
- filling missing provenance for legacy data.

## Migration and rollback constraints

Acceptance does not approve a migration. A later implementation plan must:

1. begin with one side-effect-free normalization component that returns
   structured values, provenance, and diagnostics without replacing active
   timestamp handling;
2. use that component as the later shared boundary for accepted capture
   adapters and sample scheduling without coupling it to metric changes;
3. read current aware Aruba fixture values as unambiguous instants;
4. preserve legacy inputs and outputs during equivalence validation;
5. emit canonical UTC only to a separately identified canonical artifact or an
   internal path approved by the physical-schema decision;
6. retain enough source evidence to diagnose every conversion;
7. fail before timestamp-dependent analysis when evidence is invalid or
   ambiguous; and
8. permit rollback by selecting the unchanged legacy path and artifacts.

Public migration, filename/version placement, compatibility duration, and
rollback across persisted schema versions remain governed by a later
ADR-0006 follow-up.

## Required fixtures and acceptance evidence

### T1: synthetic timestamp-contract tests

Add small, exact oracle cases for:

- aware UTC input, preserving nanoseconds and instant identity;
- aware named local input in both standard and daylight offsets;
- explicit fixed-offset input;
- naive input with no evidence, which must be rejected;
- naive input with explicit trusted named-zone evidence;
- invalid timestamp text with a stable diagnostic;
- Denver spring-gap local time, which must fail without shifting;
- Denver fall-fold local time without offset/fold evidence, which must fail;
- both repeated Denver occurrences with explicit `-06:00` and `-07:00`
  evidence, proving distinct UTC instants;
- adapter-supplied and manifest/dataset evidence, including conflict cases;
- a schedule value and capture value representing the same instant in
  different zones;
- rejection of a requested `frame.time_epoch` source contract until its
  grammar, unit, precision, and adapter evidence are accepted;
- pandas and Arrow/Parquet round trips as `timestamp[ns, tz=UTC]`;
- exact raw-token, source-offset/zone, method-version, and diagnostic
  preservation; and
- stable row order, equality, sorting, and elapsed-difference invariants.

Property tests must additionally cover round trips across a representative
IANA-zone transition range and prove that UTC normalization is idempotent.

### T2: ordinary regression tests

The ordinary suite must add or retain focused coverage for:

- grouped index-versus-column handling;
- the one approved normalization boundary from capture and schedule inputs;
- current rejection of unapproved strings, naive values, and native data;
- no silent `NaT` repair at the canonical boundary;
- schedule generation order and accepted legacy period grammar;
- current inclusive class selection as a legacy behavior assertion only;
- deduplication row order and thresholds;
- Parquet schema/timezone metadata and append compatibility; and
- plot point order, title, filename, formatter timezone, and expected local
  tick labels without PNG-byte comparison.

### T3: unchanged analysis-forward golden

Run the tracked analysis-forward pipeline and require the existing
`tests/golden/fingerprints.json` to remain unchanged. Also assert separately
that no tracked fixture was modified. A matching T3 fingerprint is necessary
but not sufficient for the reasons documented above.

### Later original-input evidence

PR-0020 must not claim source-format support until the appropriate
original-input evidence exists:

- T4: run the retained three Aruba captures through conversion, normalization,
  and grouping; compare every normalized instant, row count, and source-row
  order with the known checkpoint, and verify raw token/offset provenance;
- T5: compare the full pre-analysis class-scope checkpoint when the
  normalization boundary or source provenance affects the full capture set;
- T6: run the complete original-input Aruba path and reproduce the unchanged
  legacy T3 fingerprint before retiring any compatibility path; and
- future native/vendor evidence: retain a representative real capture before
  accepting that adapter's timestamp source, unit, precision, or timezone
  semantics.

Synthetic T1 data can prove the normalization contract. It cannot prove that a
vendor field, capture tool version, or deployment actually supplies the
claimed timezone evidence.

## Consequences and risks

### Benefits of the accepted direction

- One typed timeline supports deterministic comparison across locations and
  vendors.
- Arrow/Parquet and pandas preserve the current nanosecond-aware timestamp
  capability.
- Display choice can change without rewriting canonical data.
- DST ambiguity becomes an explicit validation state rather than an implicit
  pandas choice.
- Raw source values and normalization decisions remain auditable.
- Current aware Aruba behavior can be compared by instant without changing
  analysis math.

### Costs and compatibility risks

- Rewriting a Parquet field from `America/Denver` or `Etc/GMT+7` to `UTC`
  changes schema metadata and file bytes even when every instant is identical.
- Current plot titles and filenames use schedule timezone directly and need an
  explicit display conversion to avoid visible shifts after UTC storage.
- Historical grouped data has already lost original timestamp tokens and some
  source-zone evidence.
- The current Aruba parser's declared date format does not match all retained
  emitted tokens; relying on pandas inference would perpetuate
  format-dependent behavior.
- A global schedule timezone is insufficient for multi-location datasets.
- Fixed offsets cannot supply DST rules.
- pandas/Arrow version changes can affect mixed-zone parsing, precision, and
  ambiguous-time APIs; exact contract tests must pin intended behavior.
- T3's UTC-normalized, order-independent fingerprint can mask storage-zone,
  row-order, and display regressions unless focused tests accompany it.

## Questions explicitly deferred

- ADR-0005 / PR-0021: closed, open, or half-open class boundaries and the
  compatibility treatment of current inclusive endpoints.
- ADR-0005 / PR-0022: bin anchoring, expected cadence, timestamp jitter,
  empty-bin handling, and trailing partial bins.
- ADR-0004 / P07: missing beacon versus received beacon with missing CU,
  coverage, imputation, and streak bounds.
- ADR-0002 physical-schema follow-up: exact field names, inline versus sidecar
  timestamp provenance, manifest references, nullability, and invalid-row
  placement.
- ADR-0006 follow-up: public schema/version placement, migration duration,
  canonical/legacy coexistence, and rollback tooling.
- Vendor/adapter work: whether any source beyond the current Aruba evidence
  supplies trustworthy zone, offset, epoch-unit, or precision metadata.

## Project-owner approval record

On 2026-08-03, the project owner accepted the eleven decisions in this ADR's
Decision section. The approval completed the timestamp-representation
contract. It is now implemented; interval and missing-data semantics remain
governed by their separate ADRs.

On 2026-10-03, the project owner approved transparent daylight-saving-aware
schedule and report behavior for this project. `TIMEZONE` is therefore
`America/Denver`, not the former fixed `Etc/GMT+7`. The retained November 2025
schedule provides compatibility evidence: all 373 normalized class-session
identifiers and UTC endpoints are identical under both settings because those
dates use UTC-07. Focused winter/summer tests additionally prove that the same
09:00 civil class begins at 16:00 UTC in January (MST) and 15:00 UTC in July
(MDT). This is a project-location policy amendment; it does not infer a zone
from the executing computer or remove the need to configure a named zone for
another project's location.

The following remain evidence or implementation questions outside the
accepted scope:

1. the exact `aruba_erm.time` source-token grammar and adapter/tool versions
   for which its offset is authoritative;
2. `frame.time_epoch` grammar, unit, precision, extraction, round trip, and
   downstream support;
3. which source-, file-, dataset-, or location-timezone evidence is trusted
   for a specific vendor or deployment;
4. exact inline, sidecar, manifest, nullable, and invalid-row placement for
   timestamp provenance;
5. persisted canonical/legacy coexistence, public schema-version placement,
   migration, and rollback; and
6. retirement of the legacy Aruba timestamp path, which still requires the
   applicable T4-T6 original-input equivalence evidence.

## Addendum — ADR-0005 acceptance on 2026-08-26

[ADR-0005](0005-class-interval-boundary-semantics.md) is now Accepted for
half-open class membership, class-start-anchored left-closed/right-open bins,
actual-width terminal partial intervals, immutable timestamps, and per-BSSID
first-observation selection. On 2026-09-09, the owner removed the historical
rolling thinning gate from corrected selection because fixed bins provide the
cadence without cross-bin suppression. This
does not change ADR-0001's timestamp representation decision or current
runtime behavior. Missing/no-beacon state remains delegated to ADR-0004, and
activation remains gated by metric-version and migration work.
