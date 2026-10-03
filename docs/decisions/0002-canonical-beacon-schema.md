# ADR-0002: Canonical beacon schema

- Status: Accepted
- Decision: Accepted within the conceptual scope stated below
- Date proposed: 2026-07-29
- Date decided: 2026-07-30
- Owners/reviewers: Project owner
- Direct roadmap items:
  [PR-0007 through PR-0009](../post_refactor_roadmap.md)
- Dependent roadmap items:
  [PR-0010 and PR-0011](../post_refactor_roadmap.md)
- Supersedes: None
- Superseded by: None

> Current implementation status (2026-10-02): the canonical observation
> boundary and five-artifact output contract are implemented by
> `beacon_analysis.py run`. Migration-era compatibility discussion below is
> retained only as decision rationale; it does not define a supported legacy
> workflow.

## Scope of the accepted decision

This ADR accepts the conceptual canonical-data contract only. It establishes:

- the grain of one canonical beacon observation;
- logical raw, canonical, manifest, and derived-data authority boundaries;
- vendor-neutral canonical meanings as the intended downstream contract;
- value-state and transformation semantics;
- hybrid provenance as the architectural direction;
- the production and legacy-compatibility direction; and
- behavior-preservation and support-claim limits.

It deliberately does **not** select a complete physical schema, permanent
field placement, identifier algorithms, Parquet representation, or
migration/output-version policy.

Nothing in this record authorizes implementation, changes current analysis
math, establishes vendor or band support, or by itself makes PR-0011 ready.
[ADR-0006](0006-output-and-metric-versioning.md) was Accepted on 2026-07-31,
and the separate fixture/harness/test-plan review that now marks PR-0011
`Ready` is recorded in the
[roadmap](../post_refactor_roadmap.md).

## Context and current behavior

[`current_pipeline.md`](../current_pipeline.md) remains the source of truth for
runtime behavior. The current analysis path consumes Aruba-shaped grouped
Parquet, not a vendor-neutral canonical beacon contract.

The stages relevant to this decision currently behave as follows:

1. [`process_pcapng.py`](../../process_pcapng.py) requests decoded Aruba, WLAN,
   QBSS, and frequency fields, applies selected conversions and an SSID
   heuristic, derives `frequency_mhz`, and writes capture-level Parquet.
2. Aruba grouping combines capture-level rows by Aruba AP name. The separate
   BSSID workflow groups by BSSID and uses AP-name or BSSID filenames.
   Grouping and optional cleanup do not retain durable capture, exported-row,
   or pcap-frame lineage.
3. The former validation layer required an Aruba timestamp and a minimum set
   of Aruba/WLAN/QBSS columns while allowing extra columns to pass through. It
   rejected native `frame.time_epoch` data as unsupported downstream.
4. Analysis fills missing station count and admission capacity with zero,
   rejects retained missing CU before the legacy CU zero-fill, and derives
   Aruba-specific radio and room-level fields.
5. Published row-level outputs retain source-shaped names and incidental index
   artifacts. Aggregate outputs use separate, currently unversioned result
   schemas.

### Current field evidence

The operation codes are **R** for read, **W** for extracted/persisted, **V**
for explicitly validated, and **D** for derived.

| Current field or group | Operations | Introduction/current role | Evidence boundary |
|---|---:|---|---|
| `aruba_erm.time` | R/W/V | Preferred Aruba source timestamp when available; active legacy reports still use it directly | Tracked Aruba grouped/golden paths plus owner type-0/type-3 captures; absent from owner type-6 |
| `frame.time_epoch` | R/W/V | Frame-clock timestamp and accepted fallback when Aruba time is absent | Focused tests plus owner-supplied type-6 and native-radiotap conversion; active legacy reports still require Aruba-shaped grouped input |
| `wlan.ta` | R/W/V | Transmitter address; deduplication and current MAC-gap radio inference | Fixture-proven; stable physical-radio meaning is not |
| `wlan.bssid` | R/W | BSSID carried downstream and used by the separate BSSID grouping workflow | Fixture-proven; AP/radio membership is not |
| `wlan.vs.aruba.ap_name` | R/W/V | Aruba AP-name field and current hostname/grouping proxy | Fixture-proven only for the Aruba path |
| `wlan.ssid` | R/W/V | SSID text after the current conversion heuristic | Resulting strings are fixture-proven; original octets and repair provenance are not retained |
| `wlan.qbss.scount` | R/W/V | Station count; missing values later become zero | Current fixture path is proven; zero provenance is not |
| `wlan.qbss.cu` | R/W/V | Raw CU value; retained missing values fail before analysis | Current fixture path and selected metrics are regression-pinned; broader missing-value semantics are not |
| `wlan.qbss.adc` | R/W/V | Admission-capacity value; missing values later become zero | Current fixture path is proven; unit and zero provenance remain unresolved |
| `radiotap.channel.freq` | R/W | Preferred current source for `frequency_mhz` | Historical grouped values are null; owner type-6 and native-radiotap captures populate it |
| `wlan_radio.frequency` | R/W | Current fallback source for `frequency_mhz` | Historical grouped values are null; owner type-6 and native-radiotap captures populate it |
| `frequency_mhz` | R/W/D | Current frequency fallback result | Historical grouped values are null; corrected current-Aruba type-6 classification validates 2.4/5/6 GHz values |
| `wlan.ds.current_channel` | R/W | Beacon DS current-channel evidence retained for corrected current-Aruba grouping | Not present in the historical grouped fixture; populated in retained raw Aruba type-0 and owner-supplied type-6 evidence |
| `wlan.ext_tag.he_operation.6ghz.primary_channel` | R/W | Explicit 6 GHz primary-channel evidence | Populated at channel 165 in the owner-supplied Aruba type-6 tri-band capture |
| `wlan_radio.channel` | R/W | Capture/radio-header channel corroboration | Populated in owner-supplied Aruba type-3, type-6, and native-radiotap captures |
| Source radio rate/signal fields | Deferred | Available in some source captures but not requested by the current corrected extraction plan because no accepted metric consumes them | Re-extract from the authoritative capture if a later reviewed feature requires them; do not carry them speculatively |
| `capture_profile`, `capture_profile_detection`, `erm_type` | W/D | Per-file decoder lineage retained by conversion | Focused probes and auto-conversion of owner-supplied Aruba types 0/3/6 plus native radiotap |
| `band`, `channel` and evidence status/method | W/D | Minimal corrected current-Aruba classification used by PR-0037 radio grouping | Focused synthetic cases plus owner-supplied tri-band capture; not active legacy columns |
| `level_0`, `index`, `__index_level_0__` | W/D | Dataframe/Parquet index artifacts | Observed in current outputs/physical schemas; not source identifiers |
| `wlan.ta_int`, `radio_number` | R/W/D | Aruba MAC-gap radio derivation | Current behavior is characterized; not stable radio identity |
| `total_scount`, `location`, `room_scount` | R/W/D | Downstream radio/room enrichments | Current behavior is characterized; these are derived results, not raw beacon attributes |

Current report fields are analysis-result concepts, not attributes of one
received beacon. The current class-session stage writes plots and logs rather
than a class-session table. A notable current naming mismatch is that
`bssid_count` counts unique `wlan.ta` values rather than `wlan.bssid`.

### Fixture and provenance limits

The tracked analysis fixture contains five grouped AP Parquet files and
8,403,745 rows. Its eleven fields are:

- timestamp;
- transmitter address, BSSID, Aruba AP name, and SSID;
- station count, CU, and admission capacity; and
- radiotap frequency, WLAN-radio frequency, and normalized frequency.

The timestamp, address/name/SSID, and QBSS fields are populated in the tracked
fixture. All 8,403,745 values in each of
`radiotap.channel.freq`, `wlan_radio.frequency`, and `frequency_mhz` are null,
and this historical post-grouping fixture does not contain DS, HE 6 GHz, or
radio-header channel fields. The fixture therefore cannot validate band
classification by itself. This is a limitation of that retained fixture, not
a claim that raw 5 GHz or 6 GHz beacons lack usable channel evidence.

That historical limitation remains true, but it is no longer the only radio
evidence. The owner-supplied 2026-09-14 GUI captures establish Aruba ERM types
0, 3, and 6. Raw DS current-channel evidence supports ordinary 2.4/5 GHz
classification; the HE 6 GHz operation field explicitly identifies 6 GHz when
channel numbers overlap across bands. Type 3 adds an ERM radio header while
retaining Aruba time; type 6 has frequency and channel evidence for 2.4, 5,
and 6 GHz on one AP. PR-0037 uses this only to support the minimal current-Aruba
identity classifier; it does not make the historical grouped fixture a band
fixture or decide cross-band reporting. Full field counts and the preferred
capture type are recorded in
[`capture_input_profiles.md`](../capture_input_profiles.md).

The fixture is a narrow happy path:

- `wlan.ta` equals `wlan.bssid` on every row;
- each tracked transmitter maps to one SSID;
- repeated timestamps prove timestamp is not a source-row identifier;
- populated fields do not exercise absent, invalid, or unknown states; and
- the fixture has no independent vendor, topology, or physical-radio ground
  truth.

The tracked fixture begins after capture conversion and grouping.
[`tests/fixtures/README.md`](../../tests/fixtures/README.md) deliberately
excludes raw captures and external tools. The documented local short/full
capture checkpoints are valuable regression evidence, but they do not
establish a portable canonical provenance contract. The current grouped and
analysis outputs have no:

- stable source-capture or source-record identifier;
- retained capture hash or ingest/run identifier;
- exported-row contract or decoder-reported pcap frame number;
- adapter, decoder, or tool-version manifest;
- per-record vendor/capture-source classification;
- normalization, fallback, derivation, repair, or imputation evidence;
- schema/metric version; or
- per-field value state.

Incidental indexes, timestamps, transmitter addresses, and BSSIDs can aid a
manual search but are not durable lineage. Current grouped rows cannot be
claimed traceable to an original pcapng frame, and this ADR does not invent
that lineage.

## Decision drivers

1. Accuracy, auditability, and explicit unknown states take priority over a
   smaller or easier-to-use schema.
2. Current analysis behavior remains the baseline unless a later reviewed item
   explicitly authorizes an intentional semantic change.
3. A measured zero must remain distinct from missing, invalid, unknown,
   not-applicable, repaired, or imputed data.
4. Downstream analysis must not permanently depend on Aruba field names.
5. Provenance claims must stop where retained evidence stops.
6. Logical timestamp, identity, band, and version concepts must be reservable
   without deciding their owning ADRs prematurely.
7. Legacy fixtures must preserve regression value without becoming a permanent
   user-input or output contract.

## Options considered

### Record grain

- **Received observation:** one row per decoded, received beacon.
- **Expected interval:** one row per expected beacon opportunity, fabricating
  rows when no beacon is received.
- **Mixed row type:** combine received observations and expected/missing rows
  in one table with a discriminator.

The received-observation grain was selected. Expected intervals are derived
analysis concepts with different evidence and semantics.

### Data-layer authority

- Treat current vendor fields and derived columns as one flat authoritative
  dataset.
- Require four separate physical storage systems immediately.
- Define four logical authority boundaries and defer their physical layout.

Logical authority boundaries with deferred physical layout were selected.

### Downstream naming contract

- Keep Aruba names as the permanent internal API.
- Make vendor-neutral canonical meanings authoritative while retaining raw
  vendor evidence for adapters/audit.

Vendor-neutral canonical authority was selected.

### Quality semantics

- Use one overloaded status for presence, validity, and origin.
- Use two axes: value state and origin/transformation.

The two-axis model was selected because a value can be both present/valid and
derived, normalized, or repaired.

### Provenance direction

| Option | Strength | Limitation |
|---|---|---|
| Inline-only | Row-local auditability | Repeats run/capture/tool metadata and pressures every concept into every row |
| Manifest/reference-only | Central run/capture metadata | Extracted rows lose context without durable references |
| Hybrid | Row-specific evidence plus non-duplicated run/capture/tool facts | Requires reference integrity and a later physical-storage design |

Hybrid provenance was selected as the architectural direction.

### Compatibility direction

- Make existing grouped Aruba columns a permanent supported input.
- Make canonical data authoritative and use temporary Aruba aliases/projections
  for tests and migration.
- Keep raw vendor/adapter authority separate from canonical authority and
  combine it with temporary legacy projections where necessary.

Separate raw/vendor and canonical authority, with temporary test/migration
projections only, was selected.

## Decision

### 1. Canonical row grain

One canonical beacon row represents one decoded, received beacon observation.
It exists before deduplication, expected-interval construction, radio
aggregation, class filtering, and report aggregation. A missing expected beacon
is not represented as a fabricated beacon row.

### 2. Logical data layers

The architecture distinguishes:

1. run/capture manifest data;
2. raw vendor/adapter records;
3. canonical beacon observations; and
4. derived analysis and reporting datasets.

These are logical authority boundaries and do not require four separate
physical storage systems.

### 3. Canonical authority

Vendor-neutral canonical field meanings are the only intended downstream
analysis contract. Raw Aruba and other vendor fields may be retained for audit
and adapter processing, but they are not the permanent downstream analysis
API.

The canonical system must be able to represent these concept families without
this ADR deciding their permanent physical placement:

| Concept family | Accepted conceptual requirement | Deferred owner/detail |
|---|---|---|
| Observation time | Reserve observation timestamp and raw/source-time evidence | Exact representation, timezone, naive/DST/display rules: ADR-0001 |
| Source/run lineage | Represent source capture, source record/frame/row when available, ingest run, and manifest references | Exact identifiers and storage layout require original-input prototypes |
| Adapter/source context | Represent adapter name/version and vendor/capture-source evidence | Adapter interface/support: P04; physical placement deferred |
| Network identifiers | Represent transmitter address, BSSID, and SSID without treating presentation strings as authoritative evidence | Physical MAC/SSID storage deferred |
| AP/BSSID/radio identity | Reserve project-scoped AP identity, BSSID hierarchy, and operational radio-group concepts | Identity rules/methods/conflicts/static membership/value grain: ADR-0003 |
| Radio observations | Represent frequency, channel, band, CU, station count, and admission capacity when available | Band rules: DES-017–DES-021; metric semantics remain separately governed |
| Quality/transformation | Represent value state and source/normalization/fallback/derivation/imputation/repair evidence | Physical representation requires a focused prototype |
| Contract identity | Reserve schema-version and metric-version concepts | Identifiers, compatibility, outputs, and migration: ADR-0006 |
| Derived results | Keep radio inference, room enrichment, interval state, and reports distinct from raw/canonical observation authority | Owning analysis/metric versions |

### 4. Value-state and transformation semantics

The accepted value-state axis is:

| Value state | Meaning |
|---|---|
| `present_valid` | A value is present and valid under the applicable contract |
| `absent_from_source` | The source did not provide the field/value |
| `present_invalid` | Source material was present but failed validation |
| `unknown` | Available evidence cannot establish the value or a more specific state |
| `not_applicable` | The concept does not apply to this source/record/mode |

The accepted origin/transformation axis is:

| Origin/transformation | Meaning |
|---|---|
| `source` | A lossless representation of source evidence |
| `normalized` | Representation changed without changing intended meaning |
| `fallback` | A documented alternate source supplied the value |
| `derived` | The value was calculated from other retained evidence |
| `imputed` | A value not observed in the source was supplied by an explicit policy |
| `repaired` | Source material was altered by an explicit repair policy |

A measured zero is a value and must remain distinguishable from an imputed zero
or a missing value. Missing, invalid, unknown, or not-applicable values must
not silently become zero. This distinction does not itself authorize any
imputation or decide how a metric treats missing data.

### 5. Provenance direction

Hybrid provenance is the accepted architectural direction:

- row-level records should retain stable source/run/adapter references and
  row-specific value-state or transformation evidence where available;
- repeated capture, tool, configuration, adapter, and run metadata should live
  in referenced manifests or equivalent run-level records; and
- raw vendor values must remain available inline or through an auditable
  reference when canonical values cannot reproduce them.

Legacy data must report unavailable provenance honestly. It must not receive an
invented capture hash, source-row identity, adapter version, or pcap-frame
reference.

### 6. Production compatibility direction

The final production path begins with original beacon captures and sample
files, passes them through reviewed vendor adapters, and produces canonical
data for downstream analysis.

Separate raw/vendor and canonical authority is the production design.
Temporary Aruba aliases or legacy projections may exist only as test and
migration aids. They are not:

- supported user inputs;
- permanent public outputs; or
- a long-term downstream analysis contract.

They may be removed after the Aruba original-input canonical path passes its
required end-to-end equivalence gates.

### 7. Legacy fixture role

Existing Aruba-shaped grouped fixtures remain regression evidence only. They
may be labeled as a **test-only legacy Aruba profile**, but they are not a
permanent supported input format.

Missing legacy provenance, frequency evidence, adapter metadata, and frame
identity remain explicitly unavailable. Validation may report those
capabilities as unavailable; it may not synthesize them.

### 8. Behavior preservation and support limits

ADR-0002 changes none of the following:

- filtering or class inclusion;
- deduplication;
- radio inference;
- aggregation or station-count alignment;
- thresholds, formulas, or interval handling;
- plots, filenames, output schemas, or persisted outputs; or
- analysis math.

It does not authorize multi-vendor support, stable physical-radio identity,
band classification, 6 GHz support, or a new input/output compatibility
contract.

## Accepted logical definitions and required identifier properties

MAC-address concepts represent six octets. This ADR does not choose
fixed-size binary, normalized text, or another physical representation.

SSID represents zero to thirty-two octets. Display/search text is derived and
must not replace authoritative source bytes when those bytes are available.
This ADR does not finalize the Parquet representation of SSID bytes or text.

Future source/run identifiers must be:

- deterministic where appropriate;
- unique within a declared scope;
- path-independent;
- collision-resistant;
- reproducible;
- based on real retained source evidence; and
- independent of later canonical schema changes.

These properties do not select an identifier algorithm or imply that legacy
grouped rows already have such identifiers.

## Explicitly deferred implementation choices

### Physical quality/transformation storage

The two-axis semantic model is accepted. Its representation is not. A focused
prototype must compare nested per-row maps/lists, ordinary columns, sparse
sidecar tables, and any other viable representation against:

- faithful null/state round trips;
- row-level auditability;
- pandas/Arrow/Parquet interoperability;
- query cost and storage size;
- reference integrity; and
- compatibility with manifests and derived datasets.

### Identifier algorithms

This ADR does not select the construction of `source_capture_id`,
`source_record_id`, `ingest_run_id`, or related references. Exact algorithms
require original-input experiments using capture hashes, decoder frame
numbers, exported-row behavior, deterministic reruns, and collision cases.

No stable source-record or pcap-frame ID can currently be proven for the
tracked grouped fixture. A validator-local row locator must not be labeled as
source identity.

### MAC and SSID physical storage

Fixed-size binary versus normalized text for MAC addresses remains open. The
exact Parquet representation of SSID octets and derived display text also
remains open.

### Permanent field placement and requiredness

This ADR accepts the concepts the canonical system must represent; it does not
declare every concept permanently present or required in every beacon row.
Future prototypes and owning decisions may place fields in:

- canonical observation rows;
- run/capture manifests;
- raw adapter datasets;
- sidecar quality/provenance tables;
- derived analysis datasets; or
- later schema versions.

Timestamp, AP identity, physical-radio identity, band, schema version, and
metric version remain reserved concepts governed by their owning decisions.

### PR-0011 readiness

ADR-0002 acceptance alone did not make PR-0011 `Ready`. ADR-0006 now accepts
the required schema/metric version separation, legacy-profile identity,
validator outcomes, diagnostic persistence, and no-output-change rules. The
separate readiness review in the roadmap marks PR-0011 `Ready`, not `In
progress`; neither ADR authorizes implementation as part of its documentation
decision.

## Delegated decision ownership

| Owner | Decisions not made by ADR-0002 |
|---|---|
| ADR-0001 | Exact timestamp representation, timezone handling, naive timestamps, DST, and display conversion |
| ADR-0003 | Project-scoped AP identity, BSSID hierarchy, operational radio grouping, mapping methods/conflicts, project-static BSSID/SSID membership, fallback, and metric grain |
| ADR-0004 | Missing-CU and missing-beacon metric behavior, imputation authorization, averages, thresholds, and streak semantics |
| ADR-0005 | Class interval inclusion, binning, cadence, jitter, empty bins, and trailing intervals |
| ADR-0006 | Schema/metric identifiers, compatibility categories, validation-report persistence, migration, and output-version policy |
| P02/P04 | Parser repair/failure policy, adapter API, vendor detection, dataset partitioning, and lifecycle |
| DES-017–DES-021 / P06 | Frequency/band classification, BSSID band identity, justified 5/6 GHz pairing, comparability, and band-specific reporting |

No PSC, RNR, FILS discovery, security, or capability-element analysis is
included in this ADR.

## Analysis-semantic impact

behavior-preserving

This ADR changes documentation and architectural authority only. It does not
change row inclusion, deduplication, current Aruba radio inference,
aggregation, thresholds, interval handling, plots, filenames, outputs, or
analysis math.

Later use of canonical identity, band classification, or missing-data states
may enable intentional semantic changes, but those require their own accepted
decisions, metric versions, fixtures, comparisons, and roadmap authorization.

## Data, provenance, output, and compatibility impact

There is no runtime data, output, fixture, or schema change from accepting this
ADR. Future implementation will introduce canonical/provenance artifacts only
under separately reviewed physical schemas and versioning rules.

The current Aruba grouped fixture remains unchanged and test-only. It must not
be rewritten or relabeled as fully canonical. Current published Parquet/CSV
schemas and filenames remain unchanged.

## Migration and rollback

This ADR does not approve a data migration or public compatibility policy.
ADR-0006 and later persistence/runner decisions own those details.

It does accept two migration constraints:

1. temporary Aruba aliases/projections are test and migration aids rather than
   permanent supported inputs/outputs; and
2. they are removable only after the original-input Aruba-to-canonical path
   passes the required end-to-end equivalence gates.

Until that gate is met, current baseline fixtures and outputs remain available
for regression comparison.

## Required tests and acceptance evidence

Acceptance of this conceptual ADR is supported by:

- the active-pipeline and field inventory above;
- the tracked Aruba grouped/analysis-forward evidence;
- explicit documentation that the tracked fixture lacks frequency and source
  lineage evidence; and
- project-owner review on 2026-07-30.

Future implementation requires separate evidence:

| Future obligation | Minimum evidence |
|---|---|
| Physical quality/provenance prototype | Synthetic truth table covering every accepted value state/origin plus measured and imputed zero |
| Identifier algorithm | Original capture, retained capture identity/hash evidence, decoder frame numbers/export rows, deterministic rerun and collision cases |
| MAC/SSID storage | Lossless round trips for six-octet MAC values and zero-to-thirty-two-octet SSIDs, including non-text SSIDs |
| Legacy profile validation | T1 focused cases plus unchanged T3 tracked golden evidence |
| Original-input canonical equivalence | Appropriate T4–T6 checks from retained Aruba captures and sample files |
| Any vendor or band support | Representative real capture and topology/frequency evidence for the exact claimed capability |

PR-0011 remains validation-only and must leave the existing T3 golden
fingerprint, fixtures, published outputs, and active metric version unchanged.

## Consequences and risks

### Benefits

- Downstream analysis has one intended vendor-neutral authority.
- Raw evidence remains distinguishable from canonical meanings and derived
  results.
- Missing, invalid, unknown, derived, repaired, and imputed values can be
  represented without conflating them with measured zero.
- Provenance has a direction that scales beyond repeated per-row metadata.
- Legacy regression evidence remains useful without becoming a permanent API.

### Costs and risks

- Several implementation decisions remain deliberately open and require
  prototypes before schema implementation.
- Hybrid provenance will require reference-integrity and lifecycle tests.
- Temporary aliases can become accidental APIs unless their test-only status
  and removal gate remain explicit.
- Legacy grouped data cannot recover lineage already discarded.
- Treating an AP name or current `radio_number` as stable identity would bypass
  ADR-0003.
- Treating the tracked fixture as 5 GHz would bypass the absent frequency
  evidence and the P06 decision/fixture gates.

## Unresolved follow-up work

1. Prototype physical quality/transformation representations.
2. Experiment with capture and source-record identifier algorithms using
   original inputs.
3. Prototype lossless MAC and SSID storage representations.
4. Decide permanent field placement and mode-requiredness in the physical
   schema design.
5. Implement PR-0011 only within ADR-0006's accepted validation-only scope and
   the roadmap's T1-T3 acceptance gates.
6. Implement the accepted timestamp, identity, and interval contracts only
   through their roadmap gates; complete the still-unresolved missing-data and
   band decisions under their owning ADRs/phases.

## Addendum — interval ownership after ADR-0005 acceptance

On 2026-08-26, [ADR-0005](0005-class-interval-boundary-semantics.md) was
Accepted for class/bin geometry, Aruba raw/captured/dedup/bin timing
separation, and per-BSSID first-observation selection. ADR-0003 continues to
own project radio grouping and BSSID membership. Accepted ADR-0004 owns
received/missing CU, expected/no-beacon, coverage, empty-bin, and streak-bound
semantics. This clarification changes no ADR-0002 schema decision or runtime
behavior.

## Addendum — accepted ADR-0003 identity and value-grain scope

Also on 2026-08-26,
[ADR-0003](0003-ap-and-physical-radio-identity-contract.md) was Accepted after
owner scope clarification. It selects project-scoped configured AP name,
BSSID-primary stream grouping, the BSSID-to-radio-to-AP-to-classroom hierarchy,
whole-project operational radio grouping, project-static BSSID ownership, and
method/grain diagnostics without a device matrix.

One canonical ADR-0002 observation still represents one received beacon before
de-duplication. ADR-0003's at-most-one-record-per-BSSID/bin rule applies to a
derived selected-observation table, not canonical ingestion. CU and admission
capacity then resolve once per radio group/bin, while station count remains
BSSID-grain and is summed across the radio group. Exact physical placement and
missing/conflicting-value policy remain deferred, so this addendum changes no
accepted ADR-0002 schema, capability claim, or runtime behavior.
