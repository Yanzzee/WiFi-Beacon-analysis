# ADR-0003: Project-scoped AP identity, BSSID hierarchy, and radio grouping

- Status: Accepted
- Decision: Accept C-1 through C-8 as revised, together with the topology,
  field-precedence, and metric-grain clarifications recorded below
- Date proposed: 2026-08-26
- Date revised: 2026-09-14
- Date decided: 2026-08-26
- Owners/reviewers: Project owner, also serving as analysis-correctness
  reviewer
- Roadmap items: Directly
  [PR-0035 and PR-0038](../post_refactor_roadmap.md); dependent
  PR-0036, PR-0037, PR-0039, and PR-0040
- Related decisions:
  [ADR-0001](0001-canonical-internal-timestamp-representation.md),
  [ADR-0002](0002-canonical-beacon-schema.md),
  [ADR-0005](0005-class-interval-boundary-semantics.md), and
  [ADR-0006](0006-output-and-metric-versioning.md)
- Supersedes: None
- Superseded by: None

> Current implementation status (2026-10-02): configured AP-name filtering,
> AP/classroom identity, and AP + band + channel radio grouping are active in
> `beacon_analysis.py run` for the supported Aruba and Extreme adapters.
> Migration wording below is historical decision rationale.

## Scope and decision state

This decision establishes an operational identity contract for one analysis
project. It is deliberately narrower than hardware inventory or long-term
asset management.

The project owner supplied these intended operating assumptions:

- every configured AP name used by the software is unique within the project;
- AP name, AP-to-room placement, radio count, and BSSID allocation are stable
  for the project;
- a BSSID is unique within the project and identifies one BSS, meaning one
  radio/SSID combination;
- `wlan.ta` in a beacon is expected to equal `wlan.bssid`, but BSSID is the
  technically preferred key;
- BSSID reuse or reassignment is not expected within a project;
- a new survey/year may be a separate project and compared manually; and
- the current supported input files must complete analysis without an
  identity-review step.

These are project input-contract assumptions, not claims that names or BSSIDs
can never change or be reused in every network for all time.

This ADR therefore decides:

1. configured AP name as project-scoped AP identity;
2. BSSID as the primary observed BSS stream and per-BSSID de-duplication
   grouping key;
3. an automatically derived radio group under each AP rather than a hardware
   serial/slot registry;
4. static BSSID ownership within one project rather than time-scoped reuse
   history; and
5. deterministic fallbacks and diagnostics rather than interactive identity
   adjudication.

The project owner accepted C-1 through C-8 on 2026-08-26 and supplied the
additional networking invariants recorded in this revision. The supported
Aruba and Extreme paths now implement configured-hostname scope and shared
project-radio identity in the corrected workflow.

### Current-Aruba PR-0036 scope refinement — 2026-09-11

For the current supported Aruba project path, every in-scope beacon is now
expected to carry the configured AP hostname. `Sample_APs.csv` is the project
scope list and supplies the existing `Hostname -> Building-Room` association.
This permits a smaller implementation than the general fallback contract:

- validate that configured hostnames are non-empty and unique after trimmed,
  case-insensitive comparison, and that every configured AP has a classroom;
- match observed Aruba AP names after the same comparison while retaining the
  configured spelling;
- use the configured hostname directly as the deterministic project-scoped
  `ap_id` and attach its configured classroom;
- discard missing hostnames, unconfigured neighboring AP hostnames, and APs
  configured for another class's room before full adapter normalization and
  timing selection; and
- retain only aggregate exclusion counts for those rows.

Therefore the current PR-0036 path does not emit an ordinary “unmapped BSSID”
result. Supporting an in-scope AP that omits its hostname is deferred and will
likely require an optional configured BSSID list. Anonymous AP grouping,
vendor BSSID-pattern AP discovery, and other vendors remain later work. The
general C-7/C-8 fallback definitions remain available for that future scope;
they are not exercised by current-Aruba PR-0036.

### Current-Aruba PR-0037 radio resolution — 2026-09-14

For current Aruba input, radio grouping now occurs after configured-hostname
AP resolution and across all configured project observations before a
classroom or class-time slice is selected.

The practical evidence order is:

1. classify band from frequency when available; a decoded HE 6 GHz operation
   primary-channel field is the explicit fallback proof of 6 GHz;
2. choose the primary channel from HE 6 GHz operation for a 6 GHz beacon,
   otherwise the DS parameter-set current channel, then the radio-header
   channel;
3. compare channels only inside the same configured AP and band;
4. group BSSIDs whose observed channel sets overlap, retaining all observed
   channels so a DFS or other whole-radio channel change does not create a new
   radio identity.

MAC allocation patterns are not used when the AP name is available. Missing
or conflicting AP-name cases remain future work and may later use an explicit
BSSID mapping or a separately reviewed inference rule.

Channel numbers alone are not band identity. For example, 2.4 GHz channel 1
and 6 GHz channel 1 are separate radio evidence. Radios on different APs also
remain separate even when their band and channel match.

The resolver assigns deterministic per-AP radio numbers from sorted stable
BSSID membership and emits `radio_group_id`, band, observed channels, grouping
method, result grain, status, and compact diagnostics. Missing or conflicting
band/channel evidence remains at BSSID grain. A BSSID mapped to multiple APs
or SSIDs, or a proposed radio group containing the same SSID on multiple
BSSIDs, is not silently combined.

The owner-supplied type-6 Aruba ERM capture dated 2026-09-14 provides the
representative tri-band evidence. One AP exposes channel 11 at 2462 MHz,
channel 104 at 5520 MHz, and channel 165 at 6775 MHz; the 6 GHz beacon also
exposes `wlan.ext_tag.he_operation.6ghz.primary_channel=165`. The capture must
be decoded as `aruba_erm_type6`, unlike the retained production type-0 input.
It has no usable Aruba ERM timestamp, so accepted ADR-0008 frame-time fallback
applies.

## What this decision does not require

The accepted decision does **not** require:

- AP serial numbers;
- BSSID-to-serial inventory;
- a hardware or device matrix;
- a persistent opaque-ID registry;
- time-scoped AP replacement or BSSID-reuse history;
- per-run user inspection or approval; or
- year-over-year automated hardware continuity.

The only required operator mapping remains the existing configured AP-name to
room/location relationship. A future optional radio override may be added if
real data demonstrates a need, but it is not part of the base contract.

## Terms in networking language

| Term | Meaning in this decision |
|---|---|
| Project | One input/configuration and analysis scope in which AP names, room placement, radio count, and BSSID ownership are assumed stable. |
| AP name | The administrator-configured AP name decoded from a vendor-specific field when available. An explicit capture/BSSID-to-AP association may supply it when one exists, but the existing AP-name-to-room mapping cannot identify an otherwise unnamed BSSID. |
| `ap_id` | A deterministic project-scoped identifier based on the normalized AP name when available. A vendor-pattern-only anonymous AP group uses a separately namespaced deterministic fallback derived by that adapter; it does not invent a configured AP name or classroom. Neither form is a hardware serial or lifetime asset ID. |
| BSSID | The primary MAC address identifying the observed BSS and partitioning its observation stream. The project contract assumes it belongs to exactly one AP, one radio group, and one SSID/value state; the resolver emits at most one such mapping and leaves unresolved parent IDs null. BSSID alone does not distinguish repeated beacon observations. |
| Transmitter address | `wlan.ta`. For beacons it is expected to match BSSID and is a checked fallback when BSSID is unavailable. |
| Radio group | The operational set of BSSIDs determined to use one AP radio by explicit decoded radio metadata or a source/vendor-specific grouping rule. Without independent topology evidence, the group is not proof of a hardware serial or slot. |
| `radio_group_id` | A deterministic project-scoped label combining `ap_id` with a project-wide radio-group number. The name deliberately avoids claiming a hardware-proven physical-radio identity. |
| BSSID-level result | A result for one BSS when the software cannot safely combine it with other BSSIDs into a radio group. |
| Mapping method | The packet field, optional explicit configured association, or vendor grouping rule that produced an AP/radio grouping. |

One physical radio can advertise several BSSIDs, normally one for each BSS/
SSID it serves. BSSID identifies the individual BSS; it does not by itself
show which other BSSIDs share the same physical radio. Combining multiple
BSSIDs therefore still needs explicit radio metadata or an automatic
vendor/capture-specific rule.

## Accepted topology, grouping evidence, and metric grain

The accepted project hierarchy is:

```text
classroom <- one or more APs <- one or more radio groups <- one or more BSSIDs
          <- repeated beacon records over time
```

The corresponding invariants are:

- one canonical beacon observation represents one received beacon carrying one
  canonical BSSID value, using a labeled TA-derived fallback only when the
  received beacon lacks BSSID; canonical observations remain pre-selection
  rather than being collapsed at ingestion;
- in a resolved project topology, one BSSID maps to exactly one radio group;
- one resolved radio group maps to exactly one AP;
- one resolved AP maps to exactly one configured classroom/location;
- each level may have multiple children as shown above;
- each BSSID maps to one SSID; and
- within one radio group, an SSID occurs on at most one BSSID. A repeated SSID
  under the same AP is therefore evidence that the BSSIDs use different
  radios.

The same SSID text may still appear on different radios, APs, and BSSIDs. SSID
text is not AP/radio identity evidence by itself.

The hierarchy above describes resolved mappings; the unattended fallback must
not fabricate missing parents. When shared-radio membership is unresolved,
the output remains at `result_grain=bssid`, uses
`radio_grouping_method=per_bssid_fallback`, and leaves `radio_group_id` null.
An already resolved `ap_id` and classroom may remain populated. When AP
membership is also unresolved, `ap_id`, `radio_group_id`, and classroom are
null and the BSSID remains an explicitly unmapped result. These states are
incomplete topology, not exceptions that permit a BSSID to map to two parents.

AP and radio grouping use networking evidence in this order:

1. Equal decoded administrator-configured AP names mean the BSSIDs belong to
   the same AP. Major-vendor adapters should decode the appropriate optional
   AP-name information element when representative captures are available.
   BSSID similarity or RSSI must never merge observations carrying different
   decoded AP names.
2. When AP name is unavailable for some BSSIDs, a documented vendor-specific
   BSSID allocation pattern may associate them with the same AP. The owner
   notes that, for many vendor allocations, a one-character difference within
   the final six hexadecimal characters is useful operational same-AP evidence.
   That phrase is not itself a complete comparison algorithm: the adapter must
   define its vendor-assigned mask/range, normalization, carry behavior, and
   false-positive boundaries and validate them with a representative fixture.
   It is not a universal hardware rule.
3. Radio grouping occurs only after AP grouping and prefers an explicit radio
   number, slot, or other decoded radio designation. Different contemporaneous
   bands, frequencies, or channels under one AP separate radio groups. A
   vendor-specific radio-allocation rule may be used when validated for that
   adapter; a BSSID pattern that proves only common AP membership is
   insufficient. Otherwise the result remains at BSSID grain.
4. Over-the-air signal levels within approximately 3 dBm are supporting
   evidence that BSSIDs may share a radio when capture source, antenna,
   vantage, and observation time are comparable, but different radios can
   produce similar levels. RSSI alone must not combine BSSIDs or override
   stronger packet/configuration evidence.

Every BSSID must eventually be categorized as 2.4, 5, or 6 GHz. For
contemporaneous observations, all BSSIDs in one radio group must have the same
band because one radio operates in only one band at a time. One AP may still
have multiple radios in the same band; contemporaneous channel/frequency, an
explicit radio number, or another designation must keep those radios separate.
A stable BSSID changing channel or band evidence over time is not by itself a
new identity; P06 must decide how to model valid changes and conflicts. Band
discovery and cross-band reporting remain later P06 work rather than an
ADR-0003 implementation prerequisite.

Metric grain follows the accepted hierarchy:

- the derived selected-observation table retains at most one beacon record per
  BSSID per bin interval; an empty BSSID/bin still has no record;
- channel utilization and admission capacity are radio-group properties
  repeated in each BSSID's beacon, so one resolved value per radio group per
  bin is used, never summed across BSSIDs, and disagreement is diagnosed;
- station count is a BSSID property, so every selected BSSID/bin remains
  eligible regardless of which BSSID supplies radio-level CU. A radio/bin
  total sums one selected station-count value from every member BSSID; an
  SSID/classroom/bin total sums the selected BSSID values for that SSID across
  in-scope radios/APs; and a band/classroom/bin total sums the selected BSSID
  values on radios in that band. Each BSSID contributes at most once to each
  applicable rollup; and
- exact corrected observation-slot/tie selection is governed by
  [accepted ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md), while
  ADR-0005 remains authoritative for its retained rules and the legacy
  comparison; missing/conflicting metric treatment remains an ADR-0004
  decision.

## Capture-source boundary

Captures can present different fields depending on how they were collected:

- Aruba ERM is decoded from UDP encapsulation and can supply Aruba-specific
  metadata such as `wlan.vs.aruba.ap_name` and raw `@aruba_erm.time`.
- An over-the-air capture presents native 802.11 fields and may not contain an
  AP-name information element at all.
- Other vendors may place the AP name or radio information in different,
  optional fields that require an adapter-specific mapping.

The capture-source profile must therefore record which adapter/mode supplied
AP name, BSSID, radio grouping, and observation-time evidence. Missing
vendor-specific fields do not make the beacon itself invalid. They determine
whether the result can be AP/radio-group level or only BSSID level.

Capture time and beacon-slot time are not identity keys. In particular, raw
`@aruba_erm.time`, `frame.time_epoch`, and `wlan.fixed.timestamp` must not be
used to infer that BSSIDs belong to one AP or radio. The timing proposal raised
during this review is recorded separately under
[Timing evidence and deferred decisions](#timing-evidence-and-deferred-decisions).

## Context and current behavior

The current pipeline approximates the intended project contract but does not
express it cleanly:

- Aruba grouping uses `wlan.vs.aruba.ap_name`, but the sanitized filename then
  becomes coupled to downstream AP lookup. Sanitization/truncation can make a
  storage path different from the actual AP identity.
- The generic grouping path groups by `wlan.bssid`, but may select an Aruba AP
  name from current partition/group order as its output filename.
- The sample inputs already map AP hostname/name to location. They do not and
  should not need to map BSSIDs to hardware serials.
- Analysis partitions de-duplication by `wlan.ta` even though `wlan.bssid` is
  the preferred technical BSS stream key.
- Aruba radio inference converts transmitter addresses to integers, sorts
  them, and starts a new local `radio_number` when an adjacent MAC gap exceeds
  one.
- That radio grouping is recomputed for each AP/class subset. If a BSSID is
  absent from one class slice, the inferred number can split or renumber even
  though the project's actual radio count did not change.
- `AP_PLATFORM=aruba` permits the MAC-allocation rule but does not document the
  method in outputs.
- Current conversion extracts `radiotap.channel.freq` and
  `wlan_radio.frequency` into `frequency_mhz`, but it does not extract
  `wlan.ds.current_channel` or `wlan_radio.channel` and does not classify band.

The current adjacent-numeric-MAC rule is a legacy baseline only. It differs
from the accepted vendor-scoped same-AP BSSID-pattern concept, ignores
radio-specific band/channel/designation evidence, and is not automatically an
accepted canonical radio-group rule.

The corrected direction is to resolve AP/BSSID/radio grouping once from the
whole project dataset before class filtering, then reuse that generated map
for every class session. The map is software output, not a user-maintained
device matrix.

## Evidence and fixture limits

The tracked analysis fixture supports several project assumptions:

- it contains five grouped AP files and 8,403,745 rows;
- `wlan.ta` equals `wlan.bssid` on every tracked row;
- each tracked BSSID/transmitter maps to one SSID; and
- the existing golden pins the repeatability of current AP names,
  `radio_number`, and `radio_count` behavior.

It begins after capture conversion/grouping and has no independent physical
radio topology. It can prove that a project-scoped rule is deterministic on
the current data; it cannot prove that one MAC-allocation rule identifies
hardware radios for every vendor/AP family.

That distinction changes the support claim, not the requirement to analyze
the current files. Legacy Aruba analysis may retain its characterized
MAC-gap rule behind the existing metric/version baseline. A corrected resolver
may promote a same-AP or same-radio vendor rule only when its distinct purpose
and representative evidence are documented. This does not require every user
to maintain a device inventory.

## Decision drivers

1. Existing supported Aruba input must run unattended.
2. AP names are administrator-controlled, unique, and stable within a project,
   so they should be used directly rather than replaced by hidden IDs.
3. BSSID is the primary BSS stream and per-BSSID de-duplication grouping key;
   TA is expected to agree and is a fallback, not an additional normal-case
   grouping key. Observation time/slot logic decides which repeated beacon is
   retained.
4. AP-room configuration remains simple and is not expanded into hardware
   inventory.
5. Radio grouping must be deterministic for the entire project and not change
   because one class slice happens to miss a BSSID.
6. One radio can contain multiple BSSIDs, so BSSID-level identity and radio
   grouping remain separate concepts.
7. Source/vendor differences are handled by adapters and deterministic
   precedence, not interactive user adjudication.
8. When radio grouping is unavailable, the software should preserve
   useful BSSID-level analysis and label its scope honestly.
9. Filenames are storage locators, not AP identity authority.
10. Cross-project hardware continuity, BSSID reuse, replacement, and rotation
    are explicitly out of scope for this version.
11. The accepted BSSID/radio/AP/classroom hierarchy must preserve the distinct
    metric grains: one selected beacon per BSSID/bin, one CU/admission-capacity
    value per radio/bin, and every selected per-BSSID station count for radio,
    SSID/classroom, and band/classroom rollups.

## Options considered and accepted selections

The option tables preserve the alternatives reviewed before acceptance.

### C-1 — What AP identity and radio grouping mean

| Option | Practical meaning | Benefits | Risks |
|---|---|---|---|
| E-A — Project-scoped configured identity | AP identity is its unique configured AP name; each resolved BSSID receives stable membership in one of that AP's derived radio groups for this project | Uses current data and operational assumptions; no hardware inventory | Does not automate rename/replacement continuity across projects |
| E-B — Hardware asset identity | AP/radio identity follows serials, controller UUIDs, or hardware slots | Supports lifecycle tracking | Requires the device inventory the project does not want |
| E-C — Room/deployment identity | AP identity follows the room or installation position | Simple location reporting | Merges different APs and cannot represent several APs in one room |

**Decision:** E-A.

This decision changes software grouping, not operator data collection:

- use normalized AP name as the AP key instead of treating its output filename
  as identity;
- calculate radio groups once from the full project BSSID set instead of once
  per class slice; and
- attach the generated project radio group to every resolved observation
  before class analysis, while retaining the explicit BSSID-level fallback for
  unresolved membership.

No serial number or BSSID-to-serial mapping is required. A replacement or
rename is handled as a separate project and may be compared manually.

### C-2 — Identifier format and persistence

| Option | Practical meaning | Benefits | Risks |
|---|---|---|---|
| I-A — Deterministic natural IDs | Use project/AP name when available, an adapter-namespaced anonymous AP-group key only as fallback, and an automatically assigned project-wide radio number | Readable, reproducible, and no registry | Only stable within the declared project assumptions |
| I-B — Opaque registry IDs | Issue unrelated AP/radio IDs from a persistent device table | Supports asset lifecycle | Extra infrastructure and user-maintained mappings |
| I-C — Filename/location IDs | Use grouped filename or room as identity | Already present in the workflow | Sanitization collisions and multiple APs per room make these unsafe |

**Decision:** I-A.

The logical forms are:

```text
ap_id = <project scope>/name/<normalized configured AP name>
     or <project scope>/anonymous/<adapter rule>/<deterministic member key>
radio_group_id = <ap_id>/radio/<project-wide deterministic number>
bssid = normalized wlan.bssid
```

The anonymous form is used only when a supported adapter rule establishes a
common AP without a configured name; it carries no classroom mapping. The
exact separators, normalization, and member-key algorithm remain a small
physical-schema choice. The important requirements are that radio numbering
uses the same whole-project BSSID map for every class, is independent of
row/file order, and is regenerated deterministically from the inputs. The
generated mapping may be persisted with the run for audit/reuse, but it is not
a device matrix the operator must populate.

### C-3 — Which packet/configuration fields win

The practical use of precedence is to answer ordinary packet questions without
asking the user to resolve them during a run.

| Networking question | Primary source | Automatic fallback | Required behavior |
|---|---|---|---|
| Which AP? | Decoded vendor AP name, such as `wlan.vs.aruba.ap_name` | An explicit capture/BSSID-to-AP association, then a supported vendor BSSID same-AP pattern | Prefer the decoded AP name; a BSSID pattern may associate unnamed BSSIDs with a named or anonymous AP group but must not invent a configured name/classroom |
| Which BSS stream? | `wlan.bssid` | `wlan.ta` when BSSID is missing | Prefer BSSID; if both exist and differ, keep BSSID and emit a mismatch diagnostic; observation time/slot logic performs selection within the stream |
| Which radio group? | Explicit decoded vendor radio number, slot, or designation | A validated vendor-specific radio-allocation rule, constrained by contemporaneous band/frequency/channel evidence | Keep different bands/channels/designations separate; a same-AP BSSID pattern is not automatically a same-radio rule; if evidence remains insufficient, keep a BSSID-level result |
| Which band? | Validated frequency/channel evidence from the source adapter | None in ADR-0003 | Classify per BSSID as later P06 work; never combine BSSIDs from different bands into one radio group |
| Which room/location? | Existing configured AP-name-to-room mapping | None | An unmapped AP/BSSID can still be analyzed, but room-level aggregation is unavailable |

**Decision:** replace the general evidence ledger with
this deterministic networking-field precedence.

AP association and radio grouping are separate passes. A vendor BSSID pattern
may establish that unnamed BSSIDs belong to the same AP without proving that
they share one radio. Radio grouping requires radio-specific evidence and must
honor the band/channel separation rules above.

Examples:

- An Aruba beacon reporting AP name `B123-AP01` is assigned to that AP; the
  existing project file maps `B123-AP01` to its room.
- Beacon BSSID `aa:bb:cc:dd:ee:10` selects the BSS stream in which repeated
  observations are evaluated. Matching TA confirms the expected frame shape
  but is not added to the normal grouping key; the timing contract selects
  the retained observation.
- If a supported Aruba BSSID-allocation rule groups `...:10` and `...:11` on
  one radio, both receive the same project radio-group number.
- If two BSSIDs under one AP report different bands or channels, they receive
  different radio-group numbers. Equal RSSI alone never combines them.
- If an over-the-air beacon has no AP-name element and no supported radio rule,
  it remains a valid BSSID-level observation; it is not silently called a
  physical AP/radio result.

Filenames may locate configured inputs but never override a decoded/configured
AP name merely because they were sanitized to similar text.

### C-4 — What mapping information is reported

| Option | Practical meaning | Benefits | Risks |
|---|---|---|---|
| S-A — Numeric confidence/status system | Attach scores and several resolution/conflict states to every mapping | Detailed | Hard to interpret and unnecessary for stable project inputs |
| S-B — Method, result grain, and diagnostics | Record how AP/radio grouping was obtained and whether output is radio-group or BSSID level | Networking-focused and sufficient for troubleshooting | Does not express probabilistic confidence |
| S-C — No mapping metadata | Emit only AP/radio labels | Smallest output | A fallback result looks identical to a directly decoded result |

**Decision:** S-B.

The minimum logical fields are:

- `ap_identity_method`: for example `vendor_ap_name`,
  `explicit_ap_association`, `vendor_bssid_ap_pattern`, or `unmapped`;
- `radio_grouping_method`: for example `explicit_radio_designation`,
  `vendor_radio_pattern`, `channel_frequency_radio_group`, or
  `per_bssid_fallback`;
- `result_grain`: `ap_radio_group` or `bssid`; and
- zero or more stable diagnostic codes such as `ta_bssid_mismatch`,
  `ap_name_missing`, `ap_name_mapping_mismatch`,
  `project_bssid_mapping_conflict`, or `radio_count_changed`.

Comparable RSSI may be retained as supporting evidence or a diagnostic, but it
is not an identity method or a numeric confidence score.

These fields do not create an operator review step. They explain the result if
someone later asks why two BSSIDs were combined or why one result stayed at
BSSID level.

### C-5 — Project-static versus time-scoped membership

| Option | Practical meaning | Benefits | Risks |
|---|---|---|---|
| V-A — One mapping for the project | Each BSSID has one AP/radio/SSID relationship for the project | Matches project operations; no history table | A changed/reused BSSID requires a new project or explicit future feature |
| V-B — Time-scoped history | Model changes with `[valid_from, valid_to)` records | Supports rotation/replacement | Adds data and logic for behavior the project does not expect |
| V-C — Hardware-lifetime history | Track BSSID/AP/radio continuity across surveys and replacements | Longitudinal automation | Requires hardware inventory and adjudication |

**Decision:** V-A.

The prior statement that one BSSID could map to different radios in
non-overlapping intervals is removed from the accepted contract. In the actual
project topology, one BSSID belongs to one AP, one radio group, and one
SSID/value state. The resolver may leave a parent unknown, but it must never
emit two parent mappings for the same BSSID. A later survey with different
ownership is a separate project and may be compared manually.

Capture timestamps still show when a beacon was observed. They are not BSSID
membership-validity records. If current-project input violates the static
mapping assumption, the software records `project_bssid_mapping_conflict`,
keeps unaffected BSSIDs running, and does not silently merge the conflicting
observations.

### C-6 — BSSID, TA, SSID, and radio grouping

| Option | Practical meaning | Benefits | Risks |
|---|---|---|---|
| M-A — BSSID ownership plus AP radio grouping | BSSID identifies one BSS; adapter logic groups several BSSIDs onto one AP radio | Technically appropriate and supports multi-SSID radios | Vendor radio grouping still needs a documented rule |
| M-B — TA-first legacy grouping | Continue treating TA as BSSID and deriving radio numbers per class slice | Minimal change | Uses the less precise field and allows slice-dependent renumbering |
| M-C — Treat every BSSID as a physical radio | Never combine BSSIDs | Always automatic | Overcounts radios when one radio advertises several SSIDs |

**Decision:** M-A.

Required project invariants are:

- use normalized `wlan.bssid` to partition beacon observations into BSS
  streams before the separately decided time/slot selection;
- expect `wlan.ta == wlan.bssid` for beacons;
- when they differ, use BSSID and report the mismatch;
- when BSSID is missing but TA exists, use TA as a labeled fallback;
- project truth has one AP, one radio group, and one SSID/value state per
  BSSID; a resolved output maps at most one of each and otherwise uses the
  explicit null/unmapped fallback above;
- one SSID occurs at most once in a radio group;
- one AP radio group may contain several BSSIDs/SSIDs; and
- different contemporaneous bands, channels, or explicit radio designations
  under an AP separate radio groups; a stable BSSID's channel change over time
  does not by itself change its project identity; and
- derive radio grouping once from all project observations, before any class
  session filters.

Overlapping capture sources can produce the same BSSID and observation
timestamp more than once. They are candidates for one BSS observation under
the timing/tie-break contract unless retained source evidence proves
otherwise. Adding SSID, TA, capture source, or AP name to the normal
per-BSSID grouping key is unnecessary under the owner-supplied project
assumptions. BSSID alone is not a complete duplicate key: it repeats in every
beacon from that BSS.

### C-7 — Required operator configuration

| Option | Practical meaning | Benefits | Risks |
|---|---|---|---|
| O-A — Current project mapping only | Require AP name-to-room configuration; derive BSSID/radio mapping automatically | Meets unattended/current-input requirement | Native captures without AP name may remain BSSID level |
| O-B — Mandatory device matrix | Require serial, BSSID, radio, and location mappings | Explicit topology | High setup/maintenance burden |
| O-C — Interactive adjudication | Ask the operator to resolve missing/conflicting identity during a run | Can repair unusual inputs | Prevents unattended analysis |

**Decision:** O-A.

No serial/BSSID/radio inventory or interactive mapping review is required.
The corrected analysis must write its generated AP/BSSID/radio map as an audit
artifact. A future optional static override can be considered if an actual
capture cannot be grouped correctly, but it is not required by this ADR.

An over-the-air capture with neither a decoded AP name, an optional explicit
capture/BSSID-to-AP association, nor a supported vendor same-AP pattern cannot
truthfully be assigned to an AP automatically. A vendor pattern may form an
anonymous AP group, but the existing AP-name-to-room mapping still cannot map
that group to a classroom. Under the accepted corrected mode, the software
must complete at BSSID level whenever a stronger association is unavailable.

### C-8 — Automatic behavior when fields are missing or disagree

| Option | Practical meaning | Benefits | Risks |
|---|---|---|---|
| G-A — Stop for complete physical identity | Fail whenever AP/radio grouping is unavailable | Strict | Rejects otherwise useful BSSID data and current unattended goal |
| G-B — Pause for user review | Ask how to map each issue | Flexible | Not unattended or reproducible |
| G-C — Deterministic fallback by result grain | Continue automatically at the most specific honest level and emit diagnostics | Completes supported inputs without inventing identity | Some results may be BSSID-only or lack room aggregation |

**Decision:** G-C.

For current supported Aruba inputs, the decoded AP name is available to
establish AP membership without user inspection. In the accepted corrected
mode, radio-specific evidence must produce radio groups and insufficient radio
evidence must fall back automatically to BSSID-level results. Legacy mode may
continue its characterized `radio_number` behavior only under the existing
comparison/version baseline. More generally, the corrected implementation
must:

1. prefer BSSID and fall back to TA if BSSID alone is missing;
2. prefer decoded AP name and fall back to an explicit capture/BSSID-to-AP
   association, then a supported vendor BSSID same-AP pattern;
3. prefer explicit radio metadata, use band/frequency/channel separation, and
   then a supported vendor radio-allocation rule;
4. if radio grouping remains unavailable, emit a BSSID-level result;
5. if AP/room association remains unavailable, emit the BSSID result as
   unmapped and omit only the affected AP/room aggregate; and
6. if a source row has neither BSSID nor TA, retain only its raw/source
   evidence and provenance; do not create a canonical beacon-analysis row for
   it, and emit a counted diagnostic rather than failing unrelated data.

No fallback may relabel a BSSID-level result or an inferred radio group as
hardware-proven physical-radio topology.

## Accepted decision summary

| Decision item | Accepted selection | What changes in practice |
|---|---|---|
| C-1 | E-A — Project-scoped configured identity | AP name is the AP; each resolved BSSID receives stable membership in one of that AP's generated radio groups for this project. No serial data. |
| C-2 | I-A — Deterministic natural IDs | Use AP name (or a namespaced anonymous fallback) and a whole-project radio number. No opaque registry/device table. |
| C-3 | Packet/configuration precedence | Prefer decoded AP name and BSSID; use optional AP association, vendor BSSID patterns, TA, band/channel, and supported radio rules only in documented order. |
| C-4 | S-B — Method/grain/diagnostics | Explain grouping with networking-oriented method labels and warnings, not confidence scores. |
| C-5 | V-A — Project-static mapping | Project truth gives one BSSID one AP/radio/SSID relationship; unresolved output stays null rather than inventing or duplicating a parent. Reuse/history is out of scope. |
| C-6 | M-A — BSSID primary | Group observations by BSSID, check TA, apply timing selection within each stream, and resolve supported BSSID-to-radio groups once project-wide. |
| C-7 | O-A — Current mapping only | Existing AP-name-to-room configuration is sufficient for current supported Aruba inputs; no device matrix or interactive review. |
| C-8 | G-C — Automatic scoped fallback | Corrected mode must complete current supported input unattended; radio-specific evidence produces radio groups and unresolved membership remains clearly labeled BSSID-level output. |

## Decision

Accepted by the project owner on 2026-08-26: C-1 E-A, C-2 I-A, the C-3
networking-field precedence, C-4 S-B, C-5 V-A, C-6 M-A, C-7 O-A, and C-8 G-C,
with the topology and metric-grain clarifications in this revision.

Acceptance requires no hardware serial/topology inventory or device matrix.
Cross-vendor support still requires representative evidence for each claimed
adapter's AP-name mapping, BSSID pattern, and radio-group rule.

## Timing evidence and deferred decisions

The owner supplied two timing clarifications during this review:

1. The owner's operational expectation is that Aruba ERM time is stamped
   closer to the capture source. The accepted authority, however, rests on the
   exact raw `@aruba_erm.time` source contract rather than a proven clock-
   proximity claim; rendered `aruba_erm.time` remains compatibility evidence.
   `frame.time_epoch` is a capture-clock timestamp and, for ERM, timestamps the
   outer encapsulating frame. Revised ADR-0008 uses it when raw Aruba time is
   unavailable or unusable; native/over-the-air input uses it directly.
2. `wlan.fixed.timestamp` is the beacon Timestamp/TSF value visible in current
   TShark. Using the decoded beacon interval (`wlan.fixed.beacon`) with it is a
   design inference that may identify beacon order, duplicate views, and
   skipped intervals. When it progresses normally, revised ADR-0009 also maps
   it to absolute transmitter time through a per-BSSID/session `min(O-F)`
   origin and uses that time for bin placement. Missing, frozen, reversing,
   insufficient, or grossly inconsistent TSF instead uses observation time for
   the complete BSSID/session without discarding otherwise valid beacons.

Current conversion extracts neither `wlan.fixed.timestamp` nor
`wlan.fixed.beacon`. TSF is relative to a BSS/radio timing origin and has no
calendar date, so observation time remains necessary for scheduled-class
admission and anchoring. The resulting lower-envelope origin is best effort;
it does not prove literal chassis boot time.

This is not an ADR-0003 identity decision. The deferred ownership is:

- [accepted ADR-0008](0008-aruba-capture-timestamp-authority-and-fallback.md)
  for Aruba-before-frame observation-time precedence; and
- [accepted ADR-0009](0009-tbtt-tsf-beacon-slot-and-bin-semantics.md) for
  per-BSSID/class-session TSF reliability, origin derivation, bin placement,
  ordering, duplicate recognition, skipped-interval diagnostics, and fallback.

ADR-0005 accepts class-start-anchored analysis bins and first-observation
selection without a rolling gate. Revised ADR-0009 does not replace that grid;
it selects reliable TSF-derived time or fallback observation time as the
coordinate placed on it. This ADR still makes no timing decision and does not
change either timing contract.

## Analysis-semantic impact

intentional change

Acceptance of this ADR changes documentation only. Future implementation
intentionally changes identity handling by using BSSID instead
of TA as the primary BSS stream key and by assigning radio groups once per
project rather than once per AP/class subset. The current fixture has TA equal to
BSSID, so that key change should be behavior-preserving there; project-wide
radio grouping can still change aggregation when a class slice omitted a
BSSID.

Any changed radio counts, station totals, CU aggregation, streaks, plots, or
summaries require a separate metric version under ADR-0006. Observation-time
and TSF-selection changes have their own intentional-semantic-change gate.

## Data, provenance, output, and compatibility impact

This decision changes no current schema or artifact. A future design must be
able to represent or reference:

- project scope/version;
- normalized configured-name or adapter-namespaced anonymous-fallback `ap_id`;
- normalized BSSID and observed/fallback TA;
- project-wide `radio_group_id`/radio-group number;
- per-BSSID frequency/channel/band evidence, optional explicit radio
  designation, and any comparable supporting RSSI evidence;
- AP identity and radio grouping method;
- result grain (`ap_radio_group` or `bssid`);
- stable diagnostics for violated project assumptions;
- capture-source/adapter profile; and
- the generated AP/BSSID/radio mapping used by the run.

No hardware serial, opaque registry ID, membership validity interval, or
operator-maintained device matrix is required. Exact column/manifest placement
remains ADR-0002 physical-schema work. Outputs grouped differently require a
new metric version under ADR-0006.

## Migration and rollback

The later owner-approved output direction supersedes the original parallel
dual-run proposal. PR-0040 validates normalized AP name, BSSID-primary stream
grouping, and whole-project radio grouping against the accepted corrected
oracle and current-Aruba evidence. It does not maintain or run a production
legacy grouping path.

The generated mapping remains a reviewable output rather than a setup input.
The public
[Original release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original)
remains the historical workflow reference when its original results must be
reproduced; long-term comparisons should otherwise rerun the current metric
definition over older captures.

## Required tests and acceptance evidence

### Acceptance evidence

- Owner confirmation that AP names, room mappings, BSSIDs, radio count, and
  BSSID ownership are stable within a project.
- Current fixture evidence that TA equals BSSID and each BSSID maps to one
  SSID.
- Current golden behavior as the baseline for Aruba radio grouping.
- Owner acceptance of C-1 through C-8 and the added hierarchy/metric-grain
  clarifications on 2026-08-26.
- No hardware inventory or multi-year replacement fixture is required for ADR
  acceptance because those claims are out of scope.

### Future implementation tests

- AP-name normalization, case policy, uniqueness, and filename-sanitization
  collision cases.
- Direct vendor AP name, optional explicit configured-association fallback,
  disagreement diagnostic, and missing-name BSSID-level behavior.
- Same-name BSSIDs joining one AP; unnamed BSSIDs joined by a named/versioned
  vendor BSSID AP-pattern rule into a deterministic anonymous AP group with no
  invented name/classroom; and similar-looking BSSIDs with different AP names
  remaining separate.
- BSSID/TA equality, mismatch with BSSID precedence, missing-BSSID TA fallback,
  and missing-both raw-evidence-only diagnostic with no canonical beacon row.
- Exact and mixed-source duplicate BSSID observations proving that BSSID
  partitions the stream while the separately accepted timing/tie-break rule
  selects retained observations.
- One BSSID to one AP/radio/SSID for the project and deterministic diagnostics
  when that invariant is violated.
- Multiple BSSIDs/unique SSID instances grouped to one AP radio by a supported
  radio-specific rule; repeated SSID text forcing distinct radio groups under
  the same AP.
- Radio grouping generated from the full project producing the same IDs in
  every class subset and under different row/file/partition order.
- Explicit radio metadata taking precedence over a vendor BSSID rule.
- Different contemporaneous band/channel/frequency evidence separating radios,
  including multiple same-band radios under one AP and channel-change cases
  that do not split a stable BSSID identity.
- Comparable RSSI within approximately 3 dBm retained only as supporting
  evidence, including a false-similarity case from different radios.
- One selected observation per BSSID/bin; one non-summed CU and admission-
  capacity value per radio/bin with a conflict diagnostic; and station count
  retained once per selected BSSID/bin and summed at radio, SSID/classroom,
  and band/classroom grain without applying CU-source preference.
- No-radio-rule fallback producing a labeled BSSID-level result without
  interactive input, a null `radio_group_id`, and any already resolved AP/room
  fields preserved.
- AP/room-unmapped input preserving BSSID-level results with null parent IDs
  while omitting only the unsupported aggregate.
- A clean unattended run from the existing tracked inputs with no serial or
  device-matrix file.
- Ordinary legacy regression remains unchanged while the separately reviewed
  corrected grouping oracle and PR-0040 integration suite validate the new
  component boundary; no parallel legacy golden pipeline is required.
- Representative capture/adaptor tests for each additional vendor/source
  profile claimed supported.

Per-BSSID TSF ordering, duplicate, skipped-interval, and observation-time
fallback tests belong to the separate timing decisions, not ADR-0003
acceptance.

### Shared resolver implementation update — 2026-09-22

The corrected resolver is now vendor-neutral. `radio_resolver.py` groups
configured observations by AP, band, and overlapping channel history, retains
one project mapping per BSSID, checks the unique-SSID-per-radio rule, and falls
back to BSSID grain on incomplete or conflicting evidence. Aruba and Extreme
use the same resolver without MAC-pattern inference or corroboration.

Owner 5 GHz and 6 GHz Extreme captures provide a common-AP oracle:
`P-LVES-B105R22S30-AP-01-WN` has three BSSIDs on 5 GHz channel 44 and one BSSID
on 6 GHz channel 81. The resolver produces two radios under the same AP and
keeps all three channel-44 BSSIDs together. This implements the accepted
band-plus-channel-within-AP rule without a hardware inventory.

## Consequences and risks

### Benefits

- Matches how this project is operated and keeps current inputs sufficient.
- Uses AP name and BSSID in their practical networking roles.
- Removes unnecessary serial inventories, opaque IDs, validity histories, and
  review workflows.
- Makes radio numbering consistent across class sessions.
- Allows useful over-the-air/BSSID analysis even when optional AP-name or
  radio metadata is missing.
- Preserves method/grain diagnostics without requiring the operator to inspect
  every run.

### Costs and risks

- AP rename, replacement, movement, BSSID reuse, or radio reconfiguration
  inside one project violates the contract and is not reconciled
  automatically.
- BSSID is treated as unique within the project, not guaranteed unique for all
  networks and time.
- AP-name information is vendor-specific and optional; raw over-the-air input
  without an optional explicit AP association cannot support AP/room
  aggregation.
- BSSID does not reveal which peer BSSIDs share a physical radio; each vendor
  grouping rule still needs explicit documentation and representative tests.
- Until a topology fixture exists, an automatically derived radio group is an
  operational project grouping, not proof of a hardware serial/slot.
- Falling back to BSSID grain preserves analysis but may report more logical
  BSS results than physical radios.

## Deferred implementation and metric decisions

These follow-ups do not reopen the accepted project hierarchy:

1. ADR-0002 physical-schema work must choose AP-name normalization/case rules
   while preserving the administrator-assigned display value.
2. Each additional vendor adapter must name and test its AP-name field and any
   radio-specific allocation rule. When that AP name is usable, MAC allocation
   patterns are unnecessary. Supporting absent or conflicting AP names is
   separate future work. Aruba and Extreme now have reviewed AP-name and
   radio-resolution evidence.
3. P06 must implement per-BSSID band classification and specify how
   channel/frequency changes are distinguished from multiple same-band radios.
4. ADR-0004 decides CU resolution and now records the accepted station-count
   rollup grain. Station-count missing-member/partial-total behavior, staleness,
   and final output placement remain follow-up work, as does admission-capacity
   missing/conflicting resolution.
5. ADR-0009 implementation must validate per-BSSID TSF ordering, same-BSSID/
   same-TSF duplicate handling, skipped-interval diagnostics, unusable-TSF
   continuation through observation time, and representative/tie selection.
6. ADR-0002/ADR-0006 follow-up work must place the accepted IDs, methods,
   diagnostics, selected-record lineage, and metric version in durable outputs.

### Station-count follow-up resolution — 2026-10-03

ADR-0010 resolves item 4's station-count missing-member, partial-total,
staleness, and initial placement questions. Admission-capacity
missing/conflicting resolution remains open.
