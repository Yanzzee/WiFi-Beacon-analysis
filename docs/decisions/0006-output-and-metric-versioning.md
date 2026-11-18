# ADR-0006: PR-0011 schema and metric versioning gate

- Status: Accepted
- Decision: Accepted within the narrow PR-0011-enabling scope
- Date proposed: 2026-07-30
- Date decided: 2026-07-31
- Owners/reviewers: Project owner
- Roadmap items:
  [PR-0010 and PR-0011](../post_refactor_roadmap.md)
- Supersedes: None
- Superseded by: None

> Current implementation status (2026-10-02): versioned manifests and the
> five durable corrected artifacts are active. The temporary legacy fixture
> profile and parallel runtime discussed below have been retired; one frozen
> explanatory comparison remains under `tests/golden/`.

## Scope

This ADR is deliberately limited to the minimum versioning and validator
decisions required before PR-0011 can become `Ready`. It decides:

1. separation of canonical schema and analysis metric versions;
2. stable identifiers for the first canonical contract and the test-only
   legacy Aruba fixture profile;
3. recognition of unversioned tracked legacy fixtures;
4. validator outcome meanings;
5. persistence of PR-0011 diagnostics;
6. the no-output/no-metric-change constraints on PR-0011; and
7. the temporary profile's purpose and removal gate.

Acceptance of this ADR closes the versioning decision dependency only. It does
not implement PR-0011 or determine readiness without the repository evidence
review recorded in the roadmap.

## Historical context before the accepted refactor

At proposal time, the pre-refactor pipeline had no canonical schema identifier
or explicit metric version. Its grouped inputs and analysis outputs used
Aruba-shaped names. The implemented versioned workflow is now documented in
[`current_pipeline.md`](../current_pipeline.md); this context preserves the
reason the accepted versioning contract was needed.

The tracked analysis-forward fixture is unversioned and begins after capture
conversion/grouping. It is valuable regression evidence, but it lacks:

- stable source-capture, source-record, and frame identifiers;
- adapter/run/tool version metadata;
- explicit value-state/transformation evidence; and
- usable frequency evidence.

[ADR-0002](0002-canonical-beacon-schema.md) classifies this data as a
**test-only legacy Aruba profile**, not a permanent supported user input or
public output contract.

PR-0011 is a validation-only behavior-preserving improvement. It is not a
canonical data migration. The roadmap requires both ADR-0002 and ADR-0006 to
be Accepted before PR-0011 may become `Ready`. Both decision dependencies are
now accepted; the separate repository readiness review is recorded in the
[roadmap](../post_refactor_roadmap.md).

## Decision drivers

1. A schema representation change and an analysis-metric meaning change are
   different compatibility events.
2. Unversioned legacy fixtures must remain usable for regression without being
   relabeled as fully canonical.
3. A filename/column-shape resemblance must not silently turn arbitrary user
   data into a supported legacy profile.
4. Validator outcomes must distinguish complete conformance from deliberately
   incomplete regression evidence.
5. PR-0011 must not change active analysis behavior, outputs, fixtures, or the
   existing T3 fingerprint.
6. The temporary legacy profile needs an objective retirement gate.
7. Broader migration/public compatibility decisions should not block a narrow
   validation-only conformance layer.

## Options considered

### Schema and metric version relationship

| Option | Description | Strengths | Risks |
|---|---|---|---|
| V-A — One shared version | One number changes for either schema or metric changes | Simple label | Conflates storage/contract evolution with changed analysis meaning; creates unnecessary metric changes for additive schema work |
| V-B — Independent versions | Canonical schemas and metric definitions have separate identifiers | Makes compatibility and semantic review explicit; allows schema-only validation without changing metrics | Requires every artifact/report boundary to carry the correct pair when both apply |
| V-C — Schema version only | Version data contracts but leave metric meaning implicit | Smaller immediate scope | Does not satisfy later intentional-semantic-change gates |

**Decision:** select V-B. Schema versions and metric versions are independent.
PR-0011 validates schema/profile compatibility without changing or assigning a
new active metric version.

### Stable identifier format

| Option | Description | Strengths | Risks |
|---|---|---|---|
| I-A — Bare integer | Use values such as `1` | Compact | Ambiguous without knowing whether it names a schema, metric, or profile |
| I-B — Semantic version only | Use values such as `1.0.0` | Familiar ordering | Still ambiguous without a namespace; patch/minor meaning is not yet defined |
| I-C — Namespaced stable identifier | Use a contract/profile family plus explicit version | Self-describing and resistant to cross-domain confusion | Longer; namespace/version allocation rules must remain stable |

**Decision:** select I-C with these stable identifiers:

```text
beacon.schema/canonical-observation/v1
beacon.test-profile/legacy-aruba-grouped/v1
```

The first identifier names the accepted canonical observation contract,
not a physical Parquet schema that ADR-0002 deliberately deferred. The second
identifier must visibly remain a test profile; it must never be shortened or
aliased into a production support claim.

This ADR does not assign a new identifier to the current metric behavior.
Whether current unversioned metrics later receive a legacy identifier belongs
to the broader metric/output migration decision.

### Recognition of unversioned legacy fixtures

| Option | Description | Strengths | Risks |
|---|---|---|---|
| R-A — Shape auto-detection | Any unversioned data matching Aruba columns is assigned the legacy profile | Convenient | Can misclassify user data and creates an accidental supported-input contract |
| R-B — Explicit test-harness selection | Tests explicitly select the profile and then validate expected fixture identity/shape | Cannot silently claim support; keeps profile test-only | Requires explicit fixture configuration |
| R-C — Rewrite fixture metadata | Add a profile identifier to tracked Parquet files | Makes the fixture self-identifying | Violates PR-0011's no-fixture-rewrite constraint and changes baseline data |

**Decision:** select R-B. The test harness must explicitly select
`beacon.test-profile/legacy-aruba-grouped/v1`, then validate the known
fixture boundary and required shape. Matching columns alone must not activate
the profile for arbitrary inputs. The fixture itself remains unmodified and
unversioned.

### Validator outcome meanings

The following definitions are accepted:

| Outcome | Accepted exact meaning |
|---|---|
| `exact` | The input explicitly identifies the requested canonical contract/profile and satisfies all invariants and capabilities required by that exact target |
| `compatible` | The input explicitly identifies an accepted contract/profile that is declared compatible for the requested validation scope and satisfies that scope |
| `legacy-incomplete` | The explicitly selected test-only legacy profile satisfies its regression boundary but lacks one or more canonical capabilities/provenance fields that cannot be reconstructed honestly |
| `unsupported` | The target/profile is unrecognized, required invariants fail, or the selected capability cannot be validated; processing must not present the input as compatible |

`legacy-incomplete` is not a weaker form of production support. It exists to
describe known regression evidence without fabricating canonical provenance.

Potential parse/validation errors inside a recognized target may need more
specific error codes, but PR-0011 does not need a broader warning taxonomy to
decide these four top-level outcomes.

### Diagnostic persistence

| Option | Description | Strengths | Risks |
|---|---|---|---|
| D-A — Internal/test-only | Validator results exist only in test assertions, logs, or ephemeral in-memory diagnostics | Preserves PR-0011's no-published-output contract; avoids prematurely versioning reports | Not a durable user-facing audit artifact |
| D-B — Durable versioned report | Persist a validation artifact with its own schema/version | Supports later audit/UI use | Creates a new output contract, lifecycle, and versioning obligation outside narrow PR-0011 scope |

**Decision:** select D-A for PR-0011. Diagnostics are internal/test-only unless
a later accepted decision creates a durable, versioned validation artifact.

### Temporary profile removal gate

The profile exists to validate the current grouped fixture while the production
canonical path is built from original inputs. Candidate removal gates are:

| Option | Description | Risk |
|---|---|---|
| G-A — Time-based | Remove after a date or release count | Can retire evidence before replacement is proven |
| G-B — Code-path-based | Remove when canonical adapter code exists | Implementation alone does not prove equivalence |
| G-C — Evidence-based | Remove after original-input Aruba-to-canonical equivalence passes the required regression tiers | Requires retained original inputs and potentially expensive validation, but proves replacement |

**Decision:** select G-C. The legacy profile may be retired only after the
original-input Aruba canonical path passes the applicable T4
short-capture checkpoint, T5 full pre-analysis checkpoint, and T6 complete
pipeline comparison, with the T3 baseline unchanged until the replacement is
accepted.

## PR-0011 invariant constraints

This decision requires PR-0011 to:

- change no active metric version;
- change no published schema;
- change no output filename;
- rewrite no tracked or local fixture;
- migrate no data;
- create no permanent supported-input contract for Aruba grouped files; and
- leave the existing T3 golden fingerprint unchanged.

PR-0011 may validate compatibility and report missing capabilities internally.
It must not convert `legacy-incomplete` data into canonical data by filling
unavailable provenance.

## Broader topics deferred at acceptance

A later ADR must own:

- physical canonical schema layout and permanent metadata placement;
- public Parquet/CSV schema-version placement;
- durable validation-report schema and retention, if later selected;
- compatibility duration and migration tooling for public outputs;
- legacy/current/corrected metric coexistence and naming (production
  coexistence was later rejected by the owner direction below);
- output filename/version conventions;
- canonical-data backfill or grouped-data migration;
- alias/public-export deprecation policy;
- release-level version negotiation; and
- rollback across persisted schema/metric versions.

Deferring these topics does not permit PR-0011 to choose them implicitly.

## Decision

Accepted within the narrow PR-0011-enabling scope:

1. canonical schema versions and analysis metric versions are independent;
2. `beacon.schema/canonical-observation/v1` and
   `beacon.test-profile/legacy-aruba-grouped/v1` are the stable identifiers;
3. the unversioned legacy fixture is recognized only through explicit
   test-harness profile selection;
4. column-shape auto-detection must never assign that profile to arbitrary
   user data;
5. `exact`, `compatible`, `legacy-incomplete`, and `unsupported` have the
   accepted meanings above;
6. PR-0011 diagnostics are internal/test-only;
7. PR-0011 changes no active metric version, published schema, output
   filename, fixture, data, permanent grouped-Aruba input contract, or T3
   golden fingerprint;
8. the temporary profile may be removed only after original-input
   Aruba-to-canonical equivalence passes the applicable T4, T5, and T6 gates;
   and
9. public migration, durable validation reports, output-version placement,
   compatibility duration, aliases, and rollback remain deferred to a later
   ADR.

## Analysis-semantic impact

behavior-preserving

This accepted versioning gate does not change a metric definition or current
analysis behavior. Its accepted separation explicitly prevents a
schema-validation change from masquerading as a metric-version change.

## Data, provenance, output, and compatibility impact

No data, fixture, output, or schema change occurs from accepting this ADR.
PR-0011 uses only internal/test validation metadata and leaves all current
persisted artifacts unchanged.

The legacy profile identifies regression evidence; it does not create a
production compatibility promise.

## Migration and rollback

PR-0011 performs no migration. Broader migration and rollback remain deferred
to a later ADR.

The only retirement rule evaluated here is the evidence gate for removing the
test-only validation profile after original-input canonical equivalence has
replaced its regression role.

## Post-acceptance owner direction — 2026-09-03

The project owner clarified the broader migration goal without changing the
narrow PR-0011 decision above:

1. The public
   [Original release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original)
   is the retained reference for the original refactored workflow and
   published calculations.
2. The production application will not maintain, execute, or persist a
   complete parallel legacy metric pipeline merely for compatibility.
3. Long-term comparisons should rerun the current metric definition over older
   captures. The retained commit is used only when the historical workflow
   itself must be reproduced.
4. A specifically useful comparison metric may remain in the new paradigm
   without implying complete legacy compatibility. ADR-0004's connected
   high-CU streak is such a metric and is now described as a possible-connected
   streak.
5. Every implemented output or metric change from the retained reference must
   be identified in the root README and the maintained
   [`output_inventory.md`](../output_inventory.md).

This direction resolved the need for production legacy/current/corrected
coexistence, a compatibility duration, and runtime rollback: none is required.
The corrected metric identifier, physical artifact paths, table boundaries,
and durable version/configuration placement were subsequently accepted and
implemented through PR-0024.

### PR-0024 logical artifact approval — 2026-09-04

The owner approved five durable logical artifacts for the corrected paradigm:

1. selected observations at one row per BSSID/class bin;
2. radio/class summaries;
3. classroom/class summaries;
4. an automatically generated BSSID-to-radio-to-AP-to-classroom project
   identity map; and
5. a small run manifest.

CSVs and plots are derived exports. The identity and analysis artifacts retain
2.4/5/6 GHz band and channel evidence where applicable, and classroom radio
counts are reported per band with a derivable total. The run manifest includes
requested and actual selected sample time ranges, concise stage logging, and
references to any detailed logs.

The potentially large selected-observation table is one logical dataset that
may be physically partitioned by time. Its design must remain workable if a
future metric version selects 102.4-millisecond bins, but that smaller bin size
requires a representative performance/storage benchmark before acceptance.

This approval fixes the logical boundaries but not exact filenames, directory
layout, physical schema, partition sizing, corrected metric identifier, or
version/configuration field placement. PR-0024 remains `Needs decision` until
those small physical choices and the corrected-version golden contract close.

The five artifacts are logical and versioned contracts, not permanently
closed column lists. Later reviewed features may add fields, such as new
selected-observation evidence, without creating another artifact. Compatible
additions retain the schema contract according to its evolution rules;
incompatible meaning or type changes require a new result-schema version and
must be identified in the manifest and output inventory.

### PR-0024 physical contract approval — 2026-09-09

The owner accepted the small physical choices that complete PR-0024's
documentation gate:

- corrected metric ID: `beacon.metric/corrected-cu/v1`;
- result schema ID: `beacon.schema/analysis-results/v1`;
- one `run_id` associates all artifacts, while the manifest is authoritative;
- artifacts live under `output/runs/{project_slug}/{run_id}/` as `manifest.json`,
  `project_identity_map.parquet`, a `selected_observations/` Parquet dataset
  partitioned by `class_date`, `radio_class_summary.parquet`, and
  `classroom_class_summary.parquet`;
- timestamps use UTC nanosecond instants, TSF uses unsigned microseconds,
  durations use signed integer microseconds, raw CU uses nullable unsigned
  bytes, and coverage/averages use unrounded float64 ratios;
- tied responsible radios use a Parquet `list<string>` rather than a separate
  relationship table;
- valid Aruba ERM time precedes frame time; within the same timestamp-source
  class, caller-declared capture-source rank resolves overlapping-source
  precedence, then duplicate-view selection uses earliest preferred
  observation time and stable source-path/frame order; and
- the manifest retains concise stage status/counts and an optional detailed-
  log reference, not embedded unbounded log text.

### Run-manifest foundation implementation — 2026-09-23

`run_manifest.py` implements the accepted `manifest.json` boundary without
activating the unfinished corrected runner. It validates run/schema/metric
identity, requested and actual ranges, effective configuration, source
precedence, storage conventions, stages, artifact references, and the absence
of legacy artifacts. Each real capture plan records the input path/size/hash,
detected profile, TShark version, exact field list, beacon filter, adapter
versions, and a deterministic plan hash.

The loader rejects duplicate JSON object keys; the corrected oracle's repeated
`detailed_log` key was removed. The writer uses a unique temporary file and
atomic replacement, refuses to overwrite an existing run manifest, and cleans
up the temporary file on failure. Project/run-ID generation, aggregate input
hashing, whole-run staging/publication, and stage orchestration remain PR-0059
implementation work; their initial lifecycle policy is accepted below.

### First corrected report-package implementation — 2026-09-29

The owner approved the bounded CU, coverage, and streak package documented in
[`corrected_cu_report_package.md`](../corrected_cu_report_package.md). The
radio/class and classroom/class artifacts now include authoritative UTC
`class_start` and `class_end` fields. This is an additive evolution of the
accepted logical artifacts; reports never infer boundaries from an identifier.

Each successful run derives three human-readable CSVs, a concise run-summary
file, a comprehensive methodology/file guide, and eight project-level PNGs
under `reports/cu_coverage_streak/`. The run summary records the key scope,
input-normalization, selection, identity, band, summary-row, elapsed-time, and
warning/error results that are also useful in terminal progress output. The radio/class
artifact has an additive fixed 256-value `valid_cu_raw_counts` field populated
from the already selected radio/bin CU values. The resulting histogram and CDF
therefore count each radio/time bin once rather than counting duplicate BSSID
or capture-source views. Time-histogram buckets align to the configured
analysis-bin interval, and average-CU buckets span two raw CU units. CU CDFs
label the cumulative share at the 50% and 75% thresholds. Histogram bars use
their full bucket width, and paired classroom streak distributions share a
common x/y scale. These files are
manifest-linked derived exports, not another authoritative artifact or metric
calculation path. They are produced before atomic publication, so an
incomplete report package prevents the run from publishing.

The project identity artifact also has an additive nullable `ap_model` field,
copied from the configured AP CSV when supplied. The derived radio/class
review CSV adds the model, a same-band radio count calculated from resolved
identity, and a readable cohort label. The CU-versus-coverage figure separates
bands into panels and colors those cohorts. Model does not resolve radios;
same-band radio count is not inferred from model; and neither field changes
selection, metric weighting, or missing-data treatment. This is a compatible
schema addition under the approved open-column logical contract, so the v1
schema and metric identifiers remain appropriate.

The minimal PR-0045 addition derives a three-row band-population CSV and
separate 2.4/5/6 GHz radio/bin histogram and CDF panels. It reuses the existing
identity map, radio/class summaries, and fixed raw-CU count vectors. The
populations are all 5 GHz radios, the overlapping subset of 5 GHz radios on
APs with a resolved 6 GHz radio, and those APs' 6 GHz radios. Empty populations
remain explicit. This adds presentation rollups only and does not change the
accepted corrected metric or schema identifiers.

### Initial corrected-run lifecycle approval — 2026-09-23

The owner selected a simple create-only lifecycle for the first corrected
runner:

- every execution receives a new automatically generated `run_id`, even when
  its inputs are identical to a previous run;
- a user-supplied project name is retained prominently and represented by a
  path-safe `project_slug`, so related runs live below
  `output/runs/{project_slug}/`;
- the run directory name uses UTC date/time with microsecond precision plus an
  eight-character prefix of the aggregate input-set SHA-256, for example
  `20260923T184530123456Z-4f9a2c1d`;
- the manifest retains the original project name, the path slug, the full
  aggregate input-set SHA-256, the individual input hashes, effective
  configuration, and available code-version provenance;
- a final-directory collision is a clear failure. The runner does not replace
  or resume a run, and a rerun always creates another run directory;
- all artifacts are first built below the same project directory in the
  runner-owned hidden sibling `.{run_id}.staging/`. The manifest is written
  last, after artifact validation, and the complete directory is then renamed
  atomically to `{run_id}/`;
- a handled failure is logged and its staging directory is deleted. A process
  interruption may leave staging data; the next invocation removes only
  staging directories carrying the runner's ownership marker after recording
  that cleanup; and
- small success/failure logs live below the project directory's `_logs/`
  folder and are not analysis artifacts. Deleting the project directory also
  deletes its runs, staging data, and logs.

The aggregate input-set digest is provenance, not the run's unique identity.
It is computed deterministically from the role, normalized input reference,
size, and full content SHA-256 of every analysis-defining input file. The UTC
timestamp makes repeated executions distinct; the hash prefix makes runs over
the same inputs recognizable. Configuration and code provenance remain
separate manifest facts, so changing either still creates a new run without
mislabeling the input digest.

### Run-lifecycle utility implementation — 2026-09-23

`run_lifecycle.py` implements the accepted create-only boundary without yet
orchestrating analysis stages. It converts the display project name to a
portable slug, streams SHA-256 calculation for large inputs, computes the
deterministic aggregate input-set hash, generates the UTC-microsecond run ID,
records Git revision/dirty state when available, and creates the owned hidden
staging directory and project-local JSON-lines log.

Staging ownership uses a versioned marker containing project/run identity and
the creating process ID. A later invocation removes only a correctly marked
staging child whose recorded process is no longer running; unknown directories,
links/junctions, and active runs are left untouched. Handled failures are
logged and deleted immediately. Publication requires a valid matching
manifest and existing in-run artifact references, removes the staging marker,
and atomically renames the whole staged directory to its final run name.

`run_manifest.py` now requires and validates project name/slug, the full
aggregate input record, and code provenance. It recomputes the aggregate hash
from the manifest's ordered input facts so a stale or edited digest cannot be
accepted. Its writer can target either the ordinary run-root layout or a
lifecycle-owned staging directory.

### Selected-observation Parquet writer implementation — 2026-09-23

`selected_observation_output.py` implements the accepted
`selected_observations/` artifact without activating the corrected runner. It
validates the one-row-per-class/BSSID/bin key, source lineage, timing method
evidence, bin durations, and nullable QBSS ranges. It writes the accepted UTC
nanosecond timestamps, unsigned-microsecond TSF, nullable unsigned QBSS values,
and other durable fields with an explicit Arrow schema.

`class_date` is the UTC calendar date of `bin_start`. Non-empty datasets use
Hive-style `class_date=YYYY-MM-DD` directories, with rows ordered by class,
BSSID, and bin inside each date. Publication is atomic, refuses overwrite, and
cleans temporary data after failure. Focused tests compare the complete
accepted oracle column set, validate the physical schema and partition layout,
exercise a typed zero-row dataset, and verify validation/cleanup behavior.

### Corrected-output runner integration — 2026-09-24

`corrected_run.py` connects the create-only lifecycle, all four accepted table
writers, and final manifest through one programmatic run object. It accepts
already resolved/selected in-memory tables and an expected class-bin grid;
capture extraction, identity resolution, timing selection, and a public CLI
remain separate stages rather than being duplicated here.

The runner hashes the declared inputs before creating staging, records code and
capture-plan provenance, writes all four tables directly into the owned staging
directory, requires every selected BSSID to occur in the project identity map,
and calculates the manifest's actual selected-observation range. Known
validation/write failures are logged and cleaned; unexpected failures remain
owned staging for the next-run recovery rule.

Final publication is deliberately gated on the exact four accepted table
artifacts. The runner cannot publish only the identity map and selected
observations. The accepted ADR-0004 writer now calculates and stages both
summary tables from the expected bins, identity map, and selected observations;
arbitrary pre-existing summary files can no longer be registered through the
runner. Once all four tables exist, the runner writes the authoritative
manifest into staging and atomically publishes the whole run. This preserves
the approved five-artifact contract during incremental implementation.

The corrected oracle is
[`tests/golden/corrected_semantics_v1.json`](../../tests/golden/corrected_semantics_v1.json).
It compares logical content rather than Parquet or plot bytes and has no
parallel legacy golden pipeline. These choices define future outputs; they do
not create runtime artifacts or activate corrected calculations by
themselves. PR-0024 is now `Documented`.

<a id="corrected-capture-to-run-integration--2026-09-26"></a>

### Corrected capture-to-run integration — 2026-09-26

`corrected_capture_run.py` now supplies the first complete programmatic path
from reviewed raw captures to the accepted five-artifact publication. It hashes
the captures, AP mapping, and normalized class-session source file; starts a
unique create-only run; performs one dynamic TShark extraction per capture;
dispatches individual beacons through the reviewed Aruba and Extreme adapters;
discards observations outside configured AP hostnames; resolves BSSID, radio,
AP, band/channel, and classroom identity across the project; selects one
observation per BSSID/bin for each class; calculates the accepted ADR-0004
summaries; and atomically publishes only after every artifact succeeds.

Capture extraction was separated from the legacy per-capture Parquet wrapper.
`extract_capture_beacons()` returns the normalized in-memory rows plus detected
capture profile, exact dynamic field plan, and TShark version. The existing
`process_pcapng()` command still calls that shared extraction and writes the
same legacy processed Parquet output, so the new corrected path does not need a
redundant intermediate file.

The manifest now records every capture's content hash, detected native or
Aruba ERM profile, exact filter/field plan, TShark version, adapter versions,
compact adapter dispatch counts, configured-scope counts, radio-resolution
counts, and the extraction/identity/selection/write stages. A known handled
failure is logged and its staging directory is removed; no result directory is
published. Unresolved configured BSSIDs fail before summaries because the
accepted first metric is defined at resolved radio grain.

Focused tests cover mixed Aruba/Extreme dispatch, two overlapping views of the
same Aruba beacons, deterministic source precedence, multiple classrooms and
class sessions, all five published artifacts, manifest provenance, and the
handled-failure/no-publication path. An optional real replay using portable
TShark 4.6.6 publishes one run from the owner type-0 Aruba capture and the
owner Extreme 5 GHz capture. This closes the narrow programmatic
capture-to-run lifecycle slice.

### Current sample-input adapter — 2026-09-26

The owner approved stable class-session identity from class start/end plus the
classroom identifier. `corrected_input_adapter.py` now loads the existing
`sample_aps.parquet` and `sample_classes.parquet` shapes. AP `Hostname` and
`Building-Room` pass into configured project scope. Class `location`,
`start_time`, and `end_time` become classroom and UTC class boundaries.

For a current sample row, `class_session_id` is the transparent composite
`Building-Room|UTC-start|UTC-end`. Thus classes with exactly the same start and
end remain distinct across rooms. Two rows with the same room and interval are
rejected because current input requirements say shared-room sections must be
combined before analysis. Already normalized corrected input may retain an
explicit unique ID. The run manifest records the detected AP/class input
profiles and whether the ID was derived or provided.

Focused tests cover concurrent equal-time classes in separate rooms,
same-room duplicate rejection, timezone-to-UTC conversion, explicit-ID
preservation, hostname stored as an index, current Parquet loading, and a full
corrected run using the current class-table shape. Direct read-only validation
of the tracked project inputs adapted 5 AP rows and 61 class sessions with 61
unique IDs. The public CLI/configuration contract, progress reporting, user
acceptance, and corrected-workflow cutover were completed afterward.

The later raw three-CSV runner normalizes shared-room source sections before
this adapter boundary: rows with the same classroom, start, and end become one
physical session, enrollment is summed, and the merge is recorded in preflight
output, the run log, and the manifest. Explicit normalized class tables retain
the duplicate rejection above.

### Direct source-CSV and initial run-command implementation — 2026-09-26

The corrected path now accepts the three user-maintained source CSVs directly.
`sample_processing.build_sample_tables()` exposes AP, classroom,
schedule-cleaning, recurring-date expansion, capacity, and timezone rules as
an in-memory boundary. The raw adapter rejects a malformed class period during
preflight and creates no intermediate sample Parquet files.

`run_corrected_capture_project()` hashes the raw AP, classroom, and schedule
CSVs together with every capture. The manifest identifies the raw schedule
profile and records its timezone and inclusive expansion dates. This makes the
input-set digest describe the files that actually define the run.

`beacon_analysis.py run` is the initial user-acceptance command. It implements
flag/environment/env-file precedence, default source-CSV names under the
sample folder, TShark and capture preflight, one consolidated plan and
confirmation, `--yes`, strict `--non-interactive` operation, useful exit
statuses, and end-to-end five-artifact publication. Progress reporting and
bounded multi-worker extraction were added later; this is now the supported
command.

### First full corrected project publication — 2026-09-27

Project `2025-test` successfully processed the owner's 80-capture, 49.37 GiB
corpus and atomically published run
`20260927T060904484327Z-97b6eeb5`. All six manifest stages completed without a
recorded warning or error. The result contains 34 resolved BSSIDs, 2,334,938
selected observations, 228 radio/class rows, and 61 classroom/class rows.

The first attempt exposed that configured-hostname scope was being applied
after project concatenation. It was safely interrupted and its owned staging
was removed. The runner now applies the already accepted PR-0036 scope rule
after each capture's adapter dispatch and retains only aggregate exclusion
counts for discarded rows. The successful run retained 7,764,399 of
118,307,935 supported rows. `KeyboardInterrupt` is also now logged as an
understood failed run and removes owned staging immediately.

Full timing, output-size, and sampled-memory evidence is recorded in
[`../benchmarks/corrected_full_project_2026-09-27.md`](../benchmarks/corrected_full_project_2026-09-27.md).
Owner artifact review and release cutover remain separate gates.

### Bounded working-set implementation — 2026-09-27

The owner approved replacing the project-wide configured-observation
DataFrame with a bounded hidden staging boundary. The implementation preserves
the five durable artifacts and accepted analysis meanings:

- all configured observations contribute distinct project-wide BSSID/AP/radio
  identity evidence, but repeated row-level evidence is not kept in memory;
- preferred observation time filters rows into owned hidden Parquet partitions
  for every applicable complete class session;
- each session retains all capture-source parts until TSF ordering,
  duplicate-view collapse, skipped-interval diagnostics, and first-observed
  bin selection run together;
- selection, ADR-0004 summary calculation, and final selected-observation
  writing consume one complete session at a time, with session parts written
  as row groups in one selected-observation file per class date; and
- the `.working` tree is removed before successful atomic publication and is
  removed with owned staging after an understood failure.

The manifest records compact-evidence, class-window row/part,
maximum-session-input, and selected-row counts. Temporary Parquet is operational working data, not a
sixth artifact. The corrected oracle, focused bounded-writer/identity tests,
ordinary suite, and representative Aruba/Extreme replay pass. The previous
full-project memory measurement predates this optimization; a bounded
full-project replay remains the next performance evidence gate before capture
extraction concurrency is considered.

### Compact review exports and bounded capture concurrency — 2026-09-29

The corrected publisher now derives three human-readable CSV files from the
staged authoritative Parquet tables: project identity, radio/class summary,
and classroom/class summary. They are written under `exports/csv/` inside the
owned staging directory, linked from their corresponding manifest artifact,
and published atomically with the five approved logical artifacts. They do not
recalculate metrics or create additional authoritative contracts. List-valued
columns use compact JSON arrays. Selected observations remain partitioned
Parquet by default because a complete CSV would be large and would lose useful
types.

The initial implementation accepted one or two extraction workers. The
bounded boundary now accepts 1-16: each worker extracts, normalizes, dispatches,
filters to configured AP hostnames, and writes uniquely named class-session
parts before releasing its capture-sized table. Only compact identity/count
metadata and per-substage timings return to the coordinator. Source rank stays
part of lineage and tie-breaking, so completion order does not change durable
results. Identity resolution, selection, summaries, exports, and publication
remain serialized. One worker remains the default pending 1/2/4/8 evidence.

Production extraction also pushes configured Aruba/Extreme AP-name matching
into the TShark beacon display filter. This is an optimization, not a new AP
identity rule: every emitted row still traverses the accepted adapters and
scope check, and all in-scope rows remain available to BSSID/session TSF
deduplication. Pre-filter outside-project and missing-name counts are not
computed and are recorded as unavailable. The accepted durable artifacts and
metric/schema identifiers do not change.

The representative three-capture comparison below describes the earlier
extraction-only worker boundary. It produced identical logical
fingerprints for all four authoritative tables. Two workers reduced complete
wall time from 709.8 to 664.1 seconds (1.069x) while sampled peak process-tree
RSS rose from 2.74 to 3.28 GiB. This supports opt-in use but not a default
change. See
[`../benchmarks/capture_workers_2026-09-29.md`](../benchmarks/capture_workers_2026-09-29.md).
It is the baseline for, not evidence about, the newer worker-local stage.

The subsequent eight-capture worker-local benchmark produced identical table
fingerprints at 1, 2, 4, and 8 workers. End-to-end speedups were 1.593x,
2.199x, and 2.466x respectively. Four is therefore the current recommended
opt-in; eight gained only another 10.8% and increased sampled peak RSS from
1.642 to 2.240 GiB. Because that five-AP workload discards much more than the
expected production project, one remains the default and sixteen is deferred.
See
[`../benchmarks/capture_workers_bounded_2026-09-29.md`](../benchmarks/capture_workers_bounded_2026-09-29.md).

## Required tests and acceptance evidence

PR-0011 must add focused T1 contract cases that establish the selected
meanings:

- schema-version and metric-version identifiers cannot be confused;
- the two accepted profile/contract identifiers parse and round-trip exactly;
- arbitrary unversioned Aruba-shaped data is not auto-classified as the
  test-only profile;
- explicit selection of the tracked fixture yields the selected legacy
  outcome without inventing missing provenance;
- each validator outcome has mutually exclusive truth-table cases;
- validation writes no published output and rewrites no fixture; and
- diagnostic handling follows the accepted internal/test-only policy.

PR-0011 acceptance later requires:

- the selected T1 contract/error tests;
- T2 ordinary behavior;
- unchanged T3 golden fingerprints; and
- evidence that no output path, schema, filename, fixture, or active metric
  version changed.

T4–T6 are retirement-gate evidence for the later original-input canonical path,
not PR-0011 implementation-acceptance requirements.

## Consequences and risks

### Benefits of the accepted direction

- Schema and metric compatibility remain independently reviewable.
- The legacy fixture can retain regression value without becoming a user API.
- Validator outcomes expose missing canonical capabilities honestly.
- PR-0011 stays small and cannot silently become a migration.
- A replacement must prove original-input equivalence before legacy validation
  support is removed.

### Risks

- Namespaced identifiers can become accidental public APIs if later documents
  reuse them without accepting this ADR.
- `legacy-incomplete` could be misread as partial production support unless
  every diagnostic preserves the test-only qualifier.
- Internal-only diagnostics may need later redesign for a CLI/UI.
- T5/T6 retirement evidence is expensive and depends on retained original
  inputs.
- Broader migration questions remain unresolved and require another ADR.

## Project-owner approval record

On 2026-07-31, the project owner accepted the nine decisions in this ADR's
narrow Decision section. That approval does not accept any topic listed under
Intentionally deferred broader topics and does not authorize PR-0011
implementation as part of this documentation task.
