# Alpha 0.2 full-run release-readiness checkpoint

Date: 2026-10-29; reconciled 2026-10-31. Status: full run and cached warning-fix
verification completed successfully. Final external-capture validation and
candidate closeout are recorded below. No release has been tagged or published.

## Owner scope confirmation

Existing Pearson r and Spearman rho satisfy relationship reporting; regression
R-squared is not required for Alpha 0.2. Beacon-loss calibration is deferred.
Cross-project/year comparisons remain manual. Other unresolved analytical
purposes are not implicitly authorized by this checkpoint.

## Requested run and preflight

- Project: `2025-full`.
- Captures: `data/captures`, 80 files, 53,008,216,128 bytes (about 49.37 GiB).
- Inputs: root `sample/Sample_APs.csv`, `Sample_classrooms.csv`,
  `Sample_schedule.csv`; hashes match the previous full-run inputs.
- Inclusive schedule dates: 2025-11-10 through 2025-11-14.
- Schedule/display timezone: `America/Denver`; date-specific offsets apply.
- Scope: 21 configured APs and 373 expanded physical class sessions.
- Eight capture workers, preferred CU SSID `eduroam`, radio/class plots enabled.
- TShark 4.6.6. Preflight passed; C: had about 128.7 GiB available and no other
  Beacon analysis was running at the process check.
- Regression before launch: 387 passed, 16 external-tool/short-capture tests
  deselected, 103.71 seconds.

The active `config.env` settings were switched from the football project to
these 2025 settings. The child CLI also receives explicit project/input/date/
worker/plot flags to preserve the requested selection. Previous football
settings remain commented for reference. No code semantics were changed.

## Measurement and completion record

Launch: 2026-10-29 21:31:45 UTC (15:31:45 America/Denver).

Audit directory:
`output/release_readiness/2025-full/20261029T213144Z/`.

- `analysis.log`: unbuffered CLI output, including final published run path.
- `audit.json`: running/final state, command, config hash, code provenance,
  complete CLI wall time, validation time, output/report-family storage,
  capture/session/selected-row counts, stage and per-capture timing records.

The hidden wrapper completed the CLI and its read-only completion audit. Run
`20261029T213233082897Z-3152b7cd` was atomically published beneath
`output/runs/2025-full/`. The full input-set hash matches the previous
`20261014T160532618192Z-3152b7cd` run. Logical identity/radio/classroom summary
fingerprints match exactly, excluding run IDs, and selected row counts match.
These are corrected-to-corrected checks, not an Original replay or a full
row-by-row comparison of the large selected-observation dataset.

| Completed-run measurement | Result |
|---|---:|
| CLI wall time, including preflight/input hashing | 18,146.15 s (5h 02m 26s) |
| Post-run validation | 2.52 s |
| Captures / selected observations | 80 / 6,511,028 |
| Classroom sessions / radio sessions | 373 / 691 |
| Radio/session plots | 691, all indexed paths present |
| Manifest references checked | 83 |
| Overview links | Passed |
| Complete run storage | 500,215,181 bytes (477.04 MiB), 778 files |
| Optional radio/session PNGs | 264,593,792 bytes (252.34 MiB) |
| Other output | 235,621,389 bytes (224.71 MiB) |
| Overview HTML | 205,423 bytes (200.61 KiB) |

The CLI finished at 2026-10-30 02:34:11 UTC (2026-10-29 20:34:11
America/Denver). All 15 recorded stages completed with zero stage errors or
stage-counted warnings; one library FutureWarning appeared in the console and
was subsequently fixed and verified as described below.

| Stage | Observed wall time |
|---|---:|
| Extraction/normalization | 3h 02m 41s |
| Class observation selection | 1h 20m 26s |
| Selected-observation writing | 17m 29s |
| Radio/bin package, including optional plots and streamed accumulations | 11m 19s |
| Station package | 9m 15s |
| Final band-comparison writing | 3.49 s |
| Final affected-station writing | 1.05 s |
| Offline overview | 1.90 s |

The final family-write timings exclude accumulation shared with preceding
streaming stages; they are not isolated total family costs. Representative
5/6 GHz radio plots were visually checked and overview links were rechecked.

CLI wall time includes preflight and input hashing; post-run audit time is
separate. No controlled CPU/disk utilization trace, runtime SLA or isolated
incremental cost is claimed. Published runs remain immutable. Final owner
scope/output acceptance and tagging/publication approval remain separate.

## Station-report dtype compatibility follow-up (2026-10-31)

The completed run logged a pandas FutureWarning at the concatenation of
per-session station class reports. Four sessions had an entirely null
peak-client/enrollment ratio column, inferred as object rather than float.
Station class report construction now declares consistent nullable types for
counts, peak flags, enrollment and ratios, including empty/all-null reports.
Columns are constructed directly with these types to avoid intermediate float
rounding of integer counts. No warning is suppressed and no metric formula,
row, column or missing-value meaning is changed. Integer CSV fields may omit
an unnecessary `.0`; logical values remain the same.

Cached validation rebuilt the class and aggregate reports from the immutable
`20261029T213233082897Z-3152b7cd` run's station rollups, one session at a time.
All 2,689 class-report rows and eight aggregate rows across 373 sessions match
exactly after logical dtype normalization, including nulls. FutureWarnings were
treated as errors. This took 10.25 seconds and did not decode captures or
rewrite any published file; verification outputs are separate under
`output/previews/station_dtype_fix_20261031/`.

Regression after the fix: 390 passed, 16 external-tool/short-capture tests
deselected in 99.87 seconds. Focused tests cover valid/zero/missing enrollment,
unavailable station counts, empty reports, concat order, preserved unknown
flags, aggregate values and large UInt64 counts. Existing logical goldens,
compilation and whitespace checks pass. Another complete capture run was not
required to verify this compatibility fix.

## Final candidate closeout (2026-10-31)

The configured default is restored to `RADIO_CLASS_PLOTS=false`; a preflight
check confirms that `--radio-class-plots` still explicitly enables the collection.
This changes future default behavior back to the documented opt-in setting,
not the completed benchmark run's recorded configuration or files.

Final validation passed in two complementary invocations, with no skipped tests:

| Suite | Result | Pytest wall time |
|---|---:|---:|
| Non-external unit/integration | 392 passed, 16 deselected | 105.34 s |
| Real-capture/TShark and short-capture references | 16 passed, 392 deselected | 697.60 s (11m 37s) |

The regular suite includes the historical SSID reference-adapter regression
test described below. JUnit evidence is saved under
`output/release_readiness/alpha_0_2_closeout_20261031/` as
`regular_tests.xml` and `external_tests.xml`. All 42 local Markdown links in
the six updated/new documentation files resolve, and `git diff --check` passes.

The first 16-test external/short-capture attempt produced 14 passes and two failures
because the untracked local `data/processed_data_test/original` folder was
absent, before any baseline comparison could occur. The owner identified the
read-only reference folder `E:/2025_classroom_captures/Data/processed_data`.
All three required files exist there; their metadata row counts are 1,070,997,
1,021,926 and 902,021, matching the retained fixture characterization.
The complete 16-test suite now passes with that existing reference folder;
no expected output was regenerated, reference column omitted or tolerance widened.

That reference-backed attempt exposed one accepted representation change:
historical NUL-only SSIDs were decoded into control-character text, while the
current strict decoder preserves non-printable bytes as hex. A diagnostic
comparison found 251, 358 and 1,259 such rows in the three reference files;
all other reference columns and timestamp indexes match under the existing
comparison rules. A test-only adapter now translates ONLY those historical
NUL-only expected strings to the byte-equivalent hex form. Actual output is
not adapted, SSID comparisons are not omitted, and no tolerance is widened.
The adapter does not call the production decoder or rewrite the source files.
A parameterized unit test covers object and pandas string columns, different
NUL-only lengths, duplicated indexes, unchanged readable/other-binary/missing
values, non-mutation and rejection of incorrect actual SSID bytes or NUL text.
Both conversion and timestamp-source checks pass end-to-end across all three
captures with this narrow reference adapter. The complete cached comparison
also matches every retained reference column and timestamp index. Production
analysis code is unchanged by closeout.

Reproduction command (explicit `tests` path loads the custom fixture option):

```powershell
$env:TSHARK_PATH = 'C:/temp/WiresharkPortable64/App/Wireshark/tshark.exe'
.\.venv\Scripts\python.exe -m pytest tests -q -p no:cacheprovider -m "requires_external_tools or pre_analysis_short" --short-processed-dir 'E:/2025_classroom_captures/Data/processed_data' --junitxml=output/release_readiness/alpha_0_2_closeout_20261031/external_tests.xml -rs
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider -m "not pre_analysis_short and not requires_external_tools" --junitxml=output/release_readiness/alpha_0_2_closeout_20261031/regular_tests.xml
```

Candidate notes are in `docs/alpha-0.2-release-notes.md`. Alpha 0.1 remains the
published release; these documentation/configuration updates do not create a
tag, publish a release or replace final owner approval.

## Documentation/report follow-up (2026-11-13)

The completed full run and 2026-10-31 suite counts above remain historical
evidence. Later owner review changed report wording/navigation and displayed
summary tables, added separate `report --from-run` refresh bundles, and removed
the classroom peak-exposure table/CDF from the web presentation. Underlying
metric/CSV values and the original run remain unchanged. The latest focused
report/refresh/CLI invocation passed 58 tests; this is not a replacement full
suite or a claim that final owner edits are finished.

Current status, storage measurements and pending public-hosting work are in
[reporting and publication](../reporting_and_publication.md) and the
[roadmap](../post_refactor_roadmap.md). A measured refreshed report links about
365.56 MiB including all 691 radio plots and three reporting Parquets; that
derivative footprint is distinct from the original 477.04-MiB full run above.
New full runs of previous datasets are owner-initiated and require their own
recorded outputs/storage. Final-candidate regression, owner acceptance and
explicit release authorization remain outstanding. No Pages export/deployment
or new release is implied by documentation reconciliation.
