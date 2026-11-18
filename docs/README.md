# Documentation guide

Reconciled: 2026-11-18. Alpha 0.2 is the current owner-approved release.
Final report review, representative full runs and regression checks are
complete. Selected [example reports](https://yanzzee.github.io/WiFi-Beacon-analysis/example-reports/)
are public; tag creation is separate from the accepted release scope.

## Current application and reporting references

- [Project README](../README.md): installation, run/refresh commands, inputs and limits.
- [Environment setup](environment_setup.md): supported setup and test prerequisites.
- [Current pipeline](current_pipeline.md): implemented execution and analysis rules.
- [Output inventory](output_inventory.md): authoritative artifacts, derived packages
  and separate report-refresh bundles.
- [Reporting and publication](reporting_and_publication.md): current web layout,
  weighting, refresh behavior, measured storage and the published example site.
- [Capture input profiles](capture_input_profiles.md) and
  [capture scripts](capture_scripts.md): capture compatibility and Linux acquisition.
- [Post-refactor roadmap](post_refactor_roadmap.md): completed Alpha 0.2 scope and deferred work.
- [Alpha 0.2 release notes](alpha-0.2-release-notes.md): final release scope and validation.
- [Release branch synchronization](release_branch_sync.md): copy release documentation
  to the development/public branches while preserving the published report files.

## Accepted contracts

The [decision index](decisions/README.md) maps the accepted timestamp, identity,
selection, missingness, station, CU, admission, cadence and report decisions.
Later dated addenda supersede earlier proposals only in their stated scope.
ADR-0017/0018 record the current presentation refinements; they do not change
the underlying analysis math. Stored filenames and column names can retain
`client`/`average` while the web report uses `stations`/`mean`.

The [corrected fixture specification](corrected_semantics_fixture_spec.md),
[fixture README](../tests/fixtures/README.md) and
[golden contracts](../tests/golden/README.md) define logical regression evidence.
PNG bytes, container compression and old Original output bytes are not current
metric oracles.

## Dated evidence and historical material

[Benchmark records](benchmarks/) preserve their original run dates, test counts,
runtime, storage and implementation boundaries. In particular, the
[2026-10-29/31 readiness record](benchmarks/alpha_0_2_release_readiness_2026-10-29.md)
contains the completed full-run and earlier complete-suite evidence. Those
counts do not claim that every later edit has passed a new full suite.
The [first CU package](corrected_cu_report_package.md) also retains dated
implementation and capture observations; use the current reporting guide for
today's page structure.

[Alpha 0.1 notes](alpha-0.1-release-notes.md) describe that published version.
The [Original migration guide](original_to_corrected_migration.md) and
[Original-output mapping](alpha_0_2_original_output_inventory.md) explain the
one-time informational comparison; no legacy runtime is maintained. Retained
capture characterizations and timestamp-source contracts remain evidence for
the accepted ADRs, not additional supported vendors or capture conditions.

The owner accepted Alpha 0.2 and confirmed the public site on 2026-11-18. The
[publication closeout](benchmarks/alpha_0_2_publication_closeout_2026-11-18.md)
records the four completed v2 runs and 654 passing tests, and distinguishes its
original local preparation from the subsequent owner-led publication. Historical
records retain their original dates and measurements. Further missingness
calibration and CU estimation are in the [Alpha 0.3 roadmap](alpha_0_3_roadmap.md).
