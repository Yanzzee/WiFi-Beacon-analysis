# Documentation guide

Reconciled: 2026-11-13. Alpha 0.1 remains published; Alpha 0.2 is a reporting
candidate under final owner review, not an approved tag/release.

## Current application and reporting references

- [Project README](../README.md): installation, run/refresh commands, inputs and limits.
- [Environment setup](environment_setup.md): supported setup and test prerequisites.
- [Current pipeline](current_pipeline.md): implemented execution and analysis rules.
- [Output inventory](output_inventory.md): authoritative artifacts, derived packages
  and separate report-refresh bundles.
- [Reporting and publication](reporting_and_publication.md): current web layout,
  weighting, refresh behavior, measured report storage and planned public hosting.
- [Capture input profiles](capture_input_profiles.md) and
  [capture scripts](capture_scripts.md): capture compatibility and Linux acquisition.
- [Post-refactor roadmap](post_refactor_roadmap.md): outstanding closeout and deferred work.
- [Alpha 0.2 candidate notes](alpha-0.2-release-notes.md): current candidate scope.

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

Documentation reconciliation does not approve a release, create a public report
export or configure GitHub Pages. The owner's new dataset runs and final report
review are still pending evidence.
