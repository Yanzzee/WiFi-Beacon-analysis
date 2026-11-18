# Corrected full-project acceptance run — 2026-09-27

This is the first complete corrected run over the owner `data/captures`
corpus. It is operational evidence for the current sequential implementation,
not a general performance guarantee.

## Workload and result

- project: `2025-test`;
- run ID: `20260927T060904484327Z-97b6eeb5`;
- inputs: 80 captures totaling 49.37 GiB, three source sample CSVs, 5
  configured APs, and 61 expanded class sessions;
- TShark: 4.6.6;
- raw supported rows: 118,307,935;
- retained configured-project rows: 7,764,399 (about 6.56%);
- excluded outside-project rows: 110,543,536;
- resolved identity: 34 BSSIDs, all resolved, with no fallback or conflict;
- selected observations: 2,334,938;
- radio/class rows: 228;
- classroom/class rows: 61; and
- final five-artifact run size: 29.08 MiB, including 28.88 MiB across five
  selected-observation Parquet files.

The selected-observation range is
`2025-11-10T15:00:10.340107Z` through
`2025-11-14T20:49:59.944142Z`.

## Stage time

| Stage | Elapsed |
|---|---:|
| Extract, normalize, dispatch, and early project-scope filter | 6 h 59 m 10 s |
| Resolve project radio identity | 29 s |
| Select per-class observations | 36 m 51 s |
| Write project identity map | <1 s |
| Write selected observations | 3 m 14 s |
| Write ADR-0004 summaries | 2 m 16 s |
| Whole run, start to atomic publication | 7 h 42 m 0 s |

## Resource observation and correction

The first attempt retained all adapter-claimed rows from each capture and
applied configured-hostname scope only after concatenating the complete
project. Working memory grew from about 2.9 GiB after three files to 4.6 GiB
after five. The attempt was interrupted before unsafe growth, and its owned
staging directory was removed.

That behavior did not satisfy the already accepted PR-0036 requirement to
discard out-of-project observations as early as practical. The corrected path
now applies configured-AP scope immediately after each capture's adapter
dispatch, retains only the scoped rows, and accumulates the same exclusion
counts for the manifest. Focused tests and the complete ordinary suite passed
before restart.

During the successful run, sampled working memory stayed near 2–5 GiB through
most extraction. The highest observed checkpoint was 28.61 GiB working and
29.43 GiB private during project-wide concatenation/selection; it later fell
before publication. This was sampled monitoring rather than instrumented peak
measurement, so it is a lower bound on the true peak.

## Practical conclusion

The current corrected path can complete this 49.37 GiB project without legacy
working files or partial publication. Per-capture configured-scope filtering
is necessary. Runtime is dominated by sequential TShark extraction, while
project-wide selection creates the largest observed memory pressure. Any
parallel extraction, chunked selection, or additional optimization requires a
separate high-level review and semantic-equivalence evidence.

## Scaling implication

The identity and summary artifacts are not the material scaling risk. This
five-AP run produced only 34 identity rows, 228 radio/class rows, and 61
classroom/class rows. Their growth is approximately proportional to BSSIDs,
radios, and class sessions and remains small compared with observation data.

The current in-memory retained-observation boundary is the risk for a project
with about 100 configured APs. The five-AP scope retained 6.56% of supported
rows. If the larger AP list makes most of this same corpus in scope, retained
rows could approach all 118.3 million rows, about 15.2 times this run's
retained count. The sampled 28.61 GiB peak cannot be multiplied into a precise
forecast because the pipeline has fixed costs and several temporary copies,
but it is sufficient evidence that the current design should not be assumed
safe at full scope.

Before that run, benchmark a bounded-memory boundary that keeps compact
project-wide identity evidence but stages or partitions class-window
observations by session/date instead of holding every configured-AP row in one
DataFrame. Parallel capture extraction should be assessed only after that
boundary: the legacy process pool is technically reusable in concept, but
multiple simultaneous TShark/pandas decodes would multiply per-capture memory
and disk pressure. Start any concurrency benchmark at one versus two workers,
preserve source-rank ordering, and do not adopt the legacy worker count by
default.

### Post-run implementation status

The recommended boundary was implemented after this measurement. The runner
now retains distinct project-wide identity evidence in memory, stages only
preferred-timestamp-admitted rows in hidden complete-session Parquet
partitions, and performs identity attachment, TSF selection, ADR-0004 summary
calculation, and selected-observation serialization one session at a time.
Temporary working files are removed before publication or with the owned
staging directory after an understood failure.

This document's 28.61 GiB checkpoint describes the earlier all-configured-row
implementation; it is not a measurement of the new bounded path. A second
80-capture run is required to record the new peak memory, staging-disk size,
and runtime before testing one versus two extraction workers.
