# Conservative CU bounds: implementation and retained-run review

Reviewed 2026-11-16. This records the completed ADR-0020 addition, not a new
capture analysis, owner approval, release tag or public publication.

## Implementation boundary

All five stages in the [implementation inventory](../cu_missing_bin_bounds_implementation_plan.md)
are implemented: ordinary/conditional CU means, CDF summaries and shaded ranges,
mean figures, availability-conditioned station-time exposure, and HTML/text/
saved-report integration. Existing observed fields, selected observations,
thresholds, correlations, station assistance, admission capacity, enrollment
comparisons and streaks retain their definitions.

Ranges assign CU-missing analysis bins either 0% or 100% CU. They are not
confidence intervals, estimates of missing CU or a correction for capture bias
inside otherwise observed bins. Station-time ranges exclude periods whose
station counts are unknown. Matched-window ranges describe a broader target
than the unchanged common-valid-only comparison.

## Representative retained run

Source: `output/runs/2025-full/20261029T213233082897Z-3152b7cd`.
It contains 18 classrooms, 373 class sessions, 31 observed radios and 6,511,028
selected beacon observations. No capture decode or new analysis was performed.

Refreshed bundle:
`output/reports/2025-full/20261029T213233082897Z-3152b7cd/20261116T084808533671Z-87213565`.
The report entry point is its `reports/index.html`.

After clarifying the duration-CDF table headers to cover both observed and
expected-time percentages, the final report was regenerated as
`20261116T085944192587Z-c271befc` under the same project/source-run directory.
That refresh took **27.4 seconds**, and the preservation/link checks below
were repeated successfully against it.

Refresh took **26.5 seconds**, including independent asset copying, supported
table backfill, figure regeneration and HTML generation. This is one local
measurement, not a full-run benchmark or a general performance guarantee.
Thirteen tables gained companion fields or CDF summaries. The cadence Parquet
was processed in batches; conditional radio detail used one narrow pass with
at most one class session retained for matched-radio joins.

| Example | Observed | Minimum | Maximum |
|---|---:|---:|---:|
| Overall mean CU | 9.82% | 7.58% | 30.38% |
| 5 GHz mean CU | 10.93% | 8.42% | 31.40% |
| 6 GHz mean CU | 2.38% | 1.86% | 23.43% |
| High-CU station-time share | 1.37% | 0.95% | 31.06% |
| Very-high-CU station-time share | 0.19% | 0.13% | 30.24% |

Observed station-time percentages use paired CU/station time; their endpoints
use all available station-time. Wide bounds describe what the missing-bin
evidence permits and do not indicate where within the interval the value lies.

## Preservation and presentation checks

- Every original cell in **31 CSV tables** matched the refreshed copy exactly
  when read as text; rows and original columns were preserved.
- Reporting radio/bin and station-rollup Parquets retained their original
  file hashes. The cadence Parquet retained every original field, value and
  field metadata while adding companion columns.
- The copied source manifest retained its original hash. The separate refresh
  record identifies the added fields, source/result hashes and bounds arithmetic.
- All local report links passed validation. Expected bounds were supported in
  the displayed retained-run metrics; the phrase “bounds unavailable” appeared
  only in general explanatory text.
- The report was reviewed in Chrome at desktop width and a **400-pixel** mobile
  viewport. Cards, comparison tables, wrapping navigation and CU metric text
  remained readable. The browser was returned to its full-width report view.
- Logical plot tests retain observed curves, histogram counts and point/bar
  values. No PNG-byte comparison is used.

Focused historical tests compare backfilled bounds with fresh results across
overall/band/cohort/model/classroom grains, exact core bin counts and terminal
bins, station-time exposure and cadence lags. They also check missing detail,
unsupported denominators, unchanged original values, idempotence and clear
failure for inconsistent paired durations. Prior logical goldens are retained.

The integrated Stage 5 check passed **275 tests**, with one external-capture
test deselected, in 91.9 seconds. Subsequent focused checks cover text-summary
paragraph preservation and every HTML mean location (**74 passed**); the final
HTML-only check passed **42 tests**. The long capture replay
was not repeated because extraction and selection were unchanged.

Final owner report review, final-candidate release checks and separately
authorized publication remain under the [Alpha 0.2 roadmap](../post_refactor_roadmap.md).
ERM/native calibration and inferred CU remain in [Alpha 0.3](../alpha_0_3_roadmap.md).

## Owner-requested presentation refinements

The subsequent owner review separates observed values and possible ranges into
different detailed metrics/table columns, with separate Overview cards for
sampled-band CU and very-high CU station-time. Overview descriptions say
“Between … and … possible.” Matched differences retain percentage-point units
in their labels; metric values contain numbers/ranges only. The cohort CDF
uses three separate panels on common axes. The matched-radio and
mean-CU-versus-coverage scatter plots show observed points only, while numeric
bounds and histogram count shading are retained. The histogram explanation
states that per-bucket count limits need not be jointly attainable.

Refreshed from the same retained source run into
`output/reports/2025-full/20261029T213233082897Z-3152b7cd/20261116T170107999208Z-ed6d4325`.
Its entry point is `reports/index.html`. Refresh took **68.0 seconds** while
integration tests were running concurrently; this is not an isolated runtime
benchmark and should not be compared directly with the earlier timings.

- **278 tests passed**, with one external-capture replay deselected.
- Every original cell in 31 source CSV tables is preserved, and all 33 CSV
  tables in the preceding bounds-enabled report match this bundle exactly.
- Original reporting Parquet fields/metadata and the source manifest remain
  unchanged. All local HTML links pass validation.
- The three-panel CDF and both observed-only scatter plots were inspected
  directly as generated PNGs. The new HTML cards/columns were structurally
  checked. A fresh browser layout review was blocked because Computer Use
  could not verify the current browser URL; it was not completed or retried.

Verification evidence is retained locally at
`output/previews/cu_bounds_refinement/verification.json` and
`output/cu_bounds_refinement_integration.xml`.
