# Alpha 0.2 publication-candidate audit

Date: 2026-11-18. The owner selected four completed runs for public examples.
The audit below records the original local preparation, before the owner's
subsequent catalog edits and publication. Final owner acceptance and the live
site are recorded in the follow-up at the end; original run measurements and
prepared-package hashes remain dated evidence, not a new deployed-site audit.

## Completed source runs

| Project | Source run | Analysis elapsed | Classrooms | Class sessions | Radio/class-sessions | Full output MiB |
|---|---|---:|---:|---:|---:|---:|
| 2024-classroom-captures | `20261117T071615420763Z-2b8c8d02` | 6h 17m 50s | 51 | 1,063 | 1,811 | 1,202.52 |
| 2025-classroom-captures | `20261116T225557024922Z-74645435` | 6h 53m 49s | 18 | 373 | 691 | 483.87 |
| 2026-classroom-captures | `20261116T200102692404Z-99851ea7` | 2h 28m 26s | 18 | 350 | 798 | 482.04 |
| 2026-hbll | `20261116T184042802936Z-9e329a5c` | 33m 57s | 1 | 23 | 46 | 36.50 |

Elapsed time is from the recorded `run_started` event to `run_published`, so it
excludes preflight and earlier input hashing. It is not directly comparable to
the older complete CLI wall-time benchmark. No controlled CPU/disk trace or
peak-memory measurement was collected for these owner-initiated runs.

All four manifests report `beacon.metric/corrected-cu/v2`, observation-time
binning, and capture-quality v2. They record source revision `8c90bbd` with a
dirty working tree; this is retained honestly, without claiming an exact clean
release build. All stages completed with no errors. Each 2026 run recorded two
native TSF anomaly diagnostics, which do not remove observations under the
current timing policy. The 2024 and 2025 shared-room normalization events are
informational. All 85 registered artifact paths per run exist. Core summary
Parquet row counts agree with the corresponding reported session counts. All
source HTML links and internal anchors resolve.

Extraction dominates these runs. The 2025 selection matches the former
6,511,028 selected observations and 373 class sessions, but its input set now
contains 180 captures / 120.77 GiB, versus the older benchmark's 80 / 49.37 GiB.
The new elapsed time must not be attributed to CU bounds or report changes.
The 2024 run processed 70 captures / 155.26 GiB; both 2026 runs examined 200
captures / 26.06 GiB with different sample scopes.

## Publication copies and repairs

The owner's copies under `WiFi-Beacon-analysis/Output/runs/` totaled
74,746,987 bytes (71.28 MiB). They retained WebP figures but had unresolved
PNG/session links in three reports. Every project also contained six 18-byte
text placeholders named `.parquet`, with the content `large file removed`.
Those are explanations, not valid Parquet files. Original CSV measurements
matched the full local source runs.

A separate publication draft was prepared; the owner's copies and complete
analysis runs remain unchanged. It contains **415 files / 72,123,701 bytes
(68.78 MiB)**. It:

- repairs PNG-to-WebP references, including figure file-type labels;
- routes omitted Parquet downloads to an explicit omission notice and does
  not publish files falsely named `.parquet`;
- preserves summary CSV fields and values, adding `publication_path` and
  `publication_plot_type` companions to the source radio-plot indexes;
- keeps the 52 retained 2025 radio plots and all 46 actual HBLL radio plots;
- links the broader 2026 report's 46 HBLL rows to those same actual files;
- substitutes explicitly labelled, watermarked 5 GHz and 6 GHz examples for
  other omitted radio/class-session plots, as accepted by the owner;
- preserves each project's `reports/index.html` inside its named directory,
  with a root catalog, so no project-specific HTML renaming is necessary;
- records source run IDs, metric versions, source-manifest hashes, omissions,
  substitutions and a hash inventory of the public package; and
- excludes the deliberately unlinked classroom peak-exposure CDF/table.

The shared HBLL mapping is supported by exact equality of all **46** matching
radio/class-session comparison rows, plus all **172,870 radio/bin rows across
35 measurement columns**, including order and excluding only the run ID.
This is stronger than a comparison of AP names or graph appearance.

Every HTML target, internal anchor, plot-index publication target and filename
case in the staged site passed validation. All original CSV fields/rows were
checked against the complete source runs. Both watermark examples were
rendered directly from retained reporting data before rasterization and
visually inspected; original owner-provided WebPs were reused.
A localhost HTTP smoke check also passed for all **11 HTML pages / 412 unique
local targets**, served beneath `/WiFi-Beacon-analysis/`. WebP targets returned
the correct image content type. This verifies repository-prefix routing in the
prepared package, not a remote Pages deployment or a browser layout review.
All 38 local Markdown links in the four updated/new documentation files resolve.

The generated peak CDF title is shortened to
`Classroom/class-session peak association CDFs`. This file is not linked by any
of the four source reports; its calculation and underlying exports are unchanged.

## Local branch and deployment boundary

Public repository: `Yanzzee/WiFi-Beacon-analysis`.

An independent, reports-only local `example-reports` branch was finalized at
`d53ce9e387dc829ee3be99a1fc05f612505a4faa`. It is an orphan branch: report assets
do not become part of the application branch's ancestry. The original public
checkout remains on `main`; its README has a reviewable, uncommitted link to
the example branch. The ignored `Output/` tree was not force-added or modified.
Nothing was pushed and no Pages settings were changed.
Publication-specific `.gitattributes` preserves the exact inventoried bytes
through Git without imposing line-ending rules on application source. All 415
tracked Git blobs were checked against the staged public files, preventing a
newline conversion from silently invalidating publication hashes.

The worktree and draft catalog are under
`output/previews/publication_closeout_20261118/`. The worktree path is
`example-reports-worktree`; its root `index.html` opens the complete catalog.
The local `install.json`, `audit.json`, `source_artifacts.json`,
`hbll_detail_comparison.json`, `http_smoke.json` and `main-readme.diff` retain the detailed evidence.

After owner review, push `example-reports`, commit/push the README link, and
configure GitHub Pages to use `example-reports` at `/ (root)`. The `.nojekyll`
file makes this a static site. Verify the deployed repository-prefix URLs,
mobile layout, real/omitted/shared plot links and downloads. GitHub's
[branch source documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site)
supports this arrangement. The measured package is below the
[1-GB Pages site limit](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits);
the largest current asset is below the
[100-MiB ordinary Git file limit](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github).

## Final regression

The 2026-11-18 regular suite passed **638 tests**, with 16 external tests
deselected, in 282.99 seconds. It includes corrected semantic goldens, CU bounds,
scope/timing, reporting, refresh, capture-script and failure-cleanup regressions.
JUnit: `output/alpha_0_2_20261118_regular.xml`.

The complementary suite passed **16 real-capture/TShark tests**, with 638
regular tests deselected, in **778.14 seconds (12m 58s)**. It used the retained
references at `E:/2025_classroom_captures/Data/processed_data`. JUnit:
`output/alpha_0_2_20261118_external.xml`. Across both invocations, all **654 tests
passed with no failures or skipped tests**. No new full-project capture analysis
was started. The four fresh v2 runs satisfy the pending representative full-run
checkpoint; repeating them is unnecessary for this title/link-only work.

At the original local handoff, remaining actions were final owner acceptance, the final source/version
commit and release notes, promotion of the reviewed Alpha 0.2 application source
to the public repo's main branch (currently Alpha 0.1), public deployment smoke checks, and explicitly
authorized Alpha 0.2 tagging/publication. Alpha 0.3 calibration and inferred CU,
automated year-to-year comparisons and R-squared remain outside this release.

## Final owner acceptance and subsequent publication

On 2026-11-18 the owner accepted the final Alpha 0.2 report and release scope,
promoted the application snapshot to public main, edited/uploaded the example
catalog, and confirmed the [public site](https://yanzzee.github.io/WiFi-Beacon-analysis/example-reports/)
is functional. The published package is under `example-reports/` on the
`example-reports` branch; Pages publishes that branch's root. The original local
draft branch/worktree described above is not the current remote branch tip.

The 654-test result remains the final analysis/reporting regression evidence.
This follow-up reconciles documentation and ignore rules only; it does not
change calculations, rewrite source runs, claim a newly measured site size or
assert that a version tag/GitHub Releases entry has been created. Use the
[path-limited synchronization instructions](../release_branch_sync.md) for the
cleanup so existing report files are preserved.
