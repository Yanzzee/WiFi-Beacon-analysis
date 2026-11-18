# Corrected Golden Contracts

`corrected_semantics_v1.json` is the authoritative corrected metric/schema
golden. Its dedicated human-readable inputs and expected tables are under
`tests/fixtures/corrected_semantics/`.

The tests compare logical content, not Parquet or PNG bytes. Floating-point
values use the tolerances declared in the golden, and timestamp/duration/QBSS
storage is validated separately by the artifact-writer tests.

`beacon_quality_v1.json` separately freezes the exploratory ADR-0014 CSV and
Parquet logical tables from the focused synthetic native/ERM fixture. Its
logical-JSON fingerprint method is recorded in that file; it does not replace
the authoritative corrected golden or compare rendered/container bytes.

`band_comparison_v1.json` freezes the ADR-0015 logical tables using a fixture
with mixed classrooms, dual 5 GHz radios, 6 GHz and 2.4 GHz observations,
explicit enablement, missing totals, stale assistance, and terminal partial bins.

`band_comparison_mean_bounds_v1.json` freezes the compatible ordinary-mean
bound additions under ADR-0020. Explicit per-stage column projections require
every old logical column to match `band_comparison_v1.json` exactly and
preserve the Stage 1 bounds separately. The original golden is unchanged.

`band_comparison_conditional_mean_bounds_v1.json` adds equal-session,
zero-station and paired-window fields. Explicit column projections still
check both earlier band-comparison goldens, preserving their observed values
and Stage 1 bounds. No original reference is overwritten.

`beacon_quality_mean_bounds_v1.json` freezes the current/previous-window
CU ranges and supporting integer sums/durations. A separate projection checks
every pre-existing logical column against `beacon_quality_v2.json`, including
the original correlations and covariate buckets.

`cu_cdf_bounds_v1.json` freezes Stage 3's new cumulative observed/minimum/maximum
threshold summaries. Separate table fingerprints retain equal radio-bin and
radio-duration weighting, including missing and absent populations. All older
observed and mean-bound goldens remain unchanged; plot checks compare logical
curves, counts and coordinates rather than rendered PNG bytes.

`affected_stations_v1.json` freezes ADR-0016's station-time and complete-bin
peak tables. Removing the classroom peak table/CDF from the web presentation
does not remove those generated tables or change this logical golden. Report
formatting, navigation and saved-run refresh are verified separately; see the
[current reporting guide](../../docs/reporting_and_publication.md).

`affected_stations_cu_bounds_v1.json` freezes ADR-0020 Stage 4's additive
high/very-high exposure quantities and percentages, conditional on available
station counts. An explicit new-column projection still checks every original
field against `affected_stations_v1.json`; all peak values are unchanged.

Stage 5 backfill tests compare original CSV cells as text and original Parquet
columns directly. They compare fresh and backfilled companions separately and
leave all observed/mean/CDF/station-time logical goldens above unchanged.

`legacy_corrected_comparison_v1.json` is explanatory, not a second production
oracle. It uses the same small selected-observation input to preserve a frozen
snapshot of the former location-wide calculation and the current corrected
classroom result. Tests verify the input hash and corrected values but do not
execute or maintain legacy code.
The comparison was executed once and retained for informational migration
documentation. It is not a recurring Original replay or a requirement to
maintain historical functions. See
[`original_to_corrected_migration.md`](../../docs/original_to_corrected_migration.md)
for the frozen example, its limits, and guidance for rerunning old projects.

`beacon_quality_v2.json` freezes observation-time cadence and additive native
TSF diagnostic fields under ADR-0019. The v1 golden remains a historical
reference. Core v2 tests preserve the integrated fixture's measurement values
while explicitly replacing its old mapped-TSF metadata; v1 CSVs are unchanged.

Historical reproduction of the original refactored fingerprint is available
from the public
[Original release](https://github.com/Yanzzee/WiFi-Beacon-analysis/releases/tag/Original).
