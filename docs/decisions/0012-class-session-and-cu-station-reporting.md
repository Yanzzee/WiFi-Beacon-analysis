# ADR-0012: Class-session and CU/station reporting

- Status: Accepted
- Date decided: 2026-10-04
- Owners/reviewers: Project owner
- Roadmap items: Alpha 0.2 all-class and class-session report families
- Related decisions: [ADR-0010](0010-station-count-rollup-and-staleness-semantics.md)
  and [ADR-0011](0011-bounded-radio-bin-reporting-source.md)

## Decision

### Station distributions

The primary station-count distribution uses one available, complete-only,
all-band/all-SSID classroom total per class-session bin. Separate panels use
band/classroom/bin totals. Unavailable totals are excluded rather than treated
as zero. Stale-assisted values participate and are identified in the plots.

A radio/bin station histogram is not a default report because it would answer
the different question of per-radio load and would weight classrooms with more
radios more heavily.

### CU and station-count relationship

CU and station count are paired only within the same physical radio and bin,
using `reports/radio_class_detail/radio_bin_detail.parquet`. A valid pair
requires `cu_state=valid`, a non-null CU value, and an available non-null radio
station total. No CU or station value is imputed. Stale-assisted station values
participate, and their pair count is reported separately.

The default bounded report provides overall and per-band hexbins, valid-pair
coverage, pair/radio/session counts, and descriptive Pearson and tie-aware
Spearman coefficients. These coefficients make no independence claim:
adjacent bins from the same radio/session are repeated observations. The
report therefore includes no p-values, confidence intervals, or inferential
regression.

Classroom-level CU/station correlation remains deferred because no classroom
CU aggregate (maximum, mean, station-weighted, or another definition) has been
accepted.

### Class-session schedule context

The corrected class-session report has one row per physical classroom
session. Simultaneous source sections remain combined. It retains a
deterministic course-label list, source-section count, and accepted combined
enrollment.

Room capacity is descriptive schedule context only. Missing capacity remains
null. Conflicting capacity values for simultaneous source sections produce a
null value and `conflict` status. No room-capacity utilization ratio is
calculated, and room capacity is not QBSS admission capacity.

The report joins accepted classroom/radio CU coverage and streak facts with
accepted classroom station availability, peak count, stale status, and peak
client count per enrolled. It adds the eligible ratio distribution and an
enrollment-versus-peak plot with stale-assisted peaks visibly distinguished.

## Placement and performance

All outputs are manifest-registered derived reports. No sixth durable logical
artifact is added. Radio/bin relationship statistics are accumulated while
session parts are streamed into the existing derived Parquet, using weighted
frequency counts rather than loading the project-wide table into memory.
Station distributions are accumulated during the existing station-rollup
write. The class-session report joins bounded summary tables and schedule
context.

## Deferred work

- classroom-level CU aggregation and correlation;
- CU/station inferential regression or significance testing;
- stations affected by high CU;
- QBSS admission-capacity reporting;
- beacon-rate diagnostics; and
- additional station percentiles or statistics not already accepted.

### Addendum — 2026-10-08

The admission-capacity item above was subsequently accepted and implemented
under [ADR-0013](0013-radio-admission-capacity-reporting.md). Its independent
radio/bin semantics and separate report package do not change this ADR's
CU/station or classroom-capacity definitions. Beacon-cadence diagnostics were
subsequently accepted under [ADR-0014](0014-exploratory-beacon-cadence-and-capture-quality.md)
and station-time exposure under [ADR-0016](0016-potentially-affected-station-reporting.md).
Classroom-level channel aggregation, inferential regression and additional
unaccepted station statistics remain deferred.
