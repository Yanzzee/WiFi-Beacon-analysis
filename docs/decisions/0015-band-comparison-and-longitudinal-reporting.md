# ADR-0015: Band comparison and longitudinal reporting

- Status: Accepted
- Date decided: 2026-10-12
- Owners/reviewers: Project owner
- Roadmap items: Alpha 0.2 detailed 5/6 GHz comparison and manual year-over-year reporting
- Related decisions: ADR-0003, ADR-0004, ADR-0010 through ADR-0014

## Purpose and decision

Understanding how 6 GHz infrastructure and actual client use relate to observed
Wi-Fi conditions is the central purpose of the project. Channel utilization is
the primary metric. Detailed within-project comparisons and stable overall
exports support manual comparisons between projects/years initially. They do
not establish throughput, latency, individual client airtime, or causation.

Compare 5 GHz radios on APs without 6 GHz enabled, 5 GHz radios on APs with
6 GHz enabled, and those APs' 6 GHz radios. Provide analogous classroom cohorts
separately. A room with any enabled AP belongs to the enabled-room cohort;
mixed AP configurations are flagged rather than collapsed into AP-level claims.
Include 2.4 GHz in tables and all-band totals when sampled; primary charts
emphasize 5/6 GHz. All sampled SSIDs participate in accepted station totals.

## Enablement input and infrastructure evidence

The optional AP input column `6GHz_Enabled` accepts true/false, yes/no, 1/0,
on/off, or blank/auto. It states whether the AP has 6 GHz enabled for this
project; it does not overwrite an observed radio's band. The existing input
has one row per AP, so no separate hardware-radio registry is introduced.

When unset, an AP is considered enabled only if a resolved 6 GHz radio was
observed anywhere in the project. Classification is not recomputed from each
session's missing frames. Explicit true without observations remains enabled
with an enabled-but-unobserved warning. Explicit false with observed 6 GHz
is a configuration/capture-scope contradiction and fails before publication.
Malformed values fail input validation. Existing input files need no edits.

Deployment exports include configured/observed AP and classroom counts,
enabled counts/percentages, mixed-room counts, enablement source, and sampled
2.4/5/6 GHz radio counts. These describe configured/sample scope, not unsampled
infrastructure or a model-derived complete hardware inventory.

## CU and radio statistics

Retain every physical radio/session separately, including multiple 5 GHz radios
on one AP. Primary cohort means, exact discrete CDFs and high/very-high shares
use valid radio duration, including terminal partial bins. Missing CU and
no-beacon remain separate; no CU is filled. Thresholds stay at raw 128/192.
Accepted observed/possible streaks come directly from authoritative radio
summaries; cohort maxima are labelled maxima, not typical session behavior.

Overall and classroom/band CU summaries are pooled summaries of constituent
radio measurements, not CU sums, merged channel measurements, or a replacement
for authoritative classroom streak semantics. Equal-weighted AP/session and
classroom/session mean views accompany the pooled valid-radio-time view.
Each entity/session mean first pools its available radio time; eligible means
then receive equal weight. Coverage and contributing counts accompany results.
Model and sampled radio-topology strata are retained separately.
CU/station density charts normalize each cohort by valid paired radio time and
share one logarithmic color scale. Their fixed display grid uses a final station
bucket for counts at least 245; statistical inputs are never clipped.

Radio station means use available duration and accepted complete totals,
including permitted 30-second stale assistance. Report available/stale durations
and maximum radio counts. Capacity means use independently valid radio capacity,
not CU availability or sums across radios. Unknowns are excluded, not zero.

CU relative to associated station count is:

```text
sum(raw_CU * bin_duration) * 100 / 255 / sum(stations * bin_duration)
```

Both sums use the same valid radio/bin pairs with available, positive station
counts. Stale-assisted pairs participate and their duration is reported.
Zero-station CU receives separate coverage and a mean. Do not average bin ratios
or divide by independently observed station peaks. Units are CU percentage
points per associated station, not measured airtime consumed per client.

## Matched AP comparisons

Compare each 5 GHz radio with each 6 GHz radio on the same AP and physical
session using only common valid CU bins. Retain both radio IDs, common duration,
coverage, matched means, and signed 6-minus-5 percentage-point difference.
One-to-many configurations create multiple descriptive pairs; these are not
independent samples, one value per AP, or an estimate of the deployment effect.
Broader distributions still include radios/sessions without an eligible match.

## Actual 6 GHz station use

Measure use, not 6 GHz capability or unique device/person counts. Report
`6/(5+6)` and `6/(2.4+5+6)` separately using simultaneous complete band totals
with a positive denominator. AP/band/bin totals sum accepted active radio
rollups without partial numeric sums; classroom values reuse accepted band
rollups. Never sum overlapping AP/classroom/SSID views together.

Each session and project share is a ratio of station-duration sums, not a mean
of bin ratios or a ratio of independent band peaks. Overall adoption uses only
classroom contributions; AP/cohort views are separate. Known/stale-assisted
eligible duration and component sums make changing coverage visible.

A band outside observed project scope, and not explicitly declared 6 GHz
enabled, contributes structural zero to adoption denominators only. Expected
bands without usable totals remain unknown. No station count is written back
to the accepted station views or assumed from an enabled-but-unobserved radio.
Accepted BSSID session membership and sample completeness limits still apply.

## Output and implementation requirements

Add `reports/band_comparison/` as a manifest-registered derived package with
separate metric/schema IDs. Accumulate summaries, 256-value CU marginals and
fixed display-density grids while existing session parts stream. Do not add
a decode pass, an authoritative artifact, or a whole-project bin table.
Detailed CSVs link through session/radio to the existing radio/bin view and
selected observations. Existing published runs and corrected goldens remain
unchanged. The run summary includes concise overall/deployment/use metrics.

### Presentation refinement, 2026-10-15

AP and classroom cohort charts classify the same radio/bin measurements; they
do not compare radio totals with whole-classroom station totals. When no room
mixes APs with and without 6 GHz enabled, those classifications coincide. Show
one shared cohort view in the CU CDF, metric bars and CU/station density plots
in that case, and both views when mixed rooms exist. Retain both definitions
in the CSV exports regardless; no metric formula or table value changes.

Keep AP/session and classroom/session station-adoption histograms separate:
multiple APs in a room make their counting units different even without mixed
enablement. These histograms measure use where available: include only
6 GHz-enabled APs/classrooms, preserving genuine zero-use observations and
excluding non-enabled entities. An enabled classroom still includes all its APs,
including any 5-only APs. Annotate exact-zero use, unavailable enabled sessions,
excluded non-enabled sessions, and the first bin's 0% to less than 2.5% range.
Missing station totals never become zero adoption. Detailed tables retain all
entities and enablement status; existing `with_6` cohort summaries supply the
enabled-only shares. The overall project share continues to include all sampled
infrastructure in its denominator for longitudinal deployment/use comparisons.

`population_summary.csv` is the central numeric lookup for overall, band and
cohort metrics in this package. `methodology.txt` describes the detailed table
lookup. Include the three primary AP-cohort CU/station means and CU coverage,
overall mean stations per radio, and the population-table path in future run
summaries. Published runs remain immutable; this refinement does not rewrite
their summaries or graphics.

Longitudinal tables retain project/run IDs, component sums, denominators,
coverage, AP models, sampled radio counts, primary channels, schedule/enrollment,
and report/metric versions. Context references the manifest and capture plans.
AP/radio IDs remain project-scoped, not lifetime hardware IDs. Compare stable
classroom labels manually first and review changes in schedules, enrollment,
capture/SSID/sample scope, radio density, channel configuration and deployment.
Unavailable bandwidth, power or firmware information remains unknown rather
than inferred. No automated cross-year joining or causal normalization is added.

## Verification and remaining checkpoints

Test explicit/default enablement, contradictions, mixed classrooms, multiple
radios, exact duration weighting, zero/missing/stale station pairs, incomplete
adoption totals, absent versus unobserved bands, no overlapping-rollup double
counting, common-bin matching, constant/empty populations, deterministic order,
create-only exports, cleanup and manifest registration. Freeze logical report
tables, not PNG/container bytes, and preserve the existing corrected oracle.

Review representative 5-only and 5/6 GHz graphics and measure cached full-project
report cost separately from full capture-run runtime. Owner output review and
complete-run timing/storage remain Alpha 0.2 release checkpoints. Paired ERM/native
calibration, inferred missing CU, safe thresholds and causal models remain deferred.
