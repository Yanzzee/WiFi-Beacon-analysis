"""ADR-0017 offline navigation over existing compact corrected reports."""
from __future__ import annotations

import csv
from datetime import datetime
from html import escape
from html.parser import HTMLParser
import math
import json
import os
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit
from zoneinfo import ZoneInfo

from PIL import Image
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID, cu_mean_bounds_fields

OVERVIEW_PATH = Path("reports/index.html")
REPORT_SCHEMA_ID = "beacon.schema/run-report-overview/v1"

FAMILIES = (
    ("cu", "Channel utilization", "cu_coverage_streak",
     "CU is the fraction of time the radio reports its channel busy. For example, 60% CU means the channel was sensed busy for roughly six tenths of the measurement period. Coverage is the share of expected radio time with a usable CU value; missing beacons do not mean an idle channel. Metrics in this section include channel utilization for the entire sample, as well as the longest streak of high or very high channel utilization per radio in each class session. These metrics are also broken down by band in the Band comparison section.",
     ("plots/radio_bin_cu_histogram.png", "plots/radio_bin_cu_cdf.png", "plots/classroom_streak_distributions.png")),
    ("capacity", "Radio admission capacity", "admission_capacity",
     "Admission capacity is the radio's advertised remaining QoS medium-admission allowance. CU describes channel activity; this value describes what the radio advertises for admission. A low value may imply low capacity for additional real-time or low latency traffic. Missing capacity is not zero, and capacities are not added across radios. Metrics in this section include admission capacity for the entire sample, as well as comparisons between admission capacity and channel utilization or station count.",
     ("admission_capacity_histogram.png", "admission_capacity_cdf.png", "admission_capacity_cu_hexbin.png", "admission_capacity_station_hexbin.png")),
    ("stations", "Station count and enrollment", "station_count",
     "Station counts describe associations across the sampled SSIDs, not unique people or active traffic sources. Mean radio values use the time for which complete station totals are available, including accepted stale assistance up to 30 seconds. Enrollment comparisons use classroom/class-session peaks and combined enrollment. Metrics include mean stations per radio and mean peak stations per enrolled student, with band, SSID and class-session breakdowns.",
     ("../class_sessions/peak_client_count_per_enrolled_histogram.png", "../class_sessions/enrollment_vs_peak_client_count.png")),
    ("bands", "Band comparison and 6 GHz use", "band_comparison",
     "This section measures 6 GHz adoption in infrastructure and client associations, and compares channel utilization and advertised admission capacity across the sampled bands. Metrics include mean CU and associated stations per band, with data also grouped by infrastructure cohort: environments with 5 GHz and no 6 GHz, and those with both bands enabled. These comparisons describe medium use and help investigate how 5 GHz utilization differs as infrastructure adds 6 GHz and client associations transition to the higher band. They show observed relationships rather than isolate the effect of deployment changes.",
     ("../cu_coverage_streak/plots/radio_average_cu_distribution.png", "../cu_coverage_streak/plots/radio_bin_cu_cdf_by_band.png",
      "cu_cohort_cdf.png", "cohort_metrics.png", "matched_ap_cu.png",
      "../admission_capacity/admission_capacity_cu_hexbin_by_band.png", "../admission_capacity/admission_capacity_histogram_by_band.png",
      "../admission_capacity/admission_capacity_cdf_by_band.png", "../admission_capacity/admission_capacity_station_hexbin_by_band.png",
      "../station_count/classroom_bin_station_histogram_by_band.png", "../radio_class_detail/cu_station_hexbin_by_band.png",
      "cohort_cu_station_density.png", "station_adoption.png")),
    ("quality", "Beacon cadence and capture quality", "beacon_quality",
     "Beacon data may be missing from the dataset for various reasons, including RF contention, local interference, AP-model behavior, or limitations of the capture path such as Encapsulated Remote Mirroring (ERM). This is a significant limitation of the analysis because missing data can bias the measurements, especially when missingness is correlated with high channel utilization or station count. These diagnostics compare CU and station counts from preceding observation windows with subsequent capture-slot availability to look for such patterns. A capture slot is an interval in which the diagnostic expects one beacon per sampled BSSID; an empty slot contains none. They explore whether higher observed CU or station counts are associated with lower observed beacon availability. A repeatable relationship could make capture gaps a useful indicator of channel conditions, but it would need calibration for the AP model and capture path before being used to estimate CU. Missing captured beacons alone do not establish over-the-air beacon loss.",
     ("cu_capture_slot_availability.png", "station_capture_slot_availability.png",
      "../cu_coverage_streak/plots/radio_coverage_and_missingness.png", "../cu_coverage_streak/plots/radio_cu_vs_coverage.png", "beacon_slot_gap_cdf.png")),
    ("radio", "CU and station-count relationship", "radio_class_detail",
     "Channel utilization and station count are compared to measure if radios with more associations tend to be busier. Multiple BSSIDs can be broadcast from the same radio; a CU measurement represents one physical radio per time bin, but is expected to be the same value as any BSSID on that channel in the vicinity, especially those on the same radio. Station counts are added across all sampled BSSIDs on that radio in the same bin. Unknown CU and unavailable station counts are excluded; accepted stale-assisted station counts participate. Correlations are listed for CU to station count, and a density plot shows overall which CU and station-count combinations occurred most often; band-specific plots follow in Band comparison.",
     ("cu_station_hexbin.png",)),
)

SECTION_GUIDES = {}

BAND_FIGURE_GROUPS = (
    ("Channel utilization", ("radio_average_cu_distribution.png", "radio_bin_cu_cdf_by_band.png", "cu_cohort_cdf.png", "cohort_metrics.png", "matched_ap_cu.png")),
    ("Admission capacity", ("admission_capacity_cu_hexbin_by_band.png", "admission_capacity_histogram_by_band.png", "admission_capacity_cdf_by_band.png", "admission_capacity_station_hexbin_by_band.png")),
    ("Station counts and 6 GHz use", ("classroom_bin_station_histogram_by_band.png", "cu_station_hexbin_by_band.png", "cohort_cu_station_density.png", "station_adoption.png")),
)

AP_COHORTS = tuple(
    (f"{band}_on_{suffix}", f"{band} GHz · AP {label} 6 GHz")
    for band in ("2.4", "5", "6")
    for suffix, label in (("without_6", "without"), ("with_6", "with"))
)

LABELS = {
    "cohort_metrics.png": "CU, coverage, station load and CU relative to stations",
    "cu_cohort_cdf.png": "CU distributions by infrastructure cohort",
    "station_adoption.png": "6 GHz use where available",
    "matched_ap_cu.png": "Matched 5/6 GHz radios on the same AP/class-session",
    "cohort_cu_station_density.png": "CU versus associated station count by cohort",
    "station_time_exposure.png": "Station-time with high and very high CU",
    "radio_bin_cu_histogram_by_band.png": "CU histogram by band",
    "radio_bin_cu_cdf_by_band.png": "CU cumulative distributions by band",
    "radio_coverage_and_missingness.png": "CU coverage and missingness",
    "classroom_streak_distributions.png": "Classroom maximum CU streaks",
    "cu_station_hexbin_by_band.png": "CU versus station count by band",
    "methodology.txt": "Definitions and interpretation",
    "population_summary.csv": "Overall, band and cohort calculated metrics",
    "entity_session_exposure.csv": "Radio, AP and classroom/session exposure and peaks",
    "population_exposure.csv": "Project, band and cohort station-time exposure",
    "radio_class_plot_index.csv": "Optional radio/session plot index",
    "station_adoption_summary.csv": "Overall and enabled-cohort station-use shares",
    "peak_client_count_per_enrolled_histogram.png": "Peak stations per enrolled across class sessions",
    "enrollment_vs_peak_client_count.png": "Enrollment versus peak classroom station count",
    "radio_bin_cu_histogram.png": "Overall CU histogram",
    "radio_bin_cu_cdf.png": "Overall CU cumulative distribution",
    "radio_average_cu_distribution.png": "Radio/class-session mean CU histogram",
    "radio_cu_vs_coverage.png": "Radio mean CU versus coverage",
    "cu_distribution.csv": "CU distribution counts and radio-time components",
    "model_population_summary.csv": "AP-model and radio-configuration metrics",
    "admission_capacity_summary.csv": "Admission-capacity values and availability",
    "admission_capacity_correlation.csv": "Admission-capacity relationship metrics",
    "beacon_quality_associations.csv": "Capture-quality relationship metrics",
    "cu_station_hexbin.png": "Overall CU versus associated station count",
    "admission_capacity_station_hexbin.png": "Admission capacity versus associated station count",
    "admission_capacity_histogram_by_band.png": "Admission-capacity histograms by band",
    "admission_capacity_cdf_by_band.png": "Admission-capacity cumulative distributions by band",
    "admission_capacity_cu_hexbin_by_band.png": "Admission capacity versus CU by band",
    "admission_capacity_station_hexbin_by_band.png": "Admission capacity versus station count by band",
    "classroom_bin_station_histogram_by_band.png": "Classroom/bin station histograms by band",
    "cu_capture_slot_availability.png": "CU capture slot availability",
    "station_capture_slot_availability.png": "Station capture slot availability",
}

FIGURE_NOTES = {
    "cohort_metrics.png": "Mean CU uses only periods with valid CU measurements. Where bounds are available, CU whiskers show minimum/maximum mean CU over all expected radio time, allowing missing CU to range from 0% to 100%. Mean stations per radio uses only periods with complete station totals, including accepted assisted counts. Each observed mean weights its usable measurements by duration, so the two mean values may use different parts of a class session. CU relative to station count uses matching periods with valid CU and positive station totals; its unit is CU percentage points per associated station. For example, matching mean values of 40% CU and 20 stations give 2 percentage points per station. This descriptive ratio does not measure an individual station's airtime.",
    "cu_cohort_cdf.png": "Each panel shows one infrastructure cohort: 5 GHz radios on APs without 6 GHz, 5 GHz radios on APs with 6 GHz, or their 6 GHz radios. Unlike the by-band curves, each observed reading is weighted by its valid duration. The groups describe 6 GHz enablement, not AP model; model-specific results are in the linked AP-model table. A separate classroom-cohort row appears when a room mixes APs with and without 6 GHz. Each panel keeps its observed curve and possible range on the same scale, without overlapping other cohorts. Shading allows missing CU to range from 0% to 100%; it is not a confidence interval. Full coverage makes the possible limits coincide with the observed curve.",
    "station_adoption.png": "These histograms include one entry per AP/class-session or classroom/class-session with 6 GHz enabled, and the comparison is useful if there are classrooms with mixed APs with and without 6 GHz enabled. The value is the 6 GHz share of combined 5+6 GHz associated station-time over matching observation periods. It measures aggregate association-time, not unique clients; accepted assisted station counts participate. Zero means enabled but unused; APs and classrooms without 6 GHz enabled are excluded.",
    "matched_ap_cu.png": "The left panel pairs observed mean CU for 5 and 6 GHz radios on the same AP/class-session, using only time bins with valid CU on both radios. Points below the diagonal have lower mean CU on 6 GHz. The scatter shows observed values only. The right panel shows 6-minus-5 differences in percentage points. Its shading gives possible counts in each bucket for the complete paired-window scenario: common valid readings constrain both radios, and remaining time may have any CU from 0% to 100%. Bucket maxima need not occur together. This broader window differs from the common-valid-only observations. An AP with multiple radios can contribute multiple descriptive pairs, so these are not independent AP-level comparisons.",
    "cohort_cu_station_density.png": "Brighter cells contain a larger share of that cohort's valid paired radio time. The shared color scale compares the shapes of distributions, not total population sizes. Station counts in the final display bucket are grouped; source statistics are not clipped.",
    "radio_bin_cu_histogram.png": "This histogram shows how often CU values occur across valid radio/bin observations. Each physical radio contributes one CU measurement per analysis time interval (bin), rather than one per SSID. The height of each bar is the number of measurements in that CU range.",
    "radio_bin_cu_cdf.png": "This cumulative distribution combines valid CU measurements, with one measurement for each physical radio and analysis time interval. At a CU level on the horizontal axis, the observed curve gives the fraction of these measurements at or below that level. For example, 0.90 (90%) at 50% CU means nine out of ten usable measurements had CU no higher than 50%. Each CU measurement has equal weight, and missing CU is excluded from the observed curve. Where expected bin counts are available, shading and dashed/dotted curves show minimum/maximum cumulative shares over all expected radio/bins, allowing missing CU to range from 0% to 100%. The labels retain the existing raw-field cutoffs at the 50% and 75% markers. These are possible ranges, not confidence intervals or estimates of missing CU.",
    "radio_average_cu_distribution.png": "A radio/class-session is one physical radio measured during one scheduled class or event. Each band's outline histogram counts its observed time-weighted class-session means in CU buckets about 0.78 percentage points wide. Shading shows the possible total count in each bucket after allowing missing CU to range from 0% to 100%. The lower edge counts mean intervals wholly inside the bucket; the upper edge counts all intervals that intersect it. These are separate bucket limits, not one possible histogram: each radio/class-session can occupy only one bucket, so several upper limits may rely on the same sessions and cannot occur together. Other buckets can still exceed their observed counts when different sessions move. An entirely missing radio/class-session contributes a possible range without an observed mean.",
    "radio_bin_cu_cdf_by_band.png": "These curves compare overall channel conditions on each band, combining all sampled AP models and 6 GHz deployment groups within that band. At a CU level, each observed curve gives the fraction of valid radio/bin readings at or below it. Each reading has equal weight. Where expected bin counts are available, shading and dashed/dotted curves show minimum/maximum cumulative shares over all expected radio/bins, allowing missing CU to range from 0% to 100%. An absent band is omitted; a present band with entirely missing CU has a possible range without an observed curve. The next figure separates the infrastructure groups and weights radio time.",
    "radio_cu_vs_coverage.png": "Each point is one radio/class-session, showing its mean observed CU against the percentage of its scheduled radio time with usable CU data. Colors distinguish AP models and sampled radio configurations. Check coverage when interpreting a low CU mean: missing periods may have had different channel conditions. This graph shows observed means only; possible ranges remain in the numeric tables. No point is created when CU is entirely missing.",
    "radio_coverage_and_missingness.png": "These histograms separate usable CU, beacons without usable CU, and bins without a beacon. Each entry represents one radio/class-session. Low observed CU is less reassuring when much of a radio's observation window is missing.",
    "classroom_streak_distributions.png": "These histograms consider all sampled radios on the APs associated with each classroom. For each classroom class session, the calculation finds each radio's longest high-CU or very-high-CU streak, then retains the longest single-radio streak. The radios do not need to be above the threshold simultaneously, and high periods from different radios are not joined into one streak. CU coverage determines how much can be observed: observed streaks stop at missing CU, while possible-connected streaks can bridge gaps in coverage without proving that CU stayed high during those gaps.",
    "admission_capacity_histogram.png": "This histogram shows the distribution of valid advertised admission capacity, expressed relative to the field's defined maximum. Zero is an observed value, while missing/invalid values are excluded and reported separately.",
    "admission_capacity_cdf.png": "At a capacity value, the curve gives the fraction of valid readings at or below it. A concentration near an endpoint may reflect the AP's advertised behavior; it is not a direct measurement of usable throughput.",
    "admission_capacity_cu_hexbin.png": "Pairs admission capacity and CU from the same radio/bin. Dense regions show common combinations. A relationship can reflect radio admission policy as well as channel activity; neither axis substitutes for the other.",
    "admission_capacity_station_hexbin.png": "Pairs each valid advertised admission-capacity value with the complete associated station total for the same radio and analysis time bin. The color scale shows how often each capacity/station-count combination occurs. Accepted assisted station counts participate; unavailable pairs are excluded.",
    "admission_capacity_histogram_by_band.png": "These histograms compare advertised admission capacity across bands. Each panel expresses the frequency of capacity values as a percentage of that band's valid radio/bin readings.",
    "admission_capacity_cdf_by_band.png": "For each band, the curve shows the fraction of valid advertised admission-capacity readings at or below the value on the horizontal axis. Compare how the advertised allowance is distributed across bands.",
    "admission_capacity_cu_hexbin_by_band.png": "Each panel pairs admission capacity and CU on the same radio and time bin for one band. The color scale shows the number of valid pairs in each density cell.",
    "admission_capacity_station_hexbin_by_band.png": "Each panel pairs admission capacity and the associated station total on the same radio and time bin for one band. The color scale shows the number of valid pairs; accepted assisted station counts participate.",
    "classroom_bin_station_histogram_by_band.png": "Each histogram counts complete classroom/bin station totals for one band, combining sampled radios across the classroom's APs. The panels share one vertical scale so their observation counts can be compared directly. Available includes stale-assisted observations; the orange outline identifies observations that use assisted station counts.",
    "peak_client_count_per_enrolled_histogram.png": "This histogram includes each class session's highest available classroom station count divided by combined enrollment.",
    "enrollment_vs_peak_client_count.png": "Compares combined enrollment with the maximum available classroom station count in the same class session. Stale-assisted peaks are distinguished. Attendance and the number of associated stations per attendee can vary between class sessions.",
    "station_time_exposure.png": "Weights channel conditions by the number of associated stations and duration. Ten minutes with 50 associations contributes 500 association-minutes. Observed percentages use only paired CU/station data. Where available, min/max whiskers use all station-time with known station counts, allowing missing CU to range from 0% to 100%; unknown station counts remain outside these bounds. High CU includes very high CU and does not prove client impairment.",
    "beacon_slot_gap_cdf.png": "Describes gaps between occupied expected beacon slots. Native and ERM captures use different cadence expectations. Gaps describe what reached the capture, not a proven over-the-air loss rate.",
    "cu_capture_slot_availability.png": "Bars group observation windows by the preceding window's mean CU. Each n= label counts paired radio/capture-family observation windows in that group, not beacons or individual capture slots. The vertical axis is the percentage of expected BSSID capture slots that were empty in the following windows: total empty slots divided by total expected slots. It is not the percentage of windows with missing data or the probability that a single next slot is empty. Models and bands are pooled within each capture-family panel. Missing listening exposure and capture-path congestion limit interpretation; this does not establish a safe CU threshold or fill missing CU.",
    "station_capture_slot_availability.png": "Bars group observation windows by the preceding window's mean station count, including accepted stale assistance. Each n= label counts paired radio/capture-family observation windows in that group, not beacons or individual capture slots. The vertical axis is the percentage of expected BSSID capture slots that were empty in the following windows: total empty slots divided by total expected slots. It is not the percentage of windows with missing data or the probability that a single next slot is empty. Models and bands are pooled within each capture-family panel. This is descriptive evidence for investigation, not a model that corrects missing beacons.",
    "cu_station_hexbin.png": "The color scale shows the number of valid same-radio/bin pairs in each occupied cell. The grid is a visual summary; the correlation coefficients use the underlying exact pairs, not the grid cell centers.",
    "cu_station_hexbin_by_band.png": "Separates the CU/station relationship by band so a pooled association is not mistaken for a within-band effect. The CU/station relationship section provides the per-band coefficients and pair coverage; the colors here show the frequency of valid radio/bin pairs.",
}


def _label(name):
    return LABELS.get(Path(name).name, Path(name).stem.replace("_", " ").capitalize())


def _number(value, suffix="", digits=2, scale=1):
    try:
        number = float(value) * scale
    except (TypeError, ValueError):
        return "Unavailable"
    return f"{number:,.{digits}f}{suffix}" if math.isfinite(number) else "Unavailable"


def _possible_range(row, field, suffix="", digits=2, scale=1):
    lower, upper = (_number(row.get(f"{field}_{bound}_bound"), suffix, digits, scale) for bound in ("lower", "upper"))
    return "Unavailable" if "Unavailable" in (lower, upper) else f"{lower}–{upper}"


def _possible_description(row, field, suffix="%"):
    lower, upper = (_number(row.get(f"{field}_{bound}_bound"), suffix) for bound in ("lower", "upper"))
    return "Possible range unavailable" if "Unavailable" in (lower, upper) else f"Between {lower} and {upper} possible"


def _cadence_description(configuration):
    """Explain the recorded diagnostic expectations without guessing settings."""
    def positive_integer(value):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return int(number) if not isinstance(value, bool) and math.isfinite(number) and number > 0 and number.is_integer() else None

    def seconds(value):
        return _number(value, digits=6).rstrip("0").rstrip(".")

    quality = configuration.get("beacon_quality") or {}
    interval = positive_integer(quality.get("expected_beacon_interval_tu"))
    erm_multiple = positive_integer(quality.get("erm_intervals_per_capture_slot"))
    paragraphs = []
    if interval:
        nominal = interval * 1024 / 1_000_000
        text = (f'The cadence diagnostic uses a nominal beacon interval of {interval} TU ({seconds(nominal)} seconds). '
                f'For native captures, this gives approximately {_number(1 / nominal)} expected beacon opportunities per second per BSSID. ')
        if erm_multiple:
            slot = nominal * erm_multiple
            text += (f'ERM uses one expected capture opportunity per {erm_multiple} beacon intervals ({seconds(slot)} seconds), '
                     f'or approximately {_number(1 / slot, digits=3)} per second per BSSID. '
                     'This lower baseline describes the capture expectation, not the AP\'s transmitted beacon rate. '
                     'Multiple captured beacons within one ERM slot receive only one unit of availability credit.')
        else:
            text += 'The ERM capture-slot expectation was not recorded for this run.'
        paragraphs.append('<p>' + text + '</p>')
    else:
        paragraphs.append('<p>The diagnostic beacon interval was not recorded for this run.</p>')
    widths = configuration.get("bin_widths_us")
    if widths is None:
        widths = [configuration.get("bin_width_us")]
    parsed = [positive_integer(value) for value in widths] if isinstance(widths, (list, tuple)) else []
    if parsed and all(value is not None for value in parsed):
        distinct = sorted(set(parsed))
        if len(distinct) == 1:
            text = f'CU analysis uses {seconds(distinct[0] / 1_000_000)}-second bins'
        else:
            text = 'CU analysis uses bin widths of ' + ', '.join(seconds(value / 1_000_000) + ' seconds' for value in distinct) + ', depending on the class session'
        paragraphs.append('<p>' + text + ', with at most one selected CU measurement per radio per bin. '
                          'CU coverage describes the analysis time with a usable radio measurement; cadence availability describes how many expected per-BSSID capture opportunities were occupied.</p>')
    else:
        paragraphs.append('<p>The CU analysis interval was not recorded for this run.</p>')
    return ''.join(paragraphs)


def _one(rows, **keys):
    return next((row for row in rows if all(row.get(key) == value for key, value in keys.items())), {})


def _metric_facts(items):
    return '<dl class="facts section-metrics">' + ''.join(
        f'<div><dt>{escape(label)}</dt><dd>{escape(str(value))}</dd></div>' for label, value in items
    ) + '</dl>'


def _report_inventories(root):
    """Assign each stored report file to one section, following curated figures."""
    inventories = {key: [] for key in (*[family[0] for family in FAMILIES], "sessions", "evidence")}
    folders = {family[2]: family[0] for family in FAMILIES}
    folders["class_sessions"] = "sessions"
    folders["affected_stations"] = "cu"
    owners = {}
    for section, _, directory, _, plots in FAMILIES:
        for filename in plots:
            relative = (root / "reports" / directory / filename).resolve().relative_to(root).as_posix()
            owners[relative] = section
    owners.update({
        "reports/cu_coverage_streak/band_population_report.csv": "bands",
        "reports/cu_coverage_streak/plots/radio_bin_cu_histogram_by_band.png": "bands",
        "reports/radio_class_detail/radio_class_plot_index.csv": "sessions",
    })
    for parent, directories, filenames in os.walk(root / "reports", followlinks=False):
        parent = Path(parent)
        # Individual time-series plots remain grouped through the session plot index.
        if parent == root / "reports/radio_class_detail":
            directories[:] = [name for name in directories if name != "plots"]
        for filename in filenames:
            relative = (parent / filename).relative_to(root)
            if relative == OVERVIEW_PATH or relative.as_posix() in {
                "reports/affected_stations/classroom_peak_cdf.png",
                "reports/affected_stations/classroom_peak_distribution.csv",
            }:
                continue
            section = owners.get(relative.as_posix(), folders.get(relative.parts[1], "evidence"))
            inventories[section].append(relative.as_posix())
    return {section: sorted(paths) for section, paths in inventories.items()}


def _cu_cdf_values(distribution, scope, population, *, weight="valid_radio_bin_count"):
    """Read the existing CDF at 50/75% from its saved exact raw-CU marginal."""
    members = [row for row in distribution if row.get("population_scope") == scope and row.get("population_id") == population]
    if not members:
        return None, None, None
    try:
        pairs = [(int(row["cu_raw"]), int(row[weight])) for row in members]
    except (KeyError, TypeError, ValueError):
        return None, None, None
    if any(not 0 <= raw <= 255 or count < 0 for raw, count in pairs):
        return None, None, None
    total = sum(count for _, count in pairs)
    if not total:
        return None, None, 0
    # Same boundaries as the existing CDF figures: raw <128 and raw <192.
    return (sum(count for raw, count in pairs if raw < 128) * 100 / total,
            sum(count for raw, count in pairs if raw < 192) * 100 / total, total)


def _timestamp(value):
    if not value:
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _scheduled_hours(sessions):
    if not sessions:
        return None
    if len({row["class_session_id"] for row in sessions}) != len(sessions):
        raise ValueError("overview requires one row per class session")
    durations = []
    for row in sessions:
        start, end = _timestamp(row.get("class_start_utc")), _timestamp(row.get("class_end_utc"))
        if start is None or end is None or end <= start:
            return None
        durations.append((end - start).total_seconds())
    return sum(durations) / 3600


def _session_radio_stats(radios):
    """ADR-0015 pooling from exact compact components, never rounded means."""
    groups, seen = {}, set()
    for row in radios:
        key = row.get("class_session_id"), row.get("radio_group_id")
        if key in seen:
            raise ValueError("overview requires unique radio/session source rows")
        seen.add(key)
        groups.setdefault(key[0], []).append(row)
    result = {}
    for sid, members in groups.items():
        raw = duration = None
        try:
            raw = sum(int(row["cu_raw_duration_sum"]) for row in members)
            duration = sum(int(row["valid_cu_duration_us"]) for row in members)
            mean = raw / duration * 100 / 255 if duration > 0 else None
        except (KeyError, TypeError, ValueError):
            mean = None
        bounds = {f"mean_radio_cu_percent_{bound}_bound": None for bound in ("lower", "upper")}
        try:
            total = sum(int(row.get("radio_duration_us", row.get("class_duration_us"))) for row in members)
        except (KeyError, TypeError, ValueError):
            total = None
        if raw is not None and duration is not None and total is not None:
            fields = cu_mean_bounds_fields(raw, duration, total, prefix="mean_radio_cu_percent")
            bounds.update({key: value for key, value in fields.items() if key in bounds})
            if duration == total and duration:
                bounds = dict.fromkeys(bounds, mean)
        result[sid] = {
            "ap_count": len({row["ap_id"] for row in members}) if all(row.get("ap_id") for row in members) else None,
            "radio_count": len(members) if all(row.get("radio_group_id") for row in members) else None,
            "mean_radio_cu_percent": mean,
            **bounds,
        }
    return result


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.targets, self.ids = [], set()

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if "id" in values:
            self.ids.add(values["id"])
        for key in ("href", "src"):
            if key in values:
                self.targets.append(values[key])


def validate_overview_links(text, root, *, manifest_pending=False):
    """No remote dependencies, dangling anchors, missing files or run escapes."""
    root = Path(root).resolve()
    parser = _Links()
    parser.feed(text)
    for target in parser.targets:
        url = urlsplit(target)
        if url.scheme or url.netloc or url.query:
            raise ValueError("overview links must be local run-relative paths")
        if not url.path:
            if url.fragment not in parser.ids:
                raise ValueError("overview link has an unknown anchor")
            continue
        resolved = (root / OVERVIEW_PATH.parent / unquote(url.path)).resolve()
        if not resolved.is_relative_to(root):
            raise ValueError("overview link escapes run directory")
        if manifest_pending and resolved == root / "manifest.json":
            continue
        if not resolved.is_file():
            raise ValueError(f"overview link does not exist: {target}")


def write_run_overview(run_root, *, project_name, run_id, manifest_pending=False, run_context=None):
    """Read compact CSVs only; write one create-only, offline HTML derivative."""
    root = Path(run_root).resolve()
    destination = root / OVERVIEW_PATH
    if destination.exists():
        raise FileExistsError(destination)

    def safe(relative):
        relative = str(relative)
        if "\\" in relative or ":" in relative or Path(relative).is_absolute():
            raise ValueError("overview asset path must be run-relative")
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise ValueError("overview asset path escapes run directory")
        return path

    def rows(relative):
        path = safe(relative)
        if not path.is_file():
            return []
        with path.open(encoding="utf-8-sig", newline="") as stream:
            return list(csv.DictReader(stream))

    def href(relative):
        path = safe(relative)
        return quote(Path(os.path.relpath(path, destination.parent)).as_posix(), safe="/")

    def link(relative, label=None):
        return f'<a href="{escape(href(relative), quote=True)}">{escape(label or _label(relative))}</a>'

    def table(headers, data):
        head = "".join(f'<th scope="col">{escape(label)}</th>' for label in headers)
        body = "".join("<tr>" + "".join(f"<td>{value}</td>" for value in record) + "</tr>" for record in data)
        return f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'

    inventories = _report_inventories(root)

    def inventory_html(section):
        paths = inventories[section]
        if not paths:
            return ""
        items = ''.join(f'<li>{link(path)} <span class="filetype">{escape(Path(path).suffix[1:].upper())}</span></li>' for path in paths)
        return f'<details class="file-inventory"><summary>All tables, figures and definitions ({len(paths)} files)</summary><ul class="inventory">{items}</ul></details>'

    populations = rows("reports/band_comparison/population_summary.csv")
    cu_distribution = rows("reports/band_comparison/cu_distribution.csv")
    overall = _one(populations, population_scope="overall", population_id="all")
    adoption = rows("reports/band_comparison/station_adoption_summary.csv")
    use = _one(adoption, population_scope="overall", population_id="all")
    enabled_ap_use = _one(adoption, population_scope="ap_cohort", population_id="with_6")
    deployment = rows("reports/band_comparison/deployment_summary.csv")
    infra = _one(deployment, population_scope="overall", population_id="all") or (deployment[0] if deployment else {})
    exposures = rows("reports/affected_stations/population_exposure.csv")
    exposure = _one(exposures, population_scope="overall", population_id="all")
    count_cdf_bounds = rows("reports/cu_coverage_streak/cu_cdf_bounds.csv")
    time_cdf_bounds = rows("reports/band_comparison/cu_cdf_bounds.csv")
    sessions = rows("reports/class_sessions/class_session_report.csv")
    radio_rows = rows("reports/band_comparison/radio_class_comparison.csv")
    if not radio_rows:
        radio_rows = rows("reports/cu_coverage_streak/radio_class_report.csv")
    session_stats = _session_radio_stats(radio_rows)
    enrollment = rows("reports/station_count/station_aggregate_report.csv")
    enrolled_overall = _one(enrollment, population_type="overall", population_id="all")
    context = run_context
    if context is None and safe("manifest.json").is_file():
        context = json.loads(safe("manifest.json").read_text(encoding="utf-8"))
    context = context or {}
    processing_stage_count = context.get("processing_stage_count")
    if processing_stage_count is None and isinstance(context.get("stages"), list):
        processing_stage_count = len(context["stages"])
    configuration = context.get("effective_configuration", {})
    timezones = sorted({row["display_timezone"] for row in sessions if row.get("display_timezone")})
    timezone_name = configuration.get("input_adapter", {}).get("schedule_timezone") or (timezones[0] if len(timezones) == 1 else None)
    window = context.get("actual_selected_observation_range", {})
    window_values = [_timestamp(window.get(bound)) for bound in ("start", "end")]
    window_text = "Unavailable"
    if all(window_values):
        zone = ZoneInfo(timezone_name or "UTC")
        window_text = " to ".join(value.astimezone(zone).isoformat(sep=" ", timespec="seconds") for value in window_values)

    parts = [f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
             f'<meta name="report-schema" content="{REPORT_SCHEMA_ID}"><title>{escape(str(project_name))} — Beacon Analysis</title>'
             f'<meta name="cu-bounds-metric" content="{CU_BOUNDS_METRIC_ID}"><style>{CSS}</style></head><body><a class="skip" href="#overview">Skip to results</a><header>'
             '<p class="eyebrow">Beacon Analysis · Run report</p>'
             f'<h1>{escape(str(project_name))}</h1></header>']
    nav = [("overview", "Overview"), ("cu", "Channel utilization"),
           ("stations", "Stations & enrollment"), ("radio", "CU/station"), ("capacity", "Admission capacity"), ("bands", "Band comparison"),
           ("quality", "Capture quality"), ("sessions", "Class sessions"), ("evidence", "Evidence")]
    parts.append('<nav aria-label="Report sections">' + "".join(f'<a href="#{key}">{label}</a>' for key, label in nav) + '</nav><main>')
    parts.append('<section id="overview"><h2>Overview</h2>'
                 '<p>This report analyzes Wi-Fi beacons captured at the project\'s sampled locations during scheduled classes or events. '
                 'It uses the IEEE 802.11e BSS Load information element advertised in those beacons to examine '
                 'channel utilization (CU), associated station count and available admission capacity.</p>'
                 '<p>Channel utilization describes how busy a radio senses its channel. Station count describes how many stations '
                 'are associated with the sampled wireless networks. Admission capacity describes the radio\'s advertised remaining '
                 'allowance for admitting quality of service (QoS) traffic.</p>'
                 '<p>The results summarize measurements across sampled access points (APs), classrooms, network names (SSIDs) and radio bands. '
                 'Band comparisons and CU/station-count relationships help investigate how 6 GHz deployment and actual client use '
                 'relate to channel conditions and the distribution of associated stations.</p>'
                 '<div class="overview-subsection" aria-labelledby="overview-scope"><h3 id="overview-scope">Scope</h3>'
                 '<p>The counts below describe the captured sample and scheduled sessions. Class-session hours add the duration of each '
                 'classroom session, including sessions occurring at the same time in different rooms. Selected beacon observations '
                 'count one representative beacon per BSSID per analysis time interval.</p>'
                 f'<p class="run-id">Run ID: {escape(str(run_id))}</p>'
                 f'<p class="caption">First and last analyzed beacons: {escape(window_text)}. Local timezone: {escape(timezone_name or "Unavailable")}.</p>')
    facts = [("Classrooms in schedule", _number(len({row['classroom'] for row in sessions}) if sessions else None, digits=0)),
             ("Scheduled class sessions", _number(len(sessions) if sessions else None, digits=0)),
             ("Class sessions with measurements", _number(overall.get("class_session_count"), digits=0)),
             ("Scheduled class-session hours", _number(_scheduled_hours(sessions))),
             ("Selected beacon observations", _number(configuration.get("bounded_working_set", {}).get("selected_rows"), digits=0)),
             ("Capture files", _number(configuration.get("capture_count"), digits=0)),
             ("Observed APs", _number(infra.get("observed_ap_count"), digits=0)),
             ("Observed radios", _number(overall.get("radio_count"), digits=0))]
    parts.append('<dl class="facts">' + "".join(f'<div><dt>{escape(label)}</dt><dd>{value}</dd></div>' for label, value in facts) + '</dl>')
    parts.append('</div><div class="overview-subsection" aria-labelledby="overview-results"><h3 id="overview-results">Results</h3>'
                 '<p>These headline results summarize all sampled radios during the analyzed class sessions. '
                 'The <a href="#bands">Band comparison</a> section separates these results further by band and infrastructure deployment.</p><div class="cards">')

    def band_values(field, suffix=""):
        values = []
        for band in ("2.4", "5", "6"):
            row = _one(populations, population_scope="band", population_id=band)
            if row and row.get("status") != "no_matching_radios":
                values.append(f"{band} GHz: {_number(row.get(field), suffix)}")
        return " · ".join(values)

    cu_cards = [("Mean CU overall", _number(overall.get("mean_cu_percent"), "%"),
                 _possible_description(overall, "mean_cu_percent"))]
    for band in ("2.4", "5", "6"):
        row = _one(populations, population_scope="band", population_id=band)
        if row and row.get("status") != "no_matching_radios":
            cu_cards.append((f"Mean CU {band} GHz", _number(row.get("mean_cu_percent"), "%"),
                             _possible_description(row, "mean_cu_percent")))
    for title, value, detail in (
        *cu_cards,
        ("Mean stations per radio", _number(overall.get("mean_radio_station_count")),
         band_values("mean_radio_station_count")),
        ("Peak stations per enrolled", _number(enrolled_overall.get("average_peak_client_count_per_enrolled")),
         "Mean classroom peaks relative to enrollment"),
        ("CU coverage", _number(overall.get("cu_coverage_percent"), "%"),
         "Radio observation time with usable CU"),
        ("Station coverage", _number(overall.get("station_coverage_percent"), "%"),
         "Radio observation time with complete station totals"),
        ("Station-time with high CU", _number(exposure.get("high_exposure_percent"), "%"),
         _possible_description(exposure, "high_exposure_percent")),
        ("Station-time with very high CU", _number(exposure.get("very_high_exposure_percent"), "%"),
         _possible_description(exposure, "very_high_exposure_percent")),
        ("6 GHz-enabled APs", _number(infra.get("six_ghz_enabled_ap_count"), digits=0),
         "Of " + _number(infra.get("configured_ap_count"), digits=0) + " configured APs (" + _number(infra.get("six_ghz_enabled_ap_percent"), "%") + ")"),
        ("6 GHz associated station-time", _number(use.get("5_6_six_station_share_percent"), "%"),
         "Of 5 and 6 GHz associated station-time on all APs"),
        ("6 GHz associated station-time on 6 GHz APs", _number(enabled_ap_use.get("5_6_six_station_share_percent"), "%"),
         "Of their 5 and 6 GHz associated station-time"),
    ):
        description = f'<p>{escape(detail)}</p>' if detail else ""
        parts.append(f'<article class="card"><h4>{escape(title)}</h4><p class="value">{value}</p>{description}</article>')
    parts.append('</div><div class="overview-notes"><h4>How to read these results</h4>'
                 '<p><strong>Means and coverage.</strong> CU and station means are weighted by the duration of usable measurements; '
                 'a radio observed for longer contributes more to the mean. Coverage is the percentage of expected radio observation time '
                 'during the analyzed sessions with a usable CU value or a complete station total. When beacon data has gaps, '
                 'each BSSID\'s station count can use its last known value for up to 30 seconds, or its first known value to fill '
                 'a gap of up to 30 seconds at the start of a session. These assisted counts are included in station totals. CU is not filled across gaps. '
                 'See <a href="#cu">Channel utilization</a> and <a href="#stations">Stations &amp; enrollment</a> for availability details.</p>'
                 '<p>Observed CU describes bins with usable measurements. Minimum and maximum CU means allow missing bins to range from 0% to 100%, '
                 'using all expected radio time. Station-time exposure bounds instead use time with available station counts; periods with unknown '
                 'counts are outside those bounds. These are conservative missing-bin scenarios, not confidence intervals or estimates of missing CU. '
                 'They do not cover missed variation within a bin that has a usable reading. Bounds are unavailable when the saved components are insufficient.</p>'
                 '<p><strong>Peak stations per enrolled.</strong> The mean is calculated by dividing the sum of included class-session peak classroom '
                 'station counts by the sum of their combined student enrollments. This weights each session in proportion to enrollment: '
                 'a 200-student session has ten times the weight of a 20-student session. It represents peak associated stations per enrolled student. '
                 'See <a href="#stations">Stations &amp; enrollment</a> for session eligibility and SSID/band breakdowns.</p>'
                 '<p><strong>6 GHz associated station-time.</strong> Associated station-time is station count multiplied by duration: '
                 '10 stations for 10 minutes contributes 100 station-minutes. The displayed percentages divide 6 GHz associated station-time '
                 'by combined 5 and 6 GHz associated station-time, using matching periods with complete station totals. '
                 'This describes aggregate association time, without tracking individual clients\' connection histories. '
                 'The sample-wide value includes 5 GHz-only APs; the value for 6 GHz APs includes only APs with 6 GHz enabled. '
                 'Both percentages exclude 2.4 GHz. See <a href="#bands">Band comparison</a> for deployment '
                 'definitions, sample completeness and detailed station-use results.</p>'
                 '<p><strong>Station-time with high CU.</strong> This is the share of station-time with high CU among observations having both '
                 'usable CU and station totals. High CU starts at approximately 50%; very high starts at approximately 75% and is included in high. '
                 'This describes potential exposure to busy channels. See <a href="#exposure">Affected associations</a> for thresholds, '
                 'paired-data coverage and interpretation.</p></div></div><div class="links">')
    for relative, label in (("reports/run_summary.txt", "Read the run summary"),
                            ("reports/band_comparison/population_summary.csv", "Numeric headline source"),
                            ("reports/band_comparison/deployment_summary.csv", "Infrastructure counts"),
                            ("reports/band_comparison/station_adoption_summary.csv", "Station-use components and eligibility")):
        if safe(relative).is_file():
            parts.append(link(relative, label))
    parts.append('</div></section>')

    def section_metrics(section, *, heading=True):
        number = lambda field, suffix="", scale=1, digits=2: _number(overall.get(field), suffix, digits=digits, scale=scale)
        caption = ""
        if section == "cu":
            values = [
                ("Mean CU observed", number("mean_cu_percent", "%")),
                ("Mean CU possible", _possible_range(overall, "mean_cu_percent", "%")),
                ("CU coverage", number("cu_coverage_percent", "%")),
                ("Valid CU radio-hours", number("valid_cu_duration_us", scale=1 / 3_600_000_000)),
                ("Valid radio/bin readings", _number(_cu_cdf_values(cu_distribution, "overall", "all")[2], digits=0)),
                ("Valid CU time with high CU", number("high_cu_percent_of_valid_time", "%")),
                ("Valid CU time with very high CU", number("very_high_cu_percent_of_valid_time", "%")),
            ]
            caption = ("The mean and CU-time percentages weight valid readings by duration, so each radio contributes according to its observed time. Station-time percentages instead weight by associated station count multiplied by duration, giving more weight to periods with more associated stations. "
                       "Mean-CU minimum and maximum include all expected radio time, assigning missing CU 0% and 100%. "
                       "High CU starts at raw 128 (about 50%); very high starts at raw 192 (about 75%) and is included in high.")
        elif section == "capacity":
            capacity = _one(rows("reports/admission_capacity/admission_capacity_summary.csv"), population_type="overall", population_id="all")
            values = [(label, _number(capacity.get(field), suffix, scale=scale, digits=digits))
                      for label, field, suffix, scale, digits in (
                          ("Mean advertised capacity", "mean_percent", "%", 1, 2),
                          ("Valid radio/bin readings", "valid_rows", "", 1, 0),
                          ("Admission-capacity coverage", "valid_coverage", "%", 100, 2),
                          ("Minimum advertised capacity", "minimum_percent", "%", 1, 2),
                          ("Maximum advertised capacity", "maximum_percent", "%", 1, 2))]
            caption = "The mean gives each valid radio/bin reading equal weight. Capacity percentages are relative to the field's maximum. Coverage is the share of expected radio/bin readings with valid capacity. The band-comparison tables instead weight admission-capacity readings by their duration."
        elif section == "stations":
            values = [("Mean stations per radio", number("mean_radio_station_count")),
                      ("Mean peak stations per enrolled", _number(enrolled_overall.get("average_peak_client_count_per_enrolled"))),
                      ("Maximum single-radio station count", number("maximum_radio_station_count", digits=0)),
                      ("Station coverage", number("station_coverage_percent", "%"))]
            caption = "Mean stations per radio weights complete station totals by their duration. Mean peak stations per enrolled divides the sum of included class-session classroom peaks by the sum of their combined enrollments; larger classes therefore carry more weight."
        elif section == "bands":
            values = [("6 GHz-enabled APs", _number(infra.get("six_ghz_enabled_ap_count"), digits=0)),
                      ("6 GHz-enabled APs percentage of all APs", _number(infra.get("six_ghz_enabled_ap_percent"), "%")),
                      ("Observed 5 GHz radios", _number(infra.get("observed_5_ghz_radio_count"), digits=0)),
                      ("Observed 6 GHz radios", _number(infra.get("observed_6_ghz_radio_count"), digits=0)),
                      ("Classrooms mixing AP enablement", _number(infra.get("mixed_classroom_count"), digits=0)),
                      ("6 GHz associated station-time", _number(use.get("5_6_six_station_share_percent"), "%")),
                      ("6 GHz associated station-time on enabled APs", _number(enabled_ap_use.get("5_6_six_station_share_percent"), "%"))]
            caption = "Station-time percentages use combined 5 and 6 GHz association-time; 6 GHz associated station-time includes all sampled infrastructure, while enabled APs includes only APs that have an enabled 6 GHz radio."
        elif section == "exposure":
            values = []
            for label, field, suffix, scale in (
                    ("Station-time with high CU", "high_exposure_percent", "%", 1),
                    ("Station-time with very high CU", "very_high_exposure_percent", "%", 1)):
                values.extend([(label + " observed", _number(exposure.get(field), suffix, scale=scale)),
                               (label + " possible", _possible_range(exposure, field, suffix, scale=scale))])
            values.extend((label, _number(exposure.get(field), suffix, scale=scale))
                      for label, field, suffix, scale in (
                          ("CU/station pair coverage", "pair_coverage_percent", "%", 1),
                          ("Station-count coverage", "station_coverage_percent", "%", 1),
                          ("CU coverage of available station-time", "known_station_time_cu_coverage_percent", "%", 1),
                          ("Paired radio time using assisted counts", "stale_pair_percent", "%", 1),
                          ("Association-hours with available station counts", "available_station_time_us", "", 1 / 3_600_000_000),
                          ("Association-hours with usable CU", "paired_station_time_us", "", 1 / 3_600_000_000)))
            for label, field in (("Association-hours with high CU", "high_station_time_us"),
                                 ("Association-hours with very high CU", "very_high_station_time_us")):
                values.extend([(label + " observed", _number(exposure.get(field), scale=1 / 3_600_000_000)),
                               (label + " possible", _possible_range(exposure, field, scale=1 / 3_600_000_000))])
            caption = ("An association-hour is one associated station counted for one hour. High includes very high. "
                       "Observed exposure percentages use only periods with both usable CU and station totals; pair coverage and "
                       "station-count coverage use expected radio time. CU coverage of available station-time weights usable CU "
                       "by associated station count as well as duration.")
        elif section == "quality":
            values = [("CU coverage", number("cu_coverage_percent", "%")),
                      ("Radio-hours with beacons but no usable CU", number("missing_cu_duration_us", scale=1 / 3_600_000_000)),
                      ("Radio-hours without a beacon", number("no_beacon_duration_us", scale=1 / 3_600_000_000)),
                      ("Station coverage", number("station_coverage_percent", "%"))]
            caption = "Radio-hours sum time across sampled radios and sessions. No-beacon time describes missing observations in the capture; it does not establish why a beacon was missing."
        elif section == "radio":
            pairs = _one(rows("reports/radio_class_detail/cu_station_correlation.csv"), population_type="overall", population_id="all")
            values = [("Radios in the relationship table", _number(pairs.get("radio_count"), digits=0)),
                      ("Class sessions", _number(pairs.get("class_session_count"), digits=0)),
                      ("Valid CU/station pairs", _number(pairs.get("valid_pair_rows"), digits=0)),
                      ("Valid-pair coverage", _number(pairs.get("valid_pair_coverage"), "%", scale=100))]
            caption = "A pair contains usable CU and a complete station count for the same radio and time bin. Pair coverage and the coefficients below count readings rather than weighting their duration."
        else:
            return ""
        return ('<h3>Reported metrics</h3>' if heading else '') + _metric_facts(values) + f'<p class="caption">{escape(caption)}</p>'

    def station_exposure_subsection():
        # Preserve the Overview's existing #exposure link as a subsection anchor.
        content = ['<div id="exposure" class="report-subsection"><h3>Station-time with high CU</h3>'
                   '<p>A radio with high channel utilization has the potential to affect more stations when there are more associated to it. '
                   'Station-time combines associated station counts with duration: 50 stations associated for 10 minutes contributes 500 association-minutes. '
                   'The metrics below report association-hours and the percentage of measured station-time with high or very high CU.</p>']
        plot = "reports/affected_stations/station_time_exposure.png"
        if not exposures and not safe(plot).is_file():
            content.append('<p class="notice">Station-time exposure was not generated for this run.</p>')
        else:
            content.append(section_metrics("exposure", heading=False))
            content.append('<p class="caption">High CU starts at raw 128 (about 50%); very high starts at raw 192 (about 75%) and is included in high. '
                           'Missing CU and unavailable station counts are excluded from observed exposure; accepted stale-assisted station counts participate.</p>')
            if "high_exposure_percent_lower_bound" in exposure:
                content.append('<p>Minimum and maximum exposure use all station-time with available station counts. '
                               'The minimum assumes CU-missing bins were below the threshold; the maximum assumes they were above it. '
                               'For example, 30 observed high-CU association-hours out of 80 with usable CU, plus 20 with missing CU, '
                               'gives 37.5% observed exposure and a possible range of 30% to 50% over the 100 available association-hours. '
                               'Periods with unknown station counts are outside these bounds. The ranges are not confidence intervals '
                               'or estimates of missing CU; full CU coverage of available station-time makes them coincide with observed exposure.</p>')
            if safe(plot).is_file():
                with Image.open(safe(plot)) as image:
                    width, height = image.size
                label = "Station-time exposure by band"
                content.append(f'<figure><h4>{label}</h4><p class="figure-note">Each bar shows the share of that band\'s paired station-time with the stated CU condition. '
                               'Where available, whiskers show minimum/maximum percentages over all station-time with known station counts, '
                               'allowing missing CU to range from 0% to 100%. An unavailable observed value has no bar. '
                               'Overall combines the sampled bands. Both panels share the same percentage scale.</p>'
                               f'<a href="{escape(href(plot))}" aria-label="{label}"><img src="{escape(href(plot))}" width="{width}" height="{height}" loading="lazy" alt="{label}"></a></figure>')
            links = [link(path, label) for path, label in (
                ("reports/affected_stations/population_exposure.csv", "Project, band and cohort station-time exposure"),
                ("reports/affected_stations/methodology.txt", "Station-time definitions and interpretation"),
            ) if safe(path).is_file()]
            if links:
                content.append('<div class="links">' + ''.join(links) + '</div>')
        return ''.join(content) + '</div>'

    def cdf_possible(scope, population, threshold, *, duration_weighted=False):
        source = time_cdf_bounds if duration_weighted else count_cdf_bounds
        row = dict(_one(source, population_scope=scope, population_id=population, cu_threshold_percent=str(threshold)))
        row["cdf_lower_bound"] = row.get("cdf_lower_bound_percent")
        row["cdf_upper_bound"] = row.get("cdf_upper_bound_percent")
        return _possible_range(row, "cdf", "%")

    def figure_metrics(filename):
        if filename == "matched_ap_cu.png":
            differences = []
            intervals = []
            for row in rows("reports/band_comparison/matched_ap_radio_sessions.csv"):
                try:
                    difference = float(row.get("matched_6_minus_5_cu_percentage_points"))
                    duration = float(row.get("common_valid_cu_duration_us"))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(difference) and math.isfinite(duration) and duration > 0:
                    differences.append(difference)
                    try:
                        interval = [float(row[f"paired_window_6_minus_5_cu_percentage_points_{bound}_bound"]) for bound in ("lower", "upper")]
                    except (KeyError, TypeError, ValueError):
                        interval = [math.nan, math.nan]
                    intervals.append(interval)
            mean = math.fsum(differences) / len(differences) if differences else None
            limits = {f"delta_{bound}_bound": (math.fsum(pair[index] for pair in intervals) / len(intervals)
                      if intervals and all(math.isfinite(pair[index]) for pair in intervals) else None)
                      for index, bound in enumerate(("lower", "upper"))}
            return _metric_facts([
                ("Mean matched 6−5 GHz CU observed (percentage points)", _number(mean)),
                ("Complete paired-window mean difference possible (percentage points)", _possible_range(limits, "delta")),
                ("Usable radio-pair/class-session comparisons", _number(len(differences), digits=0)),
            ]) + '<p class="caption">Each usable radio-pair/class-session comparison has equal weight in this mean. Within each comparison, the two mean CU values weight their common valid bins by duration. A negative difference means lower observed CU on 6 GHz; comparisons without common valid CU are excluded. The separate complete paired-window range uses those same eligible comparisons and equal weights, but allows time without a common valid pair to have any CU from 0% to 100% on each radio.</p>'
        if filename == "classroom_streak_distributions.png":
            values = [(label, _number(overall.get(field), " s", scale=1 / 1_000_000)) for label, field in (
                ("Longest observed high-CU streak", "maximum_high_actual_observed_streak_us"),
                ("Longest possible-connected high-CU streak", "maximum_high_possible_connected_streak_us"),
                ("Longest observed very-high-CU streak", "maximum_very_high_actual_observed_streak_us"),
                ("Longest possible-connected very-high-CU streak", "maximum_very_high_possible_connected_streak_us"))]
            return _metric_facts(values) + '<p class="caption">These four values are the longest single-radio streaks across all analyzed class sessions. The histograms show how the classroom class-session maxima are distributed.</p>'
        if filename == "radio_bin_cu_cdf.png":
            low, lower, _ = _cu_cdf_values(cu_distribution, "overall", "all")
            return _metric_facts([("Readings at or below 50% CU", _number(low, "%")),
                                  ("Possible readings at or below 50% CU", cdf_possible("overall", "all", 50)),
                                  ("Readings at or below 75% CU", _number(lower, "%")),
                                  ("Possible readings at or below 75% CU", cdf_possible("overall", "all", 75))])
        if filename == "radio_bin_cu_cdf_by_band.png":
            data = []
            for band in ("2.4", "5", "6"):
                low, lower, count = _cu_cdf_values(cu_distribution, "band", band)
                evidence = _one(count_cdf_bounds, population_scope="band", population_id=band, cu_threshold_percent="50")
                if (count is not None and count > 0) or float(evidence.get("expected_weight") or 0) > 0:
                    data.append([f"{band} GHz", _number(low, "%"), cdf_possible("band", band, 50),
                                 _number(lower, "%"), cdf_possible("band", band, 75), _number(count if count is not None else 0, digits=0)])
            return table(["Band", "Readings at or below 50% CU", "Possible readings at or below 50% CU",
                          "Readings at or below 75% CU", "Possible readings at or below 75% CU", "Valid readings"], data) if data else ""
        if filename == "cu_cohort_cdf.png":
            scopes = [("ap_cohort", "AP deployment")]
            try:
                mixed = int(infra.get("mixed_classroom_count") or 0) > 0
            except (TypeError, ValueError):
                mixed = False
            if mixed:
                scopes.append(("classroom_cohort", "Classroom deployment"))
            data = []
            for scope, context_label in scopes:
                for population, label in (("5_on_without_6", "5 GHz without 6 GHz"), ("5_on_with_6", "5 GHz with 6 GHz"), ("6_on_with_6", "6 GHz with 6 GHz")):
                    low, lower, duration = _cu_cdf_values(cu_distribution, scope, population, weight="valid_radio_duration_us")
                    evidence = _one(time_cdf_bounds, population_scope=scope, population_id=population, cu_threshold_percent="50")
                    if (duration is not None and duration > 0) or float(evidence.get("expected_weight") or 0) > 0:
                        data.append([escape(context_label), escape(label), _number(low, "%"), cdf_possible(scope, population, 50, duration_weighted=True),
                                     _number(lower, "%"), cdf_possible(scope, population, 75, duration_weighted=True)])
            return table(["Grouping", "Radio group", "Radio time at or below 50% CU", "Possible radio time at or below 50% CU",
                          "Radio time at or below 75% CU", "Possible radio time at or below 75% CU"], data) if data else ""
        return ""

    for section, title, directory, description, plots in sorted(FAMILIES, key=lambda family: [item[0] for item in nav].index(family[0])):
        parts.append(f'<section id="{section}"><h2>{escape(title)}</h2><p>{escape(description)}</p>')
        if section in SECTION_GUIDES:
            parts.append(f'<p>{escape(SECTION_GUIDES[section])}</p>')
        folder = safe(f"reports/{directory}")
        if not folder.is_dir() and not inventories[section]:
            parts.append('<p class="notice">This report family was not generated for this run.</p>')
            if section == "cu":
                parts.append(station_exposure_subsection())
            parts.append('</section>')
            continue
        if section == "quality":
            anomalies = [row for row in rows("reports/beacon_quality/beacon_cadence_sessions.csv")
                         if row.get("metric_id") == "beacon.metric/beacon-capture-quality/v2" and row.get("capture_family") == "native" and row.get("tsf_status") in {"discontinuous", "reversing", "time_mismatch"}]
            if anomalies:
                parts.append(f'<p class="notice">Native TSF clock anomalies were detected in {len(anomalies)} BSSID/class-session streams. Measurements and capture slots use observation timestamps. Clock anomalies did not move measurements or exclude beacons from their observed class windows. Source capture/frame references and timing differences are recorded in the beacon cadence table.</p>')
            parts.append(_cadence_description(configuration))
        parts.append(section_metrics(section))
        if section == "capacity":
            relationships = rows("reports/admission_capacity/admission_capacity_correlation.csv")
            data = [[escape({"capacity_vs_cu": "Capacity and CU", "capacity_vs_station": "Capacity and station count"}.get(row.get("relationship"), row.get("relationship", ""))),
                     _number(row.get("pearson_correlation"), digits=3), _number(row.get("spearman_correlation"), digits=3),
                     _number(row.get("valid_pair_rows"), digits=0), _number(row.get("valid_pair_coverage"), "%", scale=100)]
                    for row in relationships if row.get("population_type") == "overall" and row.get("population_id") == "all"]
            if data:
                parts.append('<h3>Admission-capacity relationships</h3>' + table(["Relationship", "Pearson r", "Spearman ρ (rho)", "Valid pairs", "Pair coverage"], data))
                parts.append('<p class="caption">Each pair uses the same radio and time bin. Pearson r measures linear association; Spearman ρ (rho) measures whether values rise or fall together in rank. '
                             'Both range from −1 to +1: positive values indicate that the two measurements tend to rise together, negative values indicate opposite directions, '
                             'and values near zero indicate little of that type of association. These are correlation coefficients, not p-values. The detailed source table also separates bands.</p>')
        if section == "bands" and populations:
            cohorts = [(label, row) for population, label in AP_COHORTS
                       if (row := _one(populations, population_scope="ap_cohort", population_id=population))
                       and row.get("status") != "no_matching_radios"]
            if cohorts:
                data = [[escape(label), *[_number(row.get(field), digits=0) for field in
                         ("ap_count", "radio_count", "classroom_count", "class_session_count")]]
                        for label, row in cohorts]
                parts.append('<h3>Comparison cohort sample sizes</h3><p>These are distinct observed APs, radios, classrooms and class sessions within each AP infrastructure cohort. '
                             'The same AP, classroom or class session can appear in more than one band row; counts across rows are not additive.</p>'
                             + table(["AP infrastructure cohort", "APs", "Radios", "Classrooms", "Class sessions"], data))
            data = [[f"{band} GHz", _number(row.get("mean_cu_percent"), "%"), _possible_range(row, "mean_cu_percent", "%"), _number(row.get("cu_coverage_percent"), "%"),
                     _number(row.get("mean_radio_station_count")), _number(row.get("station_coverage_percent"), "%"),
                     _number(row.get("mean_admission_capacity_percent"), "%")]
                    for band in ("2.4", "5", "6")
                    if (row := _one(populations, population_scope="band", population_id=band)) and row.get("status") != "no_matching_radios"]
            if data:
                parts.append('<h3>Overall results by band</h3>' + table(["Band", "Mean CU observed", "Mean CU possible", "CU coverage", "Mean stations/radio", "Station coverage", "Mean admission capacity"], data))
            data = []
            for label, row in cohorts:
                data.append([escape(label), _number(row.get("mean_cu_percent"), "%"), _possible_range(row, "mean_cu_percent", "%"), _number(row.get("cu_coverage_percent"), "%"),
                             _number(row.get("mean_radio_station_count")), _number(row.get("station_coverage_percent"), "%"),
                             _number(row.get("mean_admission_capacity_percent"), "%")])
            if data:
                parts.append('<h3>Results by AP infrastructure cohort</h3>' + table(["AP infrastructure cohort", "Mean CU observed", "Mean CU possible", "CU coverage", "Mean stations/radio", "Station coverage", "Mean admission capacity"], data))
            parts.append('<p class="caption">Mean CU and mean admission-capacity values each weight valid readings by their duration; mean station values use periods with complete station totals, including accepted assisted counts. Mean admission capacity is expressed as a percentage of the field\'s defined maximum allowance, not available throughput. AP/classroom cohort views coincide in uniform rooms; mixed rooms can differ. All sampled bands, including 2.4 GHz, remain in the source tables.</p>')
        if section == "bands":
            parts.append('<p class="caption">CU values labeled observed use valid measurements. Their minimum/maximum ranges allow missing CU to be 0% to 100% over the same expected radio time. Ranges do not remove capture bias within bins that contain a reading. Missing supporting components leave bounds unavailable.</p>')
            models = [row for row in rows("reports/band_comparison/model_population_summary.csv")
                      if row.get("population_scope") == "band" and row.get("status") != "no_matching_radios"]
            if models:
                models.sort(key=lambda row: (row.get("ap_model", ""), row.get("ap_observed_5_ghz_radio_count", ""),
                                            row.get("ap_observed_6_ghz_radio_count", ""), row.get("population_id", "")))
                data = [[escape(row.get("ap_model") or "Unavailable"),
                         _number(row.get("ap_observed_5_ghz_radio_count"), digits=0) + " / " + _number(row.get("ap_observed_6_ghz_radio_count"), digits=0),
                         escape(row.get("population_id", "") + " GHz"),
                         *[_number(row.get(field), digits=0) for field in ("ap_count", "radio_count", "class_session_count")],
                         _number(row.get("mean_cu_percent"), "%"), _possible_range(row, "mean_cu_percent", "%"), _number(row.get("cu_coverage_percent"), "%"),
                         _number(row.get("mean_radio_station_count")), _number(row.get("mean_admission_capacity_percent"), "%")]
                        for row in models]
                parts.append('<details class="model-summary"><summary>AP-model summary</summary><p>Each row describes one band on an AP model with the stated sampled 5/6 GHz radio configuration. '
                             'Counts are distinct within the row; the same APs and class sessions can contribute to multiple bands. Mean CU and mean admission capacity both weight valid readings by duration. Mean stations/radio weights complete station counts by duration, including accepted assisted counts. '
                             'The sampled radio configuration is not a complete inventory of the model\'s hardware.</p>'
                             + table(["AP model", "Sampled radios/AP (5 / 6 GHz)", "Band", "APs", "Radios", "Class sessions", "Mean CU observed", "Mean CU possible", "CU coverage", "Mean stations/radio", "Mean admission capacity"], data)
                             + '<div class="links">' + link("reports/band_comparison/model_population_summary.csv", "Full AP-model metrics and coverage") + '</div></details>')
        if section == "quality":
            associations = rows("reports/beacon_quality/beacon_quality_associations.csv")
            if associations:
                data = [[escape(row.get("capture_family", "").upper()), escape(row.get("ap_model", "") or "Unavailable"),
                         escape(row.get("band", "") + " GHz"),
                         _number(row.get("observed_ap_5ghz_radio_groups"), digits=0) + " / " + _number(row.get("observed_ap_6ghz_radio_groups"), digits=0),
                         escape({"previous_window_cu_mean_percent": "Previous-window mean CU", "previous_window_station_mean": "Previous-window station count"}.get(row.get("predictor"), row.get("predictor", ""))),
                         _number(row.get("pearson_correlation"), digits=3), _number(row.get("paired_windows"), digits=0),
                         _number(row.get("paired_window_fraction"), "%", scale=100)] for row in associations]
                parts.append('<h3>Capture-slot availability relationships</h3><p>These existing coefficients relate previous-window CU or station count to the following window\'s empty-slot fraction. '
                             'Results are separated by capture family, AP model, band and sampled radio configuration.</p>' + table(
                                 ["Capture family", "AP model", "Band", "Sampled radios per AP (5 / 6 GHz)", "Context measurement", "Pearson r with empty-slot fraction", "Paired windows", "Windows with usable context"], data))
        if section == "stations":
            parts.append('<h3>Associations per radio</h3><p>These are time-weighted mean counts per radio, and the maximum is the highest single radio/bin count. Coverage shows how much expected radio time had a complete count.</p>')
            data = []
            for scope, population, label in (("overall", "all", "Overall"), ("band", "2.4", "2.4 GHz"), ("band", "5", "5 GHz"), ("band", "6", "6 GHz")):
                row = _one(populations, population_scope=scope, population_id=population)
                if scope == "band" and (not row or row.get("status") == "no_matching_radios"):
                    continue
                data.append([label, _number(row.get("mean_radio_station_count")),
                             _number(row.get("maximum_radio_station_count"), digits=0), _number(row.get("station_coverage_percent"), "%")])
            parts.append(table(["Population", "Mean stations/radio", "Maximum single-radio count", "Station coverage"], data))
            parts.append('<h3>Peak stations per enrolled</h3><p>The project mean is the sum of eligible class-session classroom peaks divided by the sum of their combined enrollments. '
                         'For example, 1.5 means 1.5 peak associations per enrolled student, not average concurrent devices per attendee. '
                         'Zero/missing enrollment is excluded. Each SSID/band row uses its own eligible sessions; their independently timed peaks and ratios should not be added.</p>')
            data = []
            for row in enrollment:
                kind, population = row.get("population_type"), row.get("population_id", "")
                label = "Overall" if kind == "overall" else f"{kind.upper() if kind == 'ssid' else 'Band'}: {population}" + (" GHz" if kind == "band" else "")
                data.append([escape(label), _number(row.get("average_peak_client_count_per_enrolled")),
                             _number(row.get("eligible_session_count"), digits=0), _number(row.get("stale_assisted_peak_session_count"), digits=0)])
            if data:
                parts.append(table(["Population", "Mean peak stations/enrolled", "Eligible class sessions", "Stale-assisted peak sessions"], data))
            else:
                parts.append('<p class="notice">Enrollment comparison values are unavailable for this run.</p>')
        if section == "radio":
            correlations = rows("reports/radio_class_detail/cu_station_correlation.csv")
            parts.append('<h3>Measured association: Pearson r and Spearman ρ</h3><p>Positive values mean higher station counts tend to coincide with higher CU. '
                         'Pearson r measures linear association; Spearman ρ measures whether the variables tend to rise together in rank. '
                         'Values near +1 or −1 indicate strong corresponding association; values near zero indicate little of that type of association. '
                         'Neither establishes that these clients caused the CU.</p>')
            data = []
            for row in correlations:
                label = "Overall" if row.get("population_type") == "overall" else row.get("population_id", "") + " GHz"
                data.append([escape(label), _number(row.get("pearson_correlation"), digits=3), _number(row.get("spearman_correlation"), digits=3),
                             _number(row.get("valid_pair_rows"), digits=0), _number(row.get("valid_pair_coverage"), "%", scale=100),
                             _number(row.get("stale_assisted_pair_rows"), digits=0)])
            if data:
                parts.append(table(["Population", "Pearson r", "Spearman ρ", "Valid pairs", "Pair coverage", "Stale-assisted pairs"], data))
            else:
                parts.append('<p class="notice">Correlation values are unavailable for this run.</p>')
            parts.append('<p class="caption">These coefficients use one count per valid radio/bin pair, not duration weighting. '
                         'Pooling can reflect differences among bands, radios and classrooms. Constant/insufficient data give unavailable coefficients. '
                         'The density plots summarize observed pairs; no regression model is fitted.</p>')
        parts.append('<div class="links">')
        for filename in ("population_summary.csv", "cu_distribution.csv", "model_population_summary.csv", "station_adoption_summary.csv", "station_aggregate_report.csv", "cu_station_correlation.csv", "admission_capacity_summary.csv", "admission_capacity_correlation.csv", "beacon_quality_associations.csv", "methodology.txt"):
            relative = f"reports/{directory}/{filename}"
            if safe(relative).is_file():
                parts.append(link(relative))
        if section == "stations" and safe("reports/band_comparison/population_summary.csv").is_file():
            parts.append(link("reports/band_comparison/population_summary.csv", "Radio station mean/maximum/coverage source"))
        if section == "cu":
            for relative, label in (("reports/band_comparison/population_summary.csv", "CU means, durations and streak maxima"),
                                    ("reports/band_comparison/cu_distribution.csv", "CU distribution counts and radio-time components")):
                if safe(relative).is_file():
                    parts.append(link(relative, label))
        if section == "quality" and safe("reports/band_comparison/population_summary.csv").is_file():
            parts.append(link("reports/band_comparison/population_summary.csv", "CU/station availability components"))
        if section in {"bands", "quality"}:
            for relative, label in (("reports/cu_coverage_streak/radio_class_report.csv", "Radio/class-session mean CU and coverage source"),
                                    ("reports/cu_coverage_streak/methodology.txt", "CU figure and coverage definitions")):
                if safe(relative).is_file():
                    parts.append(link(relative, label))
        if section == "bands":
            for relative, label in (("reports/admission_capacity/admission_capacity_summary.csv", "Overall and per-band admission-capacity metrics"),
                                    ("reports/admission_capacity/admission_capacity_correlation.csv", "Admission-capacity correlations by band"),
                                    ("reports/admission_capacity/methodology.txt", "Admission-capacity definitions"),
                                    ("reports/station_count/station_bin_rollups.parquet", "Classroom/bin station histogram source"),
                                    ("reports/station_count/methodology.txt", "Station-total definitions"),
                                    ("reports/band_comparison/matched_ap_radio_sessions.csv", "Matched radio-pair/class-session CU values"),
                                    ("reports/radio_class_detail/cu_station_correlation.csv", "CU/station correlation coefficients by band")):
                if safe(relative).is_file():
                    parts.append(link(relative, label))
        parts.append('</div>')
        if plots:
            parts.append('<div class="figures">')
            previous_group = None
            for filename in plots:
                relative = f"reports/{directory}/{filename}"
                if safe(relative).is_file():
                    label = _label(filename)
                    with Image.open(safe(relative)) as image:
                        width, height = image.size
                    note = FIGURE_NOTES.get(Path(filename).name, "See this report family's methodology for definitions, population and coverage.")
                    heading = "h3"
                    if section == "bands":
                        group = next(title for title, names in BAND_FIGURE_GROUPS if Path(filename).name in names)
                        if group != previous_group:
                            parts.append(f'<h3 class="figure-group-title">{escape(group)}</h3>')
                            previous_group = group
                        heading = "h4"
                    parts.append(f'<figure><{heading}>{escape(label)}</{heading}><p class="figure-note">{escape(note)}</p>' + figure_metrics(Path(filename).name)
                                 + f'<a href="{escape(href(relative))}" aria-label="{escape(label)}"><img src="{escape(href(relative))}" width="{width}" height="{height}" loading="lazy" alt="{escape(label)}"></a></figure>')
            parts.append('</div>')
        if section == "cu":
            parts.append(station_exposure_subsection())
        parts.append(inventory_html(section) + '</section>')

    parts.append(_session_section(root, rows, safe, link, table, session_stats, timezone_name, inventory=inventory_html("sessions")))

    parts.append('<section id="evidence"><h2>Supporting evidence</h2><p>Use the manifest for input hashes, effective configuration, capture plans, versions and processing stages. '
                 'CSV exports are review copies; Parquet artifacts and the manifest remain authoritative.</p>'
                 '<p>The counts below summarize the recorded analysis inputs and processing stages. The manifest and CSV links provide provenance, '
                 'AP/radio identity and the radio/classroom class-session summaries. The file inventory also includes the saved run summary.</p>')
    parts.append(_metric_facts([
        ("Recorded input files", _number(len(context["input_set"]["files"]) if isinstance(context.get("input_set", {}).get("files"), list) else None, digits=0)),
        ("Capture files", _number(configuration.get("capture_count"), digits=0)),
        ("Recorded processing stages", _number(processing_stage_count, digits=0)),
        ("Selected beacon observations", _number(configuration.get("bounded_working_set", {}).get("selected_rows"), digits=0)),
    ]))
    parts.append('<div class="links">')
    parts.append(link("manifest.json", "Run manifest and provenance"))
    for relative in ("exports/csv/project_identity_map.csv", "exports/csv/radio_class_summary.csv", "exports/csv/classroom_class_summary.csv"):
        if safe(relative).is_file():
            parts.append(link(relative))
    parts.append('</div><p class="caption">This overview displays existing metrics and documented compact-summary rollups; it does not decode beacons or change source metrics. Keep the run folder together so relative links continue to work. '
                 'CSV links open or download according to your browser settings; interactive filtering and cross-project browsing are not included.</p>' + inventory_html("evidence") + '</section></main>'
                 '<footer>Beacon Analysis · Derived run overview · <a href="#overview">Back to overview</a></footer>'
                 f'<script>{NAVIGATION_SCRIPT}</script></body></html>')
    text = "\n".join(parts)
    validate_overview_links(text, root, manifest_pending=manifest_pending)
    destination.parent.mkdir(parents=True, exist_ok=True)
    created = False
    try:
        with destination.open("x", encoding="utf-8", newline="\n") as stream:
            created = True
            stream.write(text)
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise
    return destination


def _room_heading_context(members, session_stats):
    """Collapse stable context without hiding variable/missing source counts."""
    labels = []
    for field, singular, plural in (("ap_count", "AP", "APs"), ("radio_count", "radio", "radios")):
        values = [session_stats.get(row["class_session_id"], {}).get(field) for row in members]
        known = sorted({value for value in values if value is not None})
        if not known:
            labels.append(f"{plural} unavailable")
        elif len(known) == 1:
            labels.append(f"{known[0]} {singular if known[0] == 1 else plural}" +
                          (" (some sessions unavailable)" if any(value is None for value in values) else ""))
        else:
            labels.append(f"{known[0]}–{known[-1]} {plural} (varies by session)" +
                          ("; some unavailable" if any(value is None for value in values) else ""))
    capacities = set()
    for row in members:
        try:
            value = float(row.get("room_capacity"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value >= 0:
            capacities.add(value)
    if len(capacities) > 1 or any(row.get("capacity_status") == "conflict" for row in members):
        labels.append("Seating capacity conflicting")
    elif capacities:
        value = next(iter(capacities))
        labels.append(_number(value, digits=0 if value.is_integer() else 2) + " seats")
    return " · ".join(labels)


def _session_section(root, rows, safe, link, table, session_stats, timezone_name, *, inventory=""):
    sessions = rows("reports/class_sessions/class_session_report.csv")
    room_bands = {}
    for row in rows("reports/band_comparison/classroom_band_summary.csv"):
        if row.get("status") != "no_matching_radios":
            room_bands.setdefault(row.get("classroom"), []).append(row)
    plots = rows("reports/radio_class_detail/radio_class_plot_index.csv")
    plot_groups = {}
    for plot in plots:
        relative = "reports/radio_class_detail/" + plot["path"]
        safe(relative)  # Validate all index paths, including disabled/missing links.
        if not safe(relative).is_file():
            raise ValueError("optional radio plot index references a missing file")
        plot_groups.setdefault(plot["class_session_id"], []).append((relative, plot["radio_group_id"]))
    parts = ['<section id="sessions"><h2>Classrooms and class sessions</h2><p>Expand a classroom to review its class sessions. A class session is a scheduled analysis period, including a class or event; a classroom is the configured room or monitored space. '
             'The summary counts describe the scheduled sample; each classroom table lists class times, enrollment, mean radio CU, peak station counts and coverage, with optional radio time-series links. '
             f'Local dates/times use {escape(timezone_name or "the schedule timezone")} with date-specific DST offsets. '
             'Peak counts include accepted stale assistance; enrollment combines simultaneous sections. '
             'Classroom headings show observed AP/radio counts and seating capacity when known; variable or unavailable counts are identified. '
             'Mean radio CU pools valid radio time; it is not a merged classroom/channel CU reading. '
             'Each room\'s band summary pools measurements across its analyzed class sessions: mean CU and mean admission capacity both weight valid readings by duration, while mean station values use the duration with complete counts, including accepted stale assistance. '
             'Radio time-series links open the radio/class-session graphs when generated.</p>']
    parts.append('<p class="caption">CU minimum/maximum values allow missing measurements to range from 0% to 100% over expected radio time. Individual class-session means and bounds pool each radio\'s exact sums and durations; they do not average radio coverage percentages.</p>')
    population = _one(rows("reports/band_comparison/population_summary.csv"), population_scope="overall", population_id="all")
    parts.append(_metric_facts([
        ("Classrooms", _number(len({row["classroom"] for row in sessions}) if sessions else None, digits=0)),
        ("Scheduled class sessions", _number(len(sessions) if sessions else None, digits=0)),
        ("Class sessions with measurements", _number(population.get("class_session_count"), digits=0)),
        ("Scheduled class-session hours", _number(_scheduled_hours(sessions))),
    ]))
    if not sessions:
        parts.append('<p class="notice">Class-session detail was not generated for this run.</p>')
    for room in sorted({row["classroom"] for row in sessions}):
        members = sorted((row for row in sessions if row["classroom"] == room), key=lambda row: (row.get("class_start_utc", ""), row["class_session_id"]))
        context = _room_heading_context(members, session_stats)
        parts.append(f'<details class="room"><summary>{escape(room)} <span class="caption">· {len(members)} class sessions · {escape(context)}</span></summary>')
        if all(row.get("radio_count_total") in {"0", "0.0"} for row in members):
            parts.append('<p class="notice">Measurements unavailable: no radios were resolved for this classroom from the configured AP names and input captures. Check AP names, capture coverage and classroom mappings.</p>')
        band_rows = sorted(room_bands.get(room, []), key=lambda row: row.get("band", ""))
        if band_rows:
            data = [[escape(row.get("band", "") + " GHz"), _number(row.get("class_session_count"), digits=0),
                     _number(row.get("mean_cu_percent"), "%"), _possible_range(row, "mean_cu_percent", "%"), _number(row.get("cu_coverage_percent"), "%"),
                     _number(row.get("mean_radio_station_count")), _number(row.get("station_coverage_percent"), "%"),
                     _number(row.get("mean_admission_capacity_percent"), "%")] for row in band_rows]
            parts.append('<h4>Band summary across class sessions</h4>' + table(
                ["Band", "Class sessions", "Mean CU observed", "Mean CU possible", "CU coverage", "Mean stations/radio", "Station coverage", "Mean admission capacity"], data)
                         + '<h4>Individual class sessions</h4>')
        data = []
        for row in members:
            plot_links = plot_groups.get(row["class_session_id"], [])
            stats = session_stats.get(row["class_session_id"], {})
            available = '<ul class="radio-plot-links">' + "".join(f'<li>{link(path, label)}</li>' for path, label in plot_links) + '</ul>' if plot_links else 'Not generated'
            data.append([escape(row.get("local_date", "Unavailable")), escape(row.get("local_start", "Unavailable") + "–" + row.get("local_end", "Unavailable")),
                         escape(row.get("course_labels", "") or "Unavailable"), _number(row.get("combined_enrollment"), digits=0),
                         _number(stats.get("mean_radio_cu_percent"), "%"), _possible_range(stats, "mean_radio_cu_percent", "%"), _number(row.get("peak_client_count"), digits=0),
                         _number(row.get("radio_cu_coverage_min"), "%", scale=100), available])
        parts.append(table(["Local date", "Local time", "Course / event", "Enrollment", "Mean radio CU observed", "Mean radio CU possible", "Peak stations", "Min radio CU coverage", "Radio time series"], data))
        parts.append('</details>')
    parts.append('<div class="links">')
    for relative, label in (("reports/class_sessions/class_session_report.csv", "Full class-session table and coverage"),
                            ("reports/band_comparison/classroom_band_summary.csv", "Full classroom-by-band metrics and coverage"),
                            ("reports/class_sessions/methodology.txt", "Class-session definitions"),
                            ("reports/band_comparison/radio_class_comparison.csv", "Radio/session source for AP counts and pooled mean CU"),
                            ("reports/class_sessions/enrollment_vs_peak_client_count.png", "Enrollment versus peak station count"),
                            ("reports/class_sessions/peak_client_count_per_enrolled_histogram.png", "Peak stations per enrolled distribution"),
                            ("reports/radio_class_detail/radio_class_plot_index.csv", "Complete optional radio plot index")):
        if safe(relative).is_file():
            parts.append(link(relative, label))
    if not plots:
        parts.append('<p class="caption">Optional radio/session plots were not generated. Future runs can enable RADIO_CLASS_PLOTS or --radio-class-plots.</p>')
    parts.append('</div>' + inventory + '</section>')
    return "\n".join(parts)


CSS = """
:root{color-scheme:light;--ink:#1d2d3e;--muted:#536579;--line:#dbe3ec;--blue:#165a96;--paper:#fff}
*{box-sizing:border-box}body{margin:0;background:#f4f7fa;color:var(--ink);font:16px/1.55 system-ui,-apple-system,Segoe UI,sans-serif}
header,nav,main,footer{max-width:1180px;margin:auto}header{padding:34px 26px 22px}h1{font-size:2rem;line-height:1.2;margin:4px 0 10px}
.eyebrow{color:var(--blue);font-weight:650;letter-spacing:.04em;font-size:.85rem}.run-id{font-family:ui-monospace,monospace;font-size:.85rem;overflow-wrap:anywhere;color:var(--muted)}
nav{display:flex;flex-wrap:wrap;gap:4px 16px;width:max-content;max-width:calc(100% - 24px);padding:10px 16px;border-block:1px solid var(--line);background:var(--paper);position:sticky;top:0;z-index:4;font-size:.9rem}
nav a{flex:none;white-space:nowrap;text-decoration:none;padding:5px 2px;border-bottom:3px solid transparent;font-weight:600}nav a[aria-current="location"]{background:#edf5fc;border-bottom-color:var(--blue);border-radius:3px 3px 0 0}
a{color:var(--blue);text-underline-offset:3px}a:hover{color:#123c63}a:focus-visible,summary:focus-visible{outline:3px solid #d68a19;outline-offset:4px}
main{padding:22px 26px}section{scroll-margin-top:var(--nav-offset,80px);margin-bottom:26px;padding:24px;background:var(--paper);border:1px solid var(--line);border-radius:8px}
h2{font-size:1.35rem;margin:0 0 12px}h3{font-size:.95rem;margin:0;font-weight:600}p{margin:8px 0 16px}.cards{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-top:22px}
.overview-subsection{margin-top:26px}.overview-subsection>h3{font-size:1.1rem;margin:0 0 10px}.overview-notes{border-top:1px solid var(--line);margin-top:24px;padding-top:18px}.overview-notes h4{font-size:1rem;margin:0 0 12px}.card h4{font-size:.95rem;margin:0;font-weight:600}
.card{padding:17px;background:#f5f9fd;border-top:3px solid #5794c5;border-radius:4px}.card p{color:var(--muted);font-size:.85rem;margin-bottom:0}.card .value{color:var(--ink);font-size:1.9rem;font-weight:650;margin:8px 0}
.links{display:flex;flex-wrap:wrap;gap:8px 20px;margin:16px 0;font-size:.9rem}.caption,figcaption{color:var(--muted);font-size:.86rem}.table-wrap{overflow-x:auto;margin:18px 0}table{border-collapse:collapse;width:100%;font-size:.9rem}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}th{background:#f1f5fa;font-weight:600}tbody tr:hover{background:#f8fafc}
.figures{display:grid;grid-template-columns:1fr;gap:32px;margin:24px 0}figure{margin:0}figure h3,figure h4{font-size:1.1rem;margin:0;font-weight:600}.figure-group-title{font-size:1.2rem;border-bottom:1px solid var(--line);padding-bottom:10px;margin-top:12px}.figure-note{font-size:.94rem;color:var(--muted);max-width:90ch;margin:8px 0 14px}img{width:100%;height:auto;border:1px solid var(--line)}
.facts{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:20px 0}.facts div{border-left:2px solid var(--line);padding-left:12px}.facts dt{font-size:.85rem;color:var(--muted)}.facts dd{margin:3px 0;font-size:1.25rem;font-weight:600}
details{margin-top:14px;border-top:1px solid var(--line);padding-top:12px}summary{cursor:pointer;color:var(--blue);font-weight:600}.inventory{columns:2;padding-left:22px;overflow-wrap:anywhere}.inventory li{padding:4px 0;break-inside:avoid}.filetype{font-size:.7rem;color:var(--muted);padding-left:6px}
.room{padding-bottom:8px}.room .caption{font-weight:400}.notice{border-left:3px solid #c48b33;padding:9px 14px;background:#fff9ed}footer{padding:4px 26px 34px;color:var(--muted);font-size:.85rem}
.report-subsection{margin-top:32px;scroll-margin-top:var(--nav-offset,80px)}.report-subsection>h3{font-size:1.1rem;margin-bottom:8px}.radio-plot-links{list-style:none;margin:0;padding:0;overflow-wrap:anywhere}.radio-plot-links li+li{margin-top:6px}
.skip{position:absolute;left:-9999px}.skip:focus{left:12px;top:12px;background:white;padding:10px;z-index:2}
.overview-subsection>h3{scroll-margin-top:var(--nav-offset,80px)}
@media(max-width:800px){.cards,.facts{grid-template-columns:repeat(2,minmax(0,1fr))}.inventory{columns:1}}
@media(max-width:480px){header{padding:24px 16px}main{padding:16px 10px}section{padding:18px 14px}.cards{grid-template-columns:1fr}nav{width:100%;max-width:100%;padding:10px 12px;gap:4px 12px}h1{font-size:1.7rem}.card .value{font-size:1.7rem}}
@media print{body{background:white}nav,.skip{display:none}main{padding:0}section{break-inside:avoid;border:0}.figures{grid-template-columns:1fr}a{color:inherit}footer{padding:0}}
"""


NAVIGATION_SCRIPT = """
(() => {
  const nav = document.querySelector('nav[aria-label="Report sections"]');
  if (!nav) return;
  const links = Array.from(nav.querySelectorAll('a[href^="#"]'));
  const sections = links.map(link => document.getElementById(link.hash.slice(1)));
  let pending = false;
  let current = null;
  function update() {
    pending = false;
    const offset = nav.getBoundingClientRect().height + 16;
    document.documentElement.style.setProperty('--nav-offset', offset + 'px');
    let active = 0;
    sections.forEach((section, index) => {
      if (section && section.getBoundingClientRect().top <= offset + 2) active = index;
    });
    if (window.scrollY + window.innerHeight >= document.documentElement.scrollHeight - 2) active = links.length - 1;
    if (active !== current) {
      current = active;
      links.forEach((link, index) => {
        if (index === active) link.setAttribute('aria-current', 'location');
        else link.removeAttribute('aria-current');
      });
    }
  }
  function schedule() {
    if (!pending) { pending = true; window.setTimeout(update, 40); }
  }
  window.addEventListener('scroll', schedule, {passive: true});
  window.addEventListener('resize', schedule);
  window.addEventListener('hashchange', schedule);
  window.addEventListener('load', schedule);
  update();
})();
"""
