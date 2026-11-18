"""Exploratory, bounded capture-slot diagnostics; never imputes CU or RF loss."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import shutil
from typing import Iterable

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from corrected_beacon_selection import (
    BeaconSelectionResult, preferred_observation_times,
    select_bssid_session_observations, TSF_DIAGNOSTIC_FIELDS,
)
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID, cu_mean_bounds_fields


METRIC_ID = "beacon.metric/beacon-capture-quality/v2"
REPORT_SCHEMA_ID = "beacon.schema/beacon-quality-reports/v2"
REPORT_DIRECTORY = Path("reports") / "beacon_quality"
STREAM_FILENAME = "beacon_cadence_sessions.csv"
WINDOW_FILENAME = "beacon_quality_windows.parquet"
ASSOCIATION_FILENAME = "beacon_quality_associations.csv"
STRATA_FILENAME = "beacon_quality_covariate_bands.csv"
GAP_FILENAME = "beacon_slot_gap_counts.csv"
METHODOLOGY_FILENAME = "methodology.txt"
PLOT_FILENAMES = (
    "beacon_slot_gap_cdf.png", "cu_capture_slot_availability.png",
    "station_capture_slot_availability.png",
)

STREAM_COLUMNS = (
    "run_id", "metric_id", "class_session_id", "classroom", "radio_group_id",
    "ap_id", "ap_model", "band", "bssid", "ssid", "capture_family",
    "capture_profiles", "source_captures", "observation_role", "listening_exposure_known",
    "expected_beacon_interval_tu", "advertised_intervals_tu", "interval_mismatch_rows",
    "interval_missing_rows", "class_duration_us", "capture_slot_width_us", "raw_views",
    "tsf_status", "known_tsf_duplicate_views", "native_timing_excluded_rows",
    "duplicate_identification_status", "occupied_slots", "expected_slots",
    "effective_slot_extra_views", "empty_slot_fraction", "leading_empty_slots",
    "interior_empty_slots", "trailing_empty_slots", "maximum_occupied_slot_gap_seconds",
    "class_window_raw_views_per_second", "class_window_effective_slots_per_second",
    *TSF_DIAGNOSTIC_FIELDS,
)
WINDOW_SCHEMA = pa.schema([
    *[pa.field(name, pa.string()) for name in (
        "run_id", "metric_id", "class_session_id", "classroom", "radio_group_id",
        "ap_id", "ap_model", "band", "capture_family", "observation_role",
    )],
    *[pa.field(name, pa.int64()) for name in (
        "observed_ap_5ghz_radio_groups", "observed_ap_6ghz_radio_groups", "window_number",
        "window_duration_us", "capture_slot_width_us", "member_bssid_count",
        "duplicate_uncertain_bssid_count", "expected_slots_per_bssid", "raw_views",
        "occupied_member_slots", "possible_member_slots", "any_beacon_occupied_slots",
        "cu_valid_duration_us", "station_available_duration_us", "station_stale_duration_us",
        "previous_window_cu_valid_duration_us", "previous_window_station_available_duration_us",
        "previous_window_station_stale_duration_us",
    )],
    pa.field("window_start", pa.timestamp("ns", tz="UTC")),
    pa.field("window_end", pa.timestamp("ns", tz="UTC")),
    pa.field("listening_exposure_known", pa.bool_()),
    *[pa.field(name, pa.float64()) for name in (
        "member_empty_slot_fraction", "any_beacon_empty_slot_fraction", "cu_mean_percent",
        "station_mean", "previous_window_cu_mean_percent", "previous_window_station_mean",
    )],
    pa.field("cu_bounds_metric_id", pa.string()),
    pa.field("cu_raw_duration_sum", pa.int64()),
    pa.field("previous_window_cu_raw_duration_sum", pa.int64()),
    pa.field("previous_window_duration_us", pa.int64()),
    *[pa.field(name, pa.float64()) for name in (
        "cu_mean_percent_lower_bound", "cu_mean_percent_upper_bound",
        "previous_window_cu_mean_percent_lower_bound", "previous_window_cu_mean_percent_upper_bound",
    )],
])
ASSOCIATION_COLUMNS = (
    "capture_family", "ap_model", "band", "observed_ap_5ghz_radio_groups",
    "observed_ap_6ghz_radio_groups", "predictor", "quality_windows", "paired_windows",
    "paired_window_fraction", "stale_assisted_station_pair_windows", "pearson_correlation", "outcome", "interpretation",
)
STRATA_COLUMNS = (
    "capture_family", "ap_model", "band", "observed_ap_5ghz_radio_groups",
    "observed_ap_6ghz_radio_groups", "predictor", "covariate_band", "paired_windows",
    "empty_member_slots", "possible_member_slots", "empty_slot_fraction",
)
GAP_COLUMNS = ("capture_family", "capture_slot_width_us", "occupied_slot_gap", "count")


@dataclass(frozen=True)
class BeaconQualityPart:
    streams: pd.DataFrame
    windows: pd.DataFrame
    gaps: pd.DataFrame


def _window_context(detail: pd.DataFrame, start: pd.Timestamp, window_us: int) -> pd.DataFrame:
    """Split existing radio bins at window boundaries; never extend CU."""
    columns = ["radio_group_id", "window_number", "cu_valid_duration_us", "cu_mean_percent",
               "station_available_duration_us", "station_mean", "station_stale_duration_us", "cu_raw_duration_sum"]
    if detail.empty:
        return pd.DataFrame(columns=columns)
    left = (pd.to_datetime(detail.bin_start, utc=True).astype("int64").to_numpy() - start.value) // 1000
    right = (pd.to_datetime(detail.bin_end, utc=True).astype("int64").to_numpy() - start.value) // 1000
    first, last = left // window_us, (right - 1) // window_us
    sizes = last - first + 1
    rows = np.repeat(np.arange(len(detail)), sizes)
    bases = np.repeat(np.cumsum(sizes) - sizes, sizes)
    windows = np.repeat(first, sizes) + np.arange(len(rows)) - bases
    durations = np.minimum(right[rows], (windows + 1) * window_us) - np.maximum(left[rows], windows * window_us)
    cu = pd.to_numeric(detail.qbss_cu_raw, errors="coerce").to_numpy(dtype=float, na_value=np.nan)[rows]
    station = pd.to_numeric(detail.station_count, errors="coerce").to_numpy(dtype=float, na_value=np.nan)[rows]
    valid_cu = detail.cu_state.eq("valid").to_numpy()[rows] & np.isfinite(cu)
    valid_station = detail.station_available.to_numpy(dtype=bool)[rows] & np.isfinite(station)
    stale = detail.station_stale_assisted.to_numpy(dtype=bool)[rows] & valid_station
    weighted = pd.DataFrame({
        "radio_group_id": detail.radio_group_id.to_numpy()[rows], "window_number": windows,
        "cu_valid_duration_us": np.where(valid_cu, durations, 0),
        "cu_sum": np.where(valid_cu, cu * durations / 255 * 100, 0),
        "station_available_duration_us": np.where(valid_station, durations, 0),
        "station_sum": np.where(valid_station, station * durations, 0),
        "station_stale_duration_us": np.where(stale, durations, 0),
        "cu_raw_duration_sum": np.where(valid_cu, cu, 0).astype(np.int64) * durations,
    }).groupby(["radio_group_id", "window_number"], sort=False).sum().reset_index()
    weighted["cu_mean_percent"] = weighted.cu_sum / weighted.cu_valid_duration_us.replace(0, np.nan)
    weighted["station_mean"] = weighted.station_sum / weighted.station_available_duration_us.replace(0, np.nan)
    return weighted.loc[:, columns]


def build_beacon_quality_part(
    observations: pd.DataFrame, identity_map: pd.DataFrame, radio_detail: pd.DataFrame,
    session: object, *, run_id: str, expected_interval_tu: int = 100,
    selection: BeaconSelectionResult | None = None,
) -> BeaconQualityPart:
    """Use pre-selection rows; ERM extras affect only this diagnostic metric."""
    if isinstance(expected_interval_tu, bool) or not isinstance(expected_interval_tu, int) or not 1 <= expected_interval_tu <= 65535:
        raise ValueError("expected_interval_tu must be an integer from 1 to 65535")
    start, end = pd.Timestamp(session.class_start), pd.Timestamp(session.class_end)
    if start.tzinfo is None or end.tzinfo is None or end <= start:
        raise ValueError("session bounds must be increasing and timezone-aware")
    if observations.empty:
        return BeaconQualityPart(pd.DataFrame(columns=STREAM_COLUMNS),
                                 pa.Table.from_batches([], schema=WINDOW_SCHEMA).to_pandas(),
                                 pd.DataFrame(columns=GAP_COLUMNS))
    duration_us = (end.value - start.value) // 1000
    nominal_us, window_us = expected_interval_tu * 1024, expected_interval_tu * 1024 * 100
    window_count = (duration_us + window_us - 1) // window_us
    frame = observations.copy(deep=False)
    profiles = frame.get("capture_profile", pd.Series("unknown", index=frame.index)).astype("string").fillna("unknown")
    frame = frame.assign(_profile=profiles, _family=np.where(profiles.eq("native_80211"), "native",
                                np.where(profiles.str.startswith("aruba_erm"), "erm", "unknown")),
                         _time=preferred_observation_times(frame))
    frame = frame.loc[frame._time.ge(start) & frame._time.lt(end)].copy()
    native = frame.loc[frame._family.eq("native")]
    native_result = None
    if not native.empty:
        native_result = selection if selection is not None else select_bssid_session_observations(
            native, class_session_id=str(session.class_session_id), class_start=start, class_end=end,
            bin_width_us=int(session.bin_width_us), default_beacon_interval_tu=100,
        )
    native_groups, native_timing = {}, {}
    if native_result is not None:
        native_observations = native_result.observations
        native_observations = native_observations.loc[native_observations.capture_profile.eq("native_80211")]
        native_groups = {str(bssid): part for bssid, part in native_observations.groupby("bssid", sort=False)}
        timing = native_result.timing
        if not timing.empty:
            native_timing = timing.loc[timing.capture_family.eq("native")].set_index("bssid").to_dict("index")
    duplicates, timing_excluded = Counter(), Counter()
    if native_result is not None and not native_result.excluded.empty:
        excluded = native_result.excluded.loc[native_result.excluded.capture_profile.eq("native_80211")]
        for reason, part in excluded.groupby("exclusion_reason"):
            target = duplicates if reason == "duplicate_view" else timing_excluded
            target.update(part.bssid.astype(str).value_counts().to_dict())
    radio_meta = identity_map.drop_duplicates("radio_group_id").set_index("radio_group_id")
    ap_bands = identity_map.drop_duplicates(["ap_id", "radio_group_id"]).groupby(["ap_id", "band"]).size()
    streams, radio_slots, gap_counts = [], {}, Counter()
    for (bssid, family), raw in frame.groupby(["bssid", "_family"], sort=True):
        radio = str(raw.radio_group_id.iloc[0])
        meta = radio_meta.loc[radio]
        sources = sorted(raw.source_capture.dropna().astype(str).unique()) if "source_capture" in raw else []
        advertised = pd.to_numeric(raw.get("beacon_interval_tu", pd.Series(index=raw.index, dtype=float)), errors="coerce")
        record = dict(zip(STREAM_COLUMNS, [None] * len(STREAM_COLUMNS)))
        record.update(run_id=run_id, metric_id=METRIC_ID, class_session_id=str(session.class_session_id),
                      classroom=str(session.classroom), radio_group_id=radio, ap_id=meta.ap_id,
                      ap_model=meta.get("ap_model", pd.NA), band=meta.band, bssid=str(bssid),
                      ssid=raw.ssid.iloc[0] if "ssid" in raw else pd.NA, capture_family=family,
                      capture_profiles=json.dumps(sorted(raw._profile.unique().tolist())),
                      source_captures=json.dumps(sources), observation_role="unknown",
                      listening_exposure_known=False, expected_beacon_interval_tu=expected_interval_tu,
                      advertised_intervals_tu=json.dumps(sorted(int(v) for v in advertised.dropna().unique())),
                      interval_mismatch_rows=int((advertised.notna() & advertised.ne(expected_interval_tu)).sum()),
                      interval_missing_rows=int(advertised.isna().sum()), class_duration_us=int(duration_us),
                      raw_views=len(raw), class_window_raw_views_per_second=len(raw) / (duration_us / 1e6))
        if family == "unknown":
            record.update(tsf_status="not_assessed", duplicate_identification_status="unknown_capture_profile")
            streams.append(record)
            continue
        slot_us = nominal_us * (10 if family == "erm" else 1)
        slots_per_window = window_us // slot_us
        expected = (duration_us + slot_us - 1) // slot_us
        if family == "native":
            timed = native_groups.get(str(bssid), native.iloc[:0])
            coordinate = timed.observation_time if not timed.empty else pd.Series(dtype="datetime64[ns, UTC]")
            diagnostic = native_timing.get(bssid, {})
            tsf_status = diagnostic.get("tsf_status", "not_assessed")
            record.update({field: diagnostic.get(field) for field in TSF_DIAGNOSTIC_FIELDS})
            uncertain = tsf_status != "usable_advancing"
            duplicate_status = "uncertain_" + tsf_status if uncertain else "validated_native_tsf_and_matching_fields"
        else:
            coordinate = raw._time
            tsf_status, uncertain = "not_used_for_erm", True
            duplicate_status = "effective_slot_cap_not_physical_deduplication"
        offsets = (pd.to_datetime(coordinate, utc=True).astype("int64").to_numpy() - start.value) // 1000
        occupied = np.unique(offsets // slot_us)
        occupied = occupied[(occupied >= 0) & (occupied < expected)]
        gaps = np.diff(occupied)
        gap_counts.update({(family, slot_us, int(gap)): int(count) for gap, count in zip(*np.unique(gaps, return_counts=True))})
        leading = int(occupied[0]) if len(occupied) else expected
        trailing = int(expected - occupied[-1] - 1) if len(occupied) else 0
        record.update(capture_slot_width_us=slot_us, tsf_status=tsf_status,
                      known_tsf_duplicate_views=duplicates[str(bssid)] if family == "native" else None,
                      native_timing_excluded_rows=timing_excluded[str(bssid)] if family == "native" else None,
                      duplicate_identification_status=duplicate_status, occupied_slots=len(occupied), expected_slots=expected,
                      effective_slot_extra_views=(len(raw) if family == "erm" else len(coordinate)) - len(occupied),
                      empty_slot_fraction=1 - len(occupied) / expected, leading_empty_slots=leading,
                      interior_empty_slots=int(np.maximum(gaps - 1, 0).sum()), trailing_empty_slots=trailing,
                      maximum_occupied_slot_gap_seconds=float(gaps.max() * slot_us / 1e6) if len(gaps) else None,
                      class_window_effective_slots_per_second=len(occupied) / (duration_us / 1e6))
        streams.append(record)
        state = radio_slots.setdefault((radio, family), {"members": 0, "uncertain": 0, "slots": [],
            "occupied": np.zeros(window_count, dtype=np.int64), "raw": np.zeros(window_count, dtype=np.int64)})
        state["members"] += 1
        state["uncertain"] += int(uncertain)
        state["slots"].append(occupied)
        state["occupied"] += np.bincount(occupied // slots_per_window, minlength=window_count)
        raw_offsets = (raw._time.astype("int64").to_numpy() - start.value) // 1000
        state["raw"] += np.bincount(raw_offsets // window_us, minlength=window_count)
    window_parts = []
    for (radio, family), state in sorted(radio_slots.items()):
        meta = radio_meta.loc[radio]
        slot_us = nominal_us * (10 if family == "erm" else 1)
        left = np.arange(window_count) * window_us
        right = np.minimum(left + window_us, duration_us)
        expected = (right - left + slot_us - 1) // slot_us
        any_slots = np.unique(np.concatenate(state["slots"]))
        any_counts = np.bincount(any_slots // (window_us // slot_us), minlength=window_count)
        part = pd.DataFrame({
            "run_id": run_id, "metric_id": METRIC_ID, "class_session_id": str(session.class_session_id),
            "classroom": str(session.classroom), "radio_group_id": radio, "ap_id": meta.ap_id,
            "ap_model": meta.get("ap_model", pd.NA), "band": meta.band, "capture_family": family,
            "observation_role": "unknown", "listening_exposure_known": False,
            "observed_ap_5ghz_radio_groups": int(ap_bands.get((meta.ap_id, "5"), 0)),
            "observed_ap_6ghz_radio_groups": int(ap_bands.get((meta.ap_id, "6"), 0)),
            "window_number": np.arange(window_count), "window_start": pd.to_datetime(start.value + left * 1000, utc=True),
            "window_end": pd.to_datetime(start.value + right * 1000, utc=True), "window_duration_us": right - left,
            "capture_slot_width_us": slot_us, "member_bssid_count": state["members"],
            "duplicate_uncertain_bssid_count": state["uncertain"], "expected_slots_per_bssid": expected,
            "raw_views": state["raw"], "occupied_member_slots": state["occupied"],
            "possible_member_slots": expected * state["members"], "any_beacon_occupied_slots": any_counts,
            "member_empty_slot_fraction": 1 - state["occupied"] / (expected * state["members"]),
            "any_beacon_empty_slot_fraction": 1 - any_counts / expected,
        })
        window_parts.append(part)
    if window_parts:
        windows = pd.concat(window_parts, ignore_index=True).merge(_window_context(radio_detail, start, window_us),
                    on=["radio_group_id", "window_number"], how="left", validate="many_to_one")
        intervals = []
        for row in windows.itertuples(index=False):
            if pd.isna(row.cu_raw_duration_sum) or pd.isna(row.cu_valid_duration_us):
                intervals.append((None, None))
                continue
            fields = cu_mean_bounds_fields(int(row.cu_raw_duration_sum), int(row.cu_valid_duration_us),
                                          int(row.window_duration_us), prefix="cu_mean_percent")
            interval = (fields["cu_mean_percent_lower_bound"], fields["cu_mean_percent_upper_bound"])
            if row.cu_valid_duration_us == row.window_duration_us:
                interval = (float(row.cu_mean_percent),) * 2
            intervals.append(interval)
        windows["cu_bounds_metric_id"] = CU_BOUNDS_METRIC_ID
        for index, bound in enumerate(("lower", "upper")):
            windows[f"cu_mean_percent_{bound}_bound"] = [interval[index] for interval in intervals]
        window_groups = windows.groupby(["radio_group_id", "capture_family"], sort=False)
        for name in ("cu_mean_percent", "station_mean", "cu_valid_duration_us",
                     "station_available_duration_us", "station_stale_duration_us",
                     "cu_raw_duration_sum", "cu_mean_percent_lower_bound", "cu_mean_percent_upper_bound"):
            windows["previous_window_" + name] = window_groups[name].shift(1)
        windows["previous_window_duration_us"] = window_groups.window_duration_us.shift(1)
        windows = windows.loc[:, WINDOW_SCHEMA.names]
    else:
        windows = pa.Table.from_batches([], schema=WINDOW_SCHEMA).to_pandas()
    return BeaconQualityPart(pd.DataFrame(streams, columns=STREAM_COLUMNS), windows,
                             pd.DataFrame([(*key, count) for key, count in sorted(gap_counts.items())], columns=GAP_COLUMNS))


class _Moments:
    def __init__(self):
        self.n = 0
        self.mx = self.my = self.xx = self.yy = self.xy = 0.0

    def add(self, x, y):
        if not len(x):
            return
        n, mx, my = len(x), float(np.mean(x)), float(np.mean(y))
        dx, dy = mx - self.mx, my - self.my
        correction = self.n * n / (self.n + n)
        self.xx += float(np.sum((x - mx) ** 2)) + dx * dx * correction
        self.yy += float(np.sum((y - my) ** 2)) + dy * dy * correction
        self.xy += float(np.sum((x - mx) * (y - my))) + dx * dy * correction
        self.mx += dx * n / (self.n + n)
        self.my += dy * n / (self.n + n)
        self.n += n

    def correlation(self):
        return float(np.clip(self.xy / np.sqrt(self.xx * self.yy), -1, 1)) if self.n > 1 and self.xx > 0 and self.yy > 0 else None


def write_beacon_quality_package(run_directory: str | Path, parts: Iterable[BeaconQualityPart]) -> tuple[Path, ...]:
    root = Path(run_directory) / REPORT_DIRECTORY
    if root.exists():
        raise FileExistsError(f"beacon quality report already exists: {root}")
    root.mkdir(parents=True)
    totals, moments, bands, gaps, stale_pairs = Counter(), {}, {}, Counter(), Counter()
    writer = None
    try:
        pd.DataFrame(columns=STREAM_COLUMNS).to_csv(root / STREAM_FILENAME, index=False)
        writer = pq.ParquetWriter(root / WINDOW_FILENAME, WINDOW_SCHEMA.with_metadata({b"report_schema_id": REPORT_SCHEMA_ID.encode()}), compression="zstd")
        for part in parts:
            part.streams.to_csv(root / STREAM_FILENAME, mode="a", header=False, index=False)
            writer.write_table(pa.Table.from_pandas(part.windows, schema=WINDOW_SCHEMA, preserve_index=False))
            gaps.update({(row.capture_family, row.capture_slot_width_us, row.occupied_slot_gap): row.count for row in part.gaps.itertuples()})
            cohort_columns = ["capture_family", "ap_model", "band", "observed_ap_5ghz_radio_groups", "observed_ap_6ghz_radio_groups"]
            for cohort, window in part.windows.groupby(cohort_columns, dropna=False, sort=False):
                cohort = tuple("unknown" if pd.isna(value) else value for value in cohort)
                for predictor, edges in (("previous_window_cu_mean_percent", [0, 25, 50, 75, 100]),
                                         ("previous_window_station_mean", [0, 25, 50, 75, 100, 150, 200, np.inf])):
                    key = (*cohort, predictor)
                    totals[key] += len(window)
                    x = window[predictor].to_numpy(dtype=float)
                    y = window.member_empty_slot_fraction.to_numpy(dtype=float)
                    valid = np.isfinite(x) & np.isfinite(y)
                    if predictor == "previous_window_station_mean":
                        stale_pairs[key] += int((valid & window.previous_window_station_stale_duration_us.fillna(0).gt(0).to_numpy()).sum())
                    moments.setdefault(key, _Moments()).add(x[valid], y[valid])
                    for index, (low, high) in enumerate(zip(edges, edges[1:])):
                        mask = valid & (x >= low) & ((x <= high) if index == len(edges) - 2 else (x < high))
                        label = f"{low:g}-{high:g}" if np.isfinite(high) else f"{low:g}+"
                        bucket = bands.setdefault((*key, label), np.zeros(3, dtype=np.int64))
                        bucket += [int(mask.sum()), int((window.possible_member_slots - window.occupied_member_slots)[mask].sum()),
                                   int(window.possible_member_slots[mask].sum())]
        writer.close()
        writer = None
        associations = pd.DataFrame([
            (*key, totals[key], value.n, value.n / totals[key] if totals[key] else None,
             stale_pairs[key] if key[-1] == "previous_window_station_mean" else None,
             value.correlation(), "member_empty_slot_fraction", "exploratory_not_causal_no_reliability_threshold")
            for key, value in sorted(moments.items())], columns=ASSOCIATION_COLUMNS)
        strata = pd.DataFrame([(*key, *values, values[1] / values[2] if values[2] else None)
                               for key, values in sorted(bands.items())], columns=STRATA_COLUMNS)
        gap_table = pd.DataFrame([(*key, count) for key, count in sorted(gaps.items())], columns=GAP_COLUMNS)
        associations.to_csv(root / ASSOCIATION_FILENAME, index=False)
        strata.to_csv(root / STRATA_FILENAME, index=False)
        gap_table.to_csv(root / GAP_FILENAME, index=False)
        _write_plots(root, strata, gap_table)
        (root / METHODOLOGY_FILENAME).write_text(METHODOLOGY, encoding="utf-8")
        return tuple(root / name for name in (STREAM_FILENAME, WINDOW_FILENAME, ASSOCIATION_FILENAME,
                                              STRATA_FILENAME, GAP_FILENAME, METHODOLOGY_FILENAME, *PLOT_FILENAMES))
    except BaseException:
        if writer is not None:
            writer.close()
        shutil.rmtree(root)
        raise


def build_capture_slot_availability(strata, predictor):
    if predictor == "previous_window_cu_mean_percent":
        label = "Previous-window observed CU (%)"
        labels = ["0-25", "25-50", "50-75", "75-100"]
        title = "CU capture slot availability"
    elif predictor == "previous_window_station_mean":
        label = "Previous-window radio station count (stale assistance allowed)"
        labels = ["0-25", "25-50", "50-75", "75-100", "100-150", "150-200", "200+"]
        title = "Station capture slot availability"
    else:
        raise ValueError(f"unknown capture-slot predictor: {predictor}")
    figure = Figure(figsize=(24, 10))
    FigureCanvasAgg(figure)
    axes = figure.subplots(1, 2)
    for axis, family in zip(axes, ("erm", "native")):
        part = strata.loc[strata.predictor.eq(predictor) & strata.capture_family.eq(family)]
        if part.empty or not part.possible_member_slots.sum():
            axis.text(0.5, 0.5, "No paired windows", transform=axis.transAxes, ha="center")
        else:
            ordered = part.groupby("covariate_band").sum(numeric_only=True).reindex(labels, fill_value=0)
            heights = ordered.empty_member_slots / ordered.possible_member_slots.replace(0, np.nan) * 100
            axis.bar(ordered.index, heights, color="#1f77b4" if family == "native" else "#9467bd")
            for index, (height, count) in enumerate(zip(heights, ordered.paired_windows)):
                axis.annotate(f"n={int(count)}" if count else "No pairs",
                              (index, float(height) if np.isfinite(height) else 0),
                              xytext=(0, -14 if height > 95 else 4), textcoords="offset points",
                              ha="center", va="top" if height > 95 else "bottom", fontsize=9)
        axis.set(title=family.upper(), xlabel=label, ylabel="Empty expected member slots (%)", ylim=(0, 100))
        axis.set_xticks(np.arange(len(labels)), labels)
        axis.set_xlim(-0.6, len(labels) - 0.4)
    figure.suptitle(title)
    figure.tight_layout()
    return figure


def build_beacon_slot_gap_cdf(gaps):
    figure = Figure(figsize=(16, 12))
    FigureCanvasAgg(figure)
    axis = figure.subplots()
    for family, group in gaps.groupby("capture_family"):
        seconds = group.occupied_slot_gap * group.capture_slot_width_us / 1e6
        counts = group.assign(seconds=seconds).groupby("seconds")["count"].sum().sort_index()
        axis.step(counts.index, counts.cumsum() / counts.sum(), where="post", label=family)
    axis.set(xlabel="Gap between occupied capture slots (seconds)", ylabel="Cumulative fraction of gaps", ylim=(0, 1),
             title="Beacon capture-slot gap CDF")
    if not gaps.empty:
        axis.set_xscale("log")
        axis.legend()
    else:
        axis.text(0.5, 0.5, "No inter-slot gaps available", transform=axis.transAxes, ha="center")
    figure.tight_layout()
    return figure


def _write_plots(root, strata, gaps):
    build_beacon_slot_gap_cdf(gaps).savefig(root / PLOT_FILENAMES[0], dpi=140)
    for predictor, filename in (
        ("previous_window_cu_mean_percent", PLOT_FILENAMES[1]),
        ("previous_window_station_mean", PLOT_FILENAMES[2]),
    ):
        build_capture_slot_availability(strata, predictor).savefig(root / filename, dpi=140)


METHODOLOGY = """Beacon cadence and capture quality — exploratory v1
Uses session-admitted observations BEFORE representative bin selection.
Default nominal interval is 100 TU (102400 us); BEACON_INTERVAL_TU / --beacon-interval-tu
overrides only this diagnostic expectation, not accepted CU/TSF/station calculations.
Advertised interval mismatches are counted, never silently used to replace the expectation.
ERM uses preferred observation time and 10 nominal intervals per class-start-anchored slot.
At most one effective observation per BSSID/ERM slot; extras are metric-only effective
duplicates, NOT proof that packet views are duplicates, and remain in canonical processing.
Native slots use capture observation timestamps, never TSF-derived wall time.
Native TSF is assessed independently of ERM for diagnostics and conservative
duplicate recognition only. Checks compare local clock increments and total
offset variation against one advertised beacon interval (100 TU if absent).
Duplicate candidates require matching retained beacon fields, distinct source
files and capture times within 102.4 ms. Conflicting/ambiguous views remain.
TSF never moves a measurement or excludes it from its observed class window.
Clock-jump counts, step errors, offset range and anomalous capture/frame/time/TSF
lineage are retained in beacon_cadence_sessions.csv; anomalies produce warnings.
Families pool capture files/receivers; source_captures records lineage. These are NOT
independent receiver paths. ERM own-transmit versus neighbour-receive role is unknown.
No exporter-to-AP mapping or validated TX/RX indication is currently available.
Rates and expected-slot fractions use the entire class window, not inferred listening time.
Missing capture files, off-channel visits and class edges may contribute empty slots:
they are NOT confirmed RF losses. Only session-observed BSSIDs enter expected membership.
The bounded window view uses 100 nominal intervals (10.24 s at default), with exact
terminal slot/window counts. Member availability counts all active family BSSIDs;
any-beacon availability unions their slots without summing SSID beacon rates.
CU and station covariates are duration-weighted existing pooled same-radio/bin measurements.
Current and previous-window CU means retain their observed values. Companion
*_lower_bound and *_upper_bound fields use 0/100% CU for missing time and the
actual window duration; cu_bounds_metric_id identifies the bounds calculation.
Exact raw-CU/duration sums and previous_window_duration_us support audit and
later refresh. The preceding window's own range/duration is lagged, including
before a partial terminal window. The first window has no preceding in-class
range. If measurement context components are absent, bounds stay unavailable;
known all-missing context instead has a 0-100% range. These are not confidence
intervals, and the pooled context is not an independent per-capture-path CU
measurement. Predictor buckets, eligibility and correlations remain observed.
CU is never interpolated, extended, or replaced using another AP. Accepted complete radio
station counts retain their existing 30-second assistance and stale duration diagnostics.
Associations compare current member-empty-slot fraction with the immediately PRECEDING
window's available CU/station mean. No lagged CU filling, significance tests, Spearman,
causal interpretation, inferred reliability threshold, or model correction is supplied.
Pearson gives each paired radio/family/window equal weight and is null for constants.
Covariate bands are descriptive display bins, not discovered thresholds; band fractions
are empty/possible member slots. Plots pool cohorts only for display; CSVs separate
family, model, band and observed AP 5/6-GHz radio-group counts. Observed radio groups
describe the sampled project identity map, NOT a complete hardware inventory.
Unpaired-window counts remain visible. Missingness may depend on the unobserved CU;
observed-covariate associations cannot establish reliable operation below 50% or 75%.
Cross-capture frame reconciliation, known listening exposure, periodic-loss correction,
simultaneous ERM/native validation and CU estimation remain deferred.
Trace back through class_session_id/radio_group_id and source_captures to manifest-hashed
raw captures; current contextual covariates derive from reports/radio_class_detail/radio_bin_detail.parquet.
"""
