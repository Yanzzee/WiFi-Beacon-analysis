"""ADR-0015 bounded within-project band comparisons and longitudinal exports."""
from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
import shutil

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import numpy as np
import pandas as pd
from matplotlib.colors import LogNorm
from matplotlib.patches import Patch

from band_comparison_inputs import SIX_GHZ_COLUMN, optional_enabled, validate_enablement_column
from project_scope import _configured_ap_mapping
from corrected_beacon_selection import build_class_bins
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID, cu_mean_bounds_fields, cu_cdf_summary_rows
from corrected_cu_plot_bounds import draw_cdf_bounds, draw_histogram_bounds, draw_vertical_mean_bounds

METRIC_ID = "beacon.metric/band-comparison/v1"
REPORT_SCHEMA_ID = "beacon.schema/band-comparison-reports/v1"
REPORT_DIRECTORY = Path("reports") / "band_comparison"
BANDS = ("2.4", "5", "6")
PLOTS = ("cu_cohort_cdf.png", "cohort_metrics.png", "matched_ap_cu.png", "station_adoption.png", "cohort_cu_station_density.png")
SUM_FIELDS = (
    "radio_duration_us", "valid_cu_duration_us", "missing_cu_duration_us", "no_beacon_duration_us",
    "cu_raw_duration_sum", "high_cu_duration_us", "very_high_cu_duration_us",
    "station_available_duration_us", "station_stale_duration_us", "station_duration_sum",
    "positive_station_pair_duration_us", "positive_station_pair_stale_duration_us",
    "positive_station_pair_cu_raw_duration_sum", "positive_station_pair_station_duration_sum",
    "zero_station_valid_cu_duration_us", "zero_station_cu_raw_duration_sum",
    "valid_admission_duration_us", "admission_raw_duration_sum", "radio_bin_rows",
    "zero_station_available_duration_us",
)
STREAK_FIELDS = ("high_actual_observed_streak_us", "high_possible_connected_streak_us",
                 "very_high_actual_observed_streak_us", "very_high_possible_connected_streak_us")
RADIO_CONTEXT_FIELDS = (
    "class_session_id", "classroom", "ap_id", "ap_model", "radio_group_id", "band", "channels",
    "class_start_utc", "class_end_utc", "class_start_local", "class_end_local", "course_labels", "combined_enrollment",
    "ap_six_ghz_enabled", "classroom_six_ghz_enabled", "classroom_mixed_ap_enablement", "six_ghz_enablement_source",
    "ap_observed_5_ghz_radio_count", "ap_observed_6_ghz_radio_count",
)


def _ratio(numerator, denominator, scale=1):
    return float(numerator / denominator * scale) if denominator else None


def _weighted(values, durations):
    values, durations = np.asarray(values, dtype=np.int64), np.asarray(durations, dtype=np.int64)
    if not len(values):
        return 0
    if int(values.max()) * int(durations.sum()) <= np.iinfo(np.int64).max:
        return int(values @ durations)
    return sum(int(value) * int(duration) for value, duration in zip(values, durations))


def _derived(record):
    return {
        "cu_coverage_percent": _ratio(record["valid_cu_duration_us"], record["radio_duration_us"], 100),
        "mean_cu_percent": _ratio(record["cu_raw_duration_sum"], record["valid_cu_duration_us"], 100 / 255),
        "high_cu_percent_of_valid_time": _ratio(record["high_cu_duration_us"], record["valid_cu_duration_us"], 100),
        "very_high_cu_percent_of_valid_time": _ratio(record["very_high_cu_duration_us"], record["valid_cu_duration_us"], 100),
        "mean_radio_station_count": _ratio(record["station_duration_sum"], record["station_available_duration_us"]),
        "station_coverage_percent": _ratio(record["station_available_duration_us"], record["radio_duration_us"], 100),
        "station_stale_percent_of_available_time": _ratio(record["station_stale_duration_us"], record["station_available_duration_us"], 100),
        "cu_relative_to_associated_station_count": _ratio(record["positive_station_pair_cu_raw_duration_sum"],
                                                         record["positive_station_pair_station_duration_sum"], 100 / 255),
        "positive_station_pair_coverage_percent": _ratio(record["positive_station_pair_duration_us"], record["radio_duration_us"], 100),
        "mean_cu_with_zero_stations_percent": _ratio(record["zero_station_cu_raw_duration_sum"], record["zero_station_valid_cu_duration_us"], 100 / 255),
        "mean_admission_capacity_percent": _ratio(record["admission_raw_duration_sum"], record["valid_admission_duration_us"], 100 / 31250),
        "admission_coverage_percent": _ratio(record["valid_admission_duration_us"], record["radio_duration_us"], 100),
        **cu_mean_bounds_fields(record["cu_raw_duration_sum"], record["valid_cu_duration_us"], record["radio_duration_us"]),
        **cu_mean_bounds_fields(record["zero_station_cu_raw_duration_sum"], record["zero_station_valid_cu_duration_us"],
                               record["zero_station_available_duration_us"], prefix="mean_cu_with_zero_stations_percent"),
    }


def _metrics(frame):
    d = frame.bin_duration_us.to_numpy(dtype=np.int64)
    cu = pd.to_numeric(frame.qbss_cu_raw, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    station = pd.to_numeric(frame.station_count, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    valid = frame.cu_state.eq("valid").to_numpy() & np.isfinite(cu)
    available = frame.station_available.to_numpy(dtype=bool) & np.isfinite(station)
    stale = frame.station_stale_assisted.to_numpy(dtype=bool) & available
    positive = valid & available & (station > 0)
    zero = valid & available & (station == 0)
    capacity = pd.to_numeric(frame.qbss_admission_capacity, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    admission = frame.admission_state.eq("valid").to_numpy() & np.isfinite(capacity)
    values = dict(zip(SUM_FIELDS, (
        int(d.sum()), int(d[valid].sum()), int(d[frame.cu_state.eq("missing_cu")].sum()),
        int(d[frame.cu_state.eq("no_beacon")].sum()), _weighted(cu[valid], d[valid]),
        int(d[valid & (cu >= 128)].sum()), int(d[valid & (cu >= 192)].sum()),
        int(d[available].sum()), int(d[stale].sum()), _weighted(station[available], d[available]),
        int(d[positive].sum()), int(d[positive & stale].sum()), _weighted(cu[positive], d[positive]),
        _weighted(station[positive], d[positive]), int(d[zero].sum()), _weighted(cu[zero], d[zero]),
        int(d[admission].sum()), _weighted(capacity[admission], d[admission]), len(frame),
        int(d[available & (station == 0)].sum()),
    )))
    values["maximum_radio_station_count"] = int(station[available].max()) if available.any() else None
    if not valid.any():
        values["high_cu_duration_us"] = values["very_high_cu_duration_us"] = None
    return {**values, **_derived(values)}


def build_infrastructure(configured_aps, identity):
    configured = validate_enablement_column(configured_aps)
    mapping = _configured_ap_mapping(configured).reset_index(drop=True)
    declared = {str(row["Hostname"]).strip().casefold(): optional_enabled(row.get(SIX_GHZ_COLUMN))
                for row in configured.to_dict("records")}
    radios = identity.dropna(subset=["radio_group_id"]).drop_duplicates("radio_group_id").copy()
    ap_rows = []
    for row in mapping.itertuples():
        observed = radios.loc[radios.ap_id.eq(row.ap_id)]
        counts = {band: int(observed.band.eq(band).sum()) for band in BANDS}
        explicit = declared[row.ap_name.casefold()]
        if explicit is False and counts["6"]:
            raise ValueError(f"{SIX_GHZ_COLUMN}=false contradicts observed 6 GHz radios for AP {row.ap_name}; correct the input or capture scope")
        enabled = explicit if explicit is not None else bool(counts["6"])
        ap_rows.append({"ap_id": row.ap_id, "ap_name": row.ap_name, "ap_model": row.ap_model,
            "classroom": row.classroom, "six_ghz_enabled_explicit": explicit, "six_ghz_enabled": enabled,
            "six_ghz_enablement_source": "explicit_input" if explicit is not None else "project_observed_bands",
            "observed_2_4_ghz_radio_count": counts["2.4"], "observed_5_ghz_radio_count": counts["5"],
            "observed_6_ghz_radio_count": counts["6"], "has_observed_data": bool(len(observed)),
            "enabled_6_ghz_not_observed": bool(enabled and not counts["6"])})
    ap = pd.DataFrame(ap_rows)
    rooms = []
    for room, group in ap.groupby("classroom", sort=True):
        enabled_count = int(group.six_ghz_enabled.sum())
        rooms.append({"classroom": room, "configured_ap_count": len(group), "six_ghz_enabled_ap_count": enabled_count,
            "six_ghz_enabled_ap_percent": enabled_count / len(group) * 100, "six_ghz_enabled": bool(enabled_count),
            "mixed_ap_enablement": 0 < enabled_count < len(group), "observed_ap_count": int(group.has_observed_data.sum()),
            "enabled_6_ghz_not_observed_ap_count": int(group.enabled_6_ghz_not_observed.sum()),
            **{name: int(group[name].sum()) for name in (
                "observed_2_4_ghz_radio_count", "observed_5_ghz_radio_count", "observed_6_ghz_radio_count")}})
    return ap, pd.DataFrame(rooms), radios


class BandComparisonAccumulator:
    """Retain only compact session summaries and 256-value cohort marginals."""

    def __init__(self, *, configured_aps, identity_map, class_sessions, run_id, project_name, display_timezone="UTC", radio_summary=None):
        self.ap, self.rooms, radios = build_infrastructure(configured_aps, identity_map)
        self.radio_meta = radios.set_index("radio_group_id")
        self.ap_meta, self.room_meta = self.ap.set_index("ap_id"), self.rooms.set_index("classroom")
        self.sessions = class_sessions.set_index("class_session_id")
        self.radio_summary = None if radio_summary is None else radio_summary.set_index(["class_session_id", "radio_group_id"])
        self.provenance = {"run_id": run_id, "project_name": project_name, "report_schema_id": REPORT_SCHEMA_ID, "report_metric_id": METRIC_ID}
        self.timezone = display_timezone
        self.radio_rows, self.pairs, self.adoption = [], [], []
        self.histograms = defaultdict(lambda: np.zeros((2, 256), dtype=np.int64))
        self.cu_station_grids = defaultdict(lambda: np.zeros((40, 50), dtype=np.int64))
        self.detail_sessions, self.station_sessions = set(), set()

    def _memberships(self, frame):
        ap_enabled = frame.ap_id.map(self.ap_meta.six_ghz_enabled).to_numpy(dtype=bool)
        room_enabled = frame.classroom.map(self.room_meta.six_ghz_enabled).to_numpy(dtype=bool)
        yield "overall", "all", np.ones(len(frame), dtype=bool)
        for band in BANDS:
            band_mask = frame.band.eq(band).to_numpy()
            yield "band", band, band_mask
            for scope, enabled in (("ap_cohort", ap_enabled), ("classroom_cohort", room_enabled)):
                for flag, label in ((False, "without_6"), (True, "with_6")):
                    yield scope, f"{band}_on_{label}", band_mask & (enabled == flag)

    def add_detail(self, part):
        if part.empty:
            return
        if part.class_session_id.nunique() != 1:
            raise ValueError("band comparison requires one complete session per detail part")
        sid = str(part.class_session_id.iloc[0])
        if sid in self.detail_sessions:
            raise ValueError("duplicate band comparison detail session")
        self.detail_sessions.add(sid)
        session = self.sessions.loc[sid]
        for radio, frame in part.groupby("radio_group_id", sort=True):
            meta = self.radio_meta.loc[radio]
            ap, room = self.ap_meta.loc[meta.ap_id], self.room_meta.loc[meta.classroom]
            streaks = dict.fromkeys(STREAK_FIELDS)
            if self.radio_summary is not None:
                authoritative = self.radio_summary.loc[(sid, radio)]
                streaks = {name: authoritative[name] for name in STREAK_FIELDS}
            self.radio_rows.append({**self.provenance, "class_session_id": sid, "classroom": meta.classroom,
                "ap_id": meta.ap_id, "ap_model": meta.get("ap_model"), "radio_group_id": radio, "band": meta.band,
                "channels": json.dumps(sorted(int(c) for c in frame.channel.dropna().unique())),
                "class_start_utc": pd.Timestamp(session.class_start).isoformat(), "class_end_utc": pd.Timestamp(session.class_end).isoformat(),
                "class_start_local": pd.Timestamp(session.class_start).tz_convert(self.timezone).isoformat(),
                "class_end_local": pd.Timestamp(session.class_end).tz_convert(self.timezone).isoformat(),
                "course_labels": session.get("course_labels"), "combined_enrollment": session.get("enrolled"),
                "ap_six_ghz_enabled": bool(ap.six_ghz_enabled), "classroom_six_ghz_enabled": bool(room.six_ghz_enabled),
                "classroom_mixed_ap_enablement": bool(room.mixed_ap_enablement),
                "six_ghz_enablement_source": ap.six_ghz_enablement_source,
                "ap_observed_5_ghz_radio_count": int(ap.observed_5_ghz_radio_count),
                "ap_observed_6_ghz_radio_count": int(ap.observed_6_ghz_radio_count),
                **streaks, **_metrics(frame)})
        valid = part.cu_state.eq("valid").to_numpy() & part.qbss_cu_raw.notna().to_numpy()
        d = part.bin_duration_us.to_numpy(dtype=np.int64)
        cu = pd.to_numeric(part.qbss_cu_raw, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        station = pd.to_numeric(part.station_count, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        pair = valid & part.station_available.to_numpy(dtype=bool) & np.isfinite(station)
        for scope, population, mask in self._memberships(part):
            histogram = self.histograms[scope, population]
            selected = mask & valid
            if selected.any():
                # Sum integer durations exactly, rather than float-weighted bincount.
                np.add.at(histogram[0], cu[selected].astype(int), 1)
                np.add.at(histogram[1], cu[selected].astype(int), d[selected])
            selected = mask & pair
            if selected.any():
                x = np.minimum((cu[selected] / 256 * 40).astype(int), 39)
                y = np.minimum((station[selected] / 5).astype(int), 49)
                np.add.at(self.cu_station_grids[scope, population], (x, y), d[selected])
        for ap_id, group in part.groupby("ap_id", sort=True):
            five = list(group.loc[group.band.eq("5")].groupby("radio_group_id", sort=True))
            six = list(group.loc[group.band.eq("6")].groupby("radio_group_id", sort=True))
            for r5, f5 in five:
                for r6, f6 in six:
                    joined = f5.merge(f6, on="bin_number", suffixes=("_5", "_6"), validate="one_to_one")
                    valid_pair = joined.cu_state_5.eq("valid") & joined.cu_state_6.eq("valid")
                    common = joined.loc[valid_pair]
                    durations = common.bin_duration_us_5.to_numpy(dtype=np.int64)
                    denominator = int(durations.sum())
                    raw5 = _weighted(common.qbss_cu_raw_5, durations)
                    raw6 = _weighted(common.qbss_cu_raw_6, durations)
                    c5 = _ratio(raw5, denominator, 100 / 255)
                    c6 = _ratio(raw6, denominator, 100 / 255)
                    possible = int(f5.bin_duration_us.sum())
                    bounds = {
                        **cu_mean_bounds_fields(raw5, denominator, possible, prefix="paired_window_mean_5_cu_percent"),
                        **cu_mean_bounds_fields(raw6, denominator, possible, prefix="paired_window_mean_6_cu_percent"),
                    }
                    # Only common observations inform these paired-window
                    # scenarios; an unpaired bin contributes no inferred CU.
                    for name, left, right in (("lower", "lower", "upper"), ("upper", "upper", "lower")):
                        value6 = bounds[f"paired_window_mean_6_cu_percent_{left}_bound"]
                        value5 = bounds[f"paired_window_mean_5_cu_percent_{right}_bound"]
                        bounds[f"paired_window_6_minus_5_cu_percentage_points_{name}_bound"] = (
                            value6 - value5 if possible else None)
                    self.pairs.append({**self.provenance, "class_session_id": sid, "classroom": str(session.classroom),
                        "ap_id": ap_id, "ap_model": self.ap_meta.loc[ap_id].ap_model, "radio_5_id": r5, "radio_6_id": r6,
                        "possible_duration_us": possible, "common_valid_cu_duration_us": denominator,
                        "common_valid_cu_coverage_percent": _ratio(denominator, int(f5.bin_duration_us.sum()), 100),
                        "matched_mean_5_cu_percent": c5, "matched_mean_6_cu_percent": c6,
                        "matched_6_minus_5_cu_percentage_points": c6 - c5 if denominator else None,
                        "common_5_cu_raw_duration_sum": raw5, "common_6_cu_raw_duration_sum": raw6, **bounds})

    def add_station(self, part):
        if part.empty:
            return
        if part.class_session_id.nunique() != 1:
            raise ValueError("band comparison requires one complete station session per part")
        sid = str(part.class_session_id.iloc[0])
        if sid in self.station_sessions:
            raise ValueError("duplicate station comparison session")
        self.station_sessions.add(sid)
        session = self.sessions.loc[sid]
        bins = part[["bin_number", "bin_duration_us"]].drop_duplicates("bin_number").set_index("bin_number")
        radio = part.loc[part.rollup_type.eq("radio")].copy()
        radio["ap_id"] = radio.rollup_id.map(self.radio_meta.ap_id)
        radio["band"] = radio.rollup_id.map(self.radio_meta.band)
        band = part.loc[part.rollup_type.eq("band")].copy()
        band["band"] = band.rollup_id
        room = str(session.classroom)
        self._adoption_row(band, bins, sid, "classroom", room, self.room_meta.loc[room])
        for ap_id, meta in self.ap_meta.loc[self.ap_meta.classroom.eq(room)].iterrows():
            group = radio.loc[radio.ap_id.eq(ap_id)]
            if not group.empty:
                group = group.groupby(["bin_number", "band"], sort=False).agg(
                    station_count=("station_count", "sum"),
                    available=("available", "all"), stale_assisted=("stale_assisted", "any")).reset_index()
                group.loc[~group.available, "station_count"] = np.nan
            self._adoption_row(group, bins, sid, "ap", ap_id, meta)

    def _adoption_row(self, frame, bins, sid, scope, entity, meta):
        session = self.sessions.loc[sid]
        d = bins.bin_duration_us.to_numpy(dtype=np.int64)
        station, stale = {}, {}
        record = {**self.provenance, "scope": scope, "entity_id": entity, "class_session_id": sid,
                  "classroom": str(session.classroom), "six_ghz_enabled": bool(meta.six_ghz_enabled),
                  "class_start_utc": pd.Timestamp(session.class_start).isoformat(), "class_end_utc": pd.Timestamp(session.class_end).isoformat(),
                  "class_start_local": pd.Timestamp(session.class_start).tz_convert(self.timezone).isoformat(),
                  "class_end_local": pd.Timestamp(session.class_end).tz_convert(self.timezone).isoformat(),
                  "course_labels": session.get("course_labels"), "combined_enrollment": session.get("enrolled"),
                  "ap_model": meta.get("ap_model") if scope == "ap" else None,
                  "class_duration_us": int((pd.Timestamp(session.class_end) - pd.Timestamp(session.class_start)).value // 1000)}
        for band, suffix in (("2.4", "2_4"), ("5", "5"), ("6", "6")):
            expected = bool(meta[f"observed_{suffix}_ghz_radio_count"]) or (band == "6" and bool(meta.six_ghz_enabled))
            values = frame.loc[frame.band.eq(band)].set_index("bin_number") if not frame.empty else pd.DataFrame()
            if not values.empty:
                values = values.reindex(bins.index)
                station[band] = pd.to_numeric(values.station_count.where(values.available.fillna(False)), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
                stale[band] = values.stale_assisted.fillna(False).to_numpy(dtype=bool)
            else:
                station[band] = np.full(len(bins), np.nan if expected else 0.0)
                stale[band] = np.zeros(len(bins), dtype=bool)
            # Structural absence is zero only for share denominators, never a
            # measured station mean/peak or claimed observation coverage.
            available = np.isfinite(station[band]) & expected
            record.update({f"band_{suffix}_expected_in_sample": expected,
                f"band_{suffix}_station_available_duration_us": int(d[available].sum()),
                f"band_{suffix}_station_duration_sum": _weighted(station[band][available], d[available]),
                f"band_{suffix}_mean_station_count": _ratio(_weighted(station[band][available], d[available]), int(d[available].sum())),
                f"band_{suffix}_peak_station_count": float(station[band][available].max()) if available.any() else None})
        for population, members in (("5_6", ("5", "6")), ("all_bands", BANDS)):
            total = sum(station[band] for band in members)
            valid = np.isfinite(total) & (total > 0)
            numerator = _weighted(station["6"][valid], d[valid])
            denominator = _weighted(total[valid], d[valid])
            record.update({f"{population}_eligible_duration_us": int(d[valid].sum()),
                f"{population}_six_station_duration_sum": numerator, f"{population}_total_station_duration_sum": denominator,
                f"{population}_six_station_share_percent": _ratio(numerator, denominator, 100),
                f"{population}_stale_assisted_duration_us": int(d[valid & np.logical_or.reduce([stale[band] for band in members])].sum())})
        self.adoption.append(record)

    def _summary(self, frame, scope, population, extra=None):
        values = {name: int(frame[name].sum()) for name in SUM_FIELDS}
        equal = {}
        for label, keys in (("ap", ["ap_id", "class_session_id"]), ("classroom", ["classroom", "class_session_id"])):
            grouped = frame.groupby(keys)[["cu_raw_duration_sum", "valid_cu_duration_us", "radio_duration_us"]].sum()
            eligible = grouped.valid_cu_duration_us.gt(0)
            means = grouped.loc[eligible, "cu_raw_duration_sum"] / grouped.loc[eligible, "valid_cu_duration_us"] * 100 / 255
            equal[f"equal_{label}_session_mean_cu_percent"] = float(means.mean()) if len(means) else None
            equal[f"eligible_{label}_session_count"] = int(eligible.sum())
            intervals = []
            for number, row in enumerate(grouped.loc[eligible].itertuples(index=False)):
                fields = cu_mean_bounds_fields(row.cu_raw_duration_sum, row.valid_cu_duration_us, row.radio_duration_us)
                interval = (fields["mean_cu_percent_lower_bound"], fields["mean_cu_percent_upper_bound"])
                if row.valid_cu_duration_us == row.radio_duration_us:
                    interval = (float(means.iloc[number]),) * 2
                intervals.append(interval)
            complete = grouped.loc[eligible, "valid_cu_duration_us"].eq(grouped.loc[eligible, "radio_duration_us"]).all()
            for index, bound in enumerate(("lower", "upper")):
                equal[f"equal_{label}_session_mean_cu_percent_{bound}_bound"] = (
                    equal[f"equal_{label}_session_mean_cu_percent"] if complete else
                    float(np.mean([interval[index] for interval in intervals])))
        return {**self.provenance, "population_scope": scope, "population_id": population,
            "status": "observed" if len(frame) else "no_matching_radios", "radio_count": frame.radio_group_id.nunique(),
            "ap_count": frame.ap_id.nunique(), "classroom_count": frame.classroom.nunique(),
            "class_session_count": frame.class_session_id.nunique(), "radio_session_count": len(frame),
            "maximum_radio_station_count": frame.maximum_radio_station_count.max() if len(frame) else None,
            **{f"maximum_{name}": frame[name].max() if len(frame) else None for name in STREAK_FIELDS},
            **(extra or {}), **values, **_derived(values), **equal}

    def tables(self):
        for sid, session in self.sessions.iterrows():
            if sid not in self.station_sessions:
                bins = build_class_bins(session.class_start, session.class_end, int(session.bin_width_us)).set_index("bin_number")
                self._adoption_row(pd.DataFrame(), bins, sid, "classroom", str(session.classroom), self.room_meta.loc[session.classroom])
                for ap_id, meta in self.ap_meta.loc[self.ap_meta.classroom.eq(session.classroom)].iterrows():
                    self._adoption_row(pd.DataFrame(), bins, sid, "ap", ap_id, meta)
                self.station_sessions.add(sid)
        radios = pd.DataFrame(self.radio_rows, columns=[*self.provenance, *RADIO_CONTEXT_FIELDS, *STREAK_FIELDS,
            *SUM_FIELDS, "maximum_radio_station_count", *_derived(dict.fromkeys(SUM_FIELDS, 0))]).sort_values(
                ["class_session_id", "radio_group_id"], kind="stable").reset_index(drop=True)
        populations, model_rows, room_rows = [], [], []
        for scope, population, mask in self._memberships(radios):
            frame = radios.loc[mask]
            populations.append(self._summary(frame, scope, population))
            for (model, five, six), group in frame.groupby(["ap_model", "ap_observed_5_ghz_radio_count", "ap_observed_6_ghz_radio_count"], dropna=False) if len(frame) else []:
                model_rows.append(self._summary(group, scope, population, {"ap_model": model,
                    "ap_observed_5_ghz_radio_count": five, "ap_observed_6_ghz_radio_count": six}))
        for (room, band), group in radios.groupby(["classroom", "band"]):
            room_rows.append(self._summary(group, "classroom_band", f"{room}/{band}", {"classroom": room, "band": band}))
        adoption = pd.DataFrame(self.adoption)
        if adoption.empty:
            adoption = pd.DataFrame(columns=[*self.provenance, "scope", "entity_id", "class_session_id", "classroom", "six_ghz_enabled", "class_duration_us",
                *[f"{prefix}_{name}" for prefix in ("5_6", "all_bands") for name in (
                    "eligible_duration_us", "six_station_duration_sum", "total_station_duration_sum", "stale_assisted_duration_us", "six_station_share_percent")]])
        adoption = adoption.sort_values(["scope", "entity_id", "class_session_id"], kind="stable").reset_index(drop=True)
        adoption_specs = [("overall", "all", adoption.loc[adoption.scope.eq("classroom")])]
        for scope in ("classroom", "ap"):
            source = adoption.loc[adoption.scope.eq(scope)]
            adoption_specs.extend((scope, entity, group) for entity, group in source.groupby("entity_id"))
            for enabled in (False, True):
                adoption_specs.append((scope + "_cohort", "with_6" if enabled else "without_6", source.loc[source.six_ghz_enabled.eq(enabled)]))
        adoption_summary = []
        for scope, entity, group in adoption_specs:
            record = {**self.provenance, "population_scope": scope, "population_id": entity, "entity_session_count": len(group),
                      "class_duration_us": int(group.class_duration_us.sum())}
            for prefix in ("5_6", "all_bands"):
                for name in ("eligible_duration_us", "six_station_duration_sum", "total_station_duration_sum", "stale_assisted_duration_us"):
                    record[f"{prefix}_{name}"] = int(group[f"{prefix}_{name}"].sum())
                record[f"{prefix}_six_station_share_percent"] = _ratio(record[f"{prefix}_six_station_duration_sum"], record[f"{prefix}_total_station_duration_sum"], 100)
                record[f"{prefix}_eligible_entity_session_count"] = int(group[f"{prefix}_eligible_duration_us"].gt(0).sum())
            adoption_summary.append(record)
        distribution = [{**self.provenance, "population_scope": scope, "population_id": population, "cu_raw": raw,
                         "cu_percent": raw * 100 / 255, "valid_radio_bin_count": int(histogram[0, raw]), "valid_radio_duration_us": int(histogram[1, raw])}
                        for (scope, population), histogram in sorted(self.histograms.items()) for raw in range(256) if histogram[0, raw]]
        paired_columns = [*self.provenance, "class_session_id", "classroom", "ap_id", "ap_model", "radio_5_id", "radio_6_id",
            "possible_duration_us", "common_valid_cu_duration_us", "common_valid_cu_coverage_percent", "matched_mean_5_cu_percent",
            "matched_mean_6_cu_percent", "matched_6_minus_5_cu_percentage_points",
            "common_5_cu_raw_duration_sum", "common_6_cu_raw_duration_sum", "cu_bounds_metric_id",
            *[f"paired_window_mean_{band}_cu_percent_{bound}_bound" for band in (5, 6) for bound in ("lower", "upper")],
            *[f"paired_window_6_minus_5_cu_percentage_points_{bound}_bound" for bound in ("lower", "upper")]]
        deployment = {**self.provenance, "configured_ap_count": len(self.ap), "observed_ap_count": int(self.ap.has_observed_data.sum()),
            "six_ghz_enabled_ap_count": int(self.ap.six_ghz_enabled.sum()),
            "six_ghz_enabled_ap_percent": _ratio(int(self.ap.six_ghz_enabled.sum()), len(self.ap), 100),
            "enabled_6_ghz_not_observed_ap_count": int(self.ap.enabled_6_ghz_not_observed.sum()),
            "configured_classroom_count": len(self.rooms), "six_ghz_enabled_classroom_count": int(self.rooms.six_ghz_enabled.sum()),
            "six_ghz_enabled_classroom_percent": _ratio(int(self.rooms.six_ghz_enabled.sum()), len(self.rooms), 100),
            "mixed_classroom_count": int(self.rooms.mixed_ap_enablement.sum()),
            **{name: int(self.ap[name].sum()) for name in (
                "observed_2_4_ghz_radio_count", "observed_5_ghz_radio_count", "observed_6_ghz_radio_count")}}
        pairs = pd.DataFrame(self.pairs, columns=paired_columns).sort_values(
            ["class_session_id", "ap_id", "radio_5_id", "radio_6_id"], kind="stable").reset_index(drop=True)
        cdf_rows = []
        for row in populations:
            histogram = self.histograms.get((row["population_scope"], row["population_id"]), np.zeros((2, 256), dtype=np.int64))[1]
            cdf_rows.extend(cu_cdf_summary_rows(histogram, row["radio_duration_us"],
                scope=row["population_scope"], population=row["population_id"], weighting="radio_duration_us"))
        return {"ap_infrastructure.csv": self.ap.sort_values("ap_id").reset_index(drop=True), "classroom_infrastructure.csv": self.rooms,
            "deployment_summary.csv": pd.DataFrame([deployment]),
            "population_summary.csv": pd.DataFrame(populations), "model_population_summary.csv": pd.DataFrame(model_rows, columns=[*pd.DataFrame(populations).columns, "ap_model", "ap_observed_5_ghz_radio_count", "ap_observed_6_ghz_radio_count"]),
            "classroom_band_summary.csv": pd.DataFrame(room_rows, columns=[*pd.DataFrame(populations).columns, "classroom", "band"]),
            "radio_class_comparison.csv": radios, "matched_ap_radio_sessions.csv": pairs,
            "station_adoption_sessions.csv": adoption, "station_adoption_summary.csv": pd.DataFrame(adoption_summary),
            "cu_cdf_bounds.csv": pd.DataFrame(cdf_rows),
            "cu_distribution.csv": pd.DataFrame(distribution, columns=[*self.provenance, "population_scope", "population_id", "cu_raw", "cu_percent", "valid_radio_bin_count", "valid_radio_duration_us"])}

    def write(self, run_directory, *, context=None):
        root = Path(run_directory) / REPORT_DIRECTORY
        if root.exists():
            raise FileExistsError(f"band comparison already exists: {root}")
        root.mkdir(parents=True)
        try:
            tables = self.tables()
            for name, frame in tables.items():
                frame = frame.copy()
                for key, value in self.provenance.items():
                    if key not in frame:
                        frame.insert(0, key, value)
                frame.to_csv(root / name, index=False, lineterminator="\n")
            _plots(root, self, tables)
            context = {**(context or {}), **self.provenance, "display_timezone": self.timezone,
                "manifest": "../../manifest.json", "core_sources": ["../radio_class_detail/radio_bin_detail.parquet", "../station_count/station_bin_rollups.parquet"],
                "cu_weighting": "valid_radio_duration; equal_AP_and_classroom_session_views_also_provided",
                "cu_mean_bounds": {"metric_id": CU_BOUNDS_METRIC_ID, "stage": "complete_exports_figures_HTML_text_and_saved_run_refresh",
                    "missing_cu_scenarios_percent": [0, 100], "denominator": "total_expected_radio_duration",
                    "observed_fields_unchanged": True,
                    "equal_session_bounds": "same_eligible_entities_and_equal_weights_as_observed",
                    "zero_station_bounds": "available_zero_station_duration_including_missing_CU",
                    "paired_window_bounds": "possible_duration_us; only_common_valid_CU_informs_both_radios"},
                "cu_cdf_bounds": {"metric_id": CU_BOUNDS_METRIC_ID, "weighting": "radio_duration_us",
                    "numeric_summary": "cu_cdf_bounds.csv", "raw_exclusive_thresholds": [128, 192],
                    "expected_weight": "radio_duration_us; absent_populations_unavailable",
                    "missing_scenarios_percent": [0, 100]},
                "adoption_weighting": "ratio_of_simultaneous_station_duration_sums; overall_uses_classrooms_only",
                "adoption_histogram_population": "6_GHz_enabled_entities_only; overall_share_retains_all_sampled_infrastructure",
                "cu_relative_station_units": "CU_percentage_points_per_associated_station; not_attributed_client_airtime",
                "core_cu_metric_id": "beacon.metric/corrected-cu/v1", "station_metric_id": "beacon.metric/corrected-station-count/v1",
                "admission_metric_id": "beacon.metric/radio-admission-capacity/v1",
                "enabled_default": "6_GHz_observed_anywhere_in_project", "explicit_input_column": SIX_GHZ_COLUMN,
                "cohort_plot_scopes": list(_cohort_plot_scopes(tables["classroom_infrastructure.csv"])),
                "missing_bandwidth_or_power": "unknown; not_inferred", "cross_project_joins": "manual; AP/radio IDs are project scoped"}
            (root / "comparison_context.json").write_text(json.dumps(context, indent=2, default=_json_default) + "\n", encoding="utf-8")
            (root / "methodology.txt").write_text(METHODOLOGY, encoding="utf-8")
            return tuple(sorted(root.iterdir()))
        except BaseException:
            shutil.rmtree(root)
            raise


def _json_default(value):
    if value is pd.NA:
        return None
    if hasattr(value, "item"):
        return value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"cannot serialize comparison context: {type(value)}")


def _figure(width=12, height=5):
    figure = Figure(figsize=(width * 2, height * 2), layout="constrained")
    FigureCanvasAgg(figure)
    return figure


def _cohort_plot_scopes(classroom_infrastructure):
    mixed = classroom_infrastructure.mixed_ap_enablement.map(optional_enabled).fillna(False).any()
    return ("ap_cohort", "classroom_cohort") if mixed else ("ap_cohort",)


def _adoption_plot_counts(frame):
    values = pd.to_numeric(frame["5_6_six_station_share_percent"], errors="coerce")
    eligible = values.notna()
    zero = eligible & values.eq(0)
    return {"eligible": int(eligible.sum()), "unknown": int((~eligible).sum()),
            "zero": int(zero.sum()),
            "first_bin": int((eligible & values.ge(0) & values.lt(2.5)).sum())}


def _cohort_plot_context(tables):
    labels = {"5_on_without_6": "5 GHz, no 6 GHz enabled", "5_on_with_6": "5 GHz, 6 GHz enabled", "6_on_with_6": "6 GHz, 6 GHz enabled"}
    scopes = _cohort_plot_scopes(tables["classroom_infrastructure.csv"])
    scope_labels = {"ap_cohort": "AP cohort", "classroom_cohort": "Classroom cohort"}
    if len(scopes) == 1:
        scope_labels["ap_cohort"] = "AP and classroom cohorts (no mixed classrooms)"
    return labels, scopes, scope_labels


def build_cohort_cu_cdf(histograms, tables):
    labels, scopes, scope_labels = _cohort_plot_context(tables)
    figure = _figure(15, 4.8 * len(scopes))
    axes = figure.subplots(len(scopes), len(labels), squeeze=False, sharex=True, sharey=True)
    summary = tables["population_summary.csv"].set_index(["population_scope", "population_id"])
    colors = ("#1f77b4", "#ff7f0e", "#2ca02c")
    for row_index, scope in enumerate(scopes):
        for axis, (population, label), color in zip(axes[row_index], labels.items(), colors):
            thresholds = []
            histogram = histograms[scope, population][1]
            key = (scope, population)
            total = (int(summary.loc[key, "radio_duration_us"])
                     if key in summary.index and "radio_duration_us" in summary and pd.notna(summary.loc[key, "radio_duration_us"]) else None)
            if (key in summary.index and "valid_cu_duration_us" in summary
                    and pd.notna(summary.loc[key, "valid_cu_duration_us"])
                    and int(histogram.sum()) != int(summary.loc[key, "valid_cu_duration_us"])):
                raise ValueError("saved CU distribution does not match valid cohort duration")
            if histogram.sum():
                axis.step(np.arange(256) * 100 / 255, histogram.cumsum() / histogram.sum(), where="post", label="Observed", color=color)
            bounded = draw_cdf_bounds(axis, histogram, total, color=color, legend=False)
            if histogram.sum() or bounded:
                for row in cu_cdf_summary_rows(histogram, total, scope=scope, population=population, weighting="radio_duration_us"):
                    observed = f"{row['cdf_observed_percent']:.1f}%" if row["cdf_observed_percent"] is not None else "unavailable"
                    limits = (f"{row['cdf_lower_bound_percent']:.1f}%–{row['cdf_upper_bound_percent']:.1f}% possible"
                              if row["cdf_lower_bound_percent"] is not None else "bounds unavailable")
                    thresholds.append(f"{row['cu_threshold_percent']}% CU: {observed} observed\n{limits}")
            if bounded or axis.lines:
                handles, names = axis.get_legend_handles_labels()
                if bounded:
                    handles.append(Patch(facecolor=color, alpha=.15))
                    names.append("Possible range")
                axis.legend(handles, names, loc="upper left", fontsize=9)
                axis.text(.98, .02, "\n".join(thresholds), transform=axis.transAxes, ha="right", va="bottom", fontsize=8,
                          bbox={"facecolor": "white", "edgecolor": "none", "alpha": .9})
                if not histogram.sum():
                    axis.text(.5, .5, "No observed CU", transform=axis.transAxes, ha="center")
            else:
                axis.text(.5, .5, "No sampled radio time" if total == 0 else "No observed CU", transform=axis.transAxes, ha="center")
            title = label if len(scopes) == 1 else scope_labels[scope] + "\n" + label
            axis.set(title=title, xlabel="CU (%)", ylabel="Cumulative fraction of radio time", xlim=(0, 100), ylim=(0, 1))
    figure.suptitle("CU cumulative distributions by infrastructure cohort" +
                   ("\n" + scope_labels[scopes[0]] if len(scopes) == 1 else ""))
    return figure


def build_cohort_metrics(tables):
    labels, scopes, scope_labels = _cohort_plot_context(tables)
    figure = _figure(12, 8)
    axes = figure.subplots(2, 2)
    summary = tables["population_summary.csv"].set_index(["population_scope", "population_id"])
    names = list(labels)
    summary = summary.reindex(pd.MultiIndex.from_product([scopes, names], names=["population_scope", "population_id"]))
    for axis, metric, title in zip(axes.flat,
            ("mean_cu_percent", "cu_coverage_percent", "mean_radio_station_count", "cu_relative_to_associated_station_count"),
            ("Observed mean CU (%)", "CU coverage (%)", "Mean stations per radio", "CU relative to station count\n(percentage points per associated station)")):
        width = .7 / len(scopes)
        ranged = False
        for index, scope in enumerate(scopes):
            offset = (index - (len(scopes) - 1) / 2) * width
            values = [float(summary.loc[(scope, name), metric]) if metric in summary and pd.notna(summary.loc[(scope, name), metric]) else np.nan for name in names]
            axis.bar(np.arange(3) + offset, values, width=width, label=scope_labels[scope])
            if metric == "mean_cu_percent" and {"mean_cu_percent_lower_bound", "mean_cu_percent_upper_bound"}.issubset(summary.columns):
                ranged |= draw_vertical_mean_bounds(axis, np.arange(3) + offset,
                    [summary.loc[(scope, name), "mean_cu_percent_lower_bound"] for name in names],
                    [summary.loc[(scope, name), "mean_cu_percent_upper_bound"] for name in names], color="black", cap_width=width * .18)
            for position, value in enumerate(values):
                axis.annotate(f"{value:.2f}" if np.isfinite(value) else "No data",
                    (position + offset, value if np.isfinite(value) else 0),
                    xytext=(0, 4), textcoords="offset points", ha="center", fontsize=9)
        axis.set_xticks(range(3), ["5 GHz / no 6", "5 GHz / with 6", "6 GHz / with 6"])
        axis.set_title(title)
        if metric == "mean_cu_percent" and "mean_cu_percent_lower_bound" in summary:
            axis.text(.02, .98, "Whiskers: missing-bin min/max", transform=axis.transAxes, va="top", fontsize=9)
        axis.set_ylim(bottom=0)
        if len(scopes) > 1:
            axis.legend()
        if not any(np.isfinite(rectangle.get_height()) for rectangle in axis.patches):
            axis.text(.5, .5, "No observed CU; ranges only" if ranged else "No eligible measurements",
                      transform=axis.transAxes, ha="center")
    if len(scopes) == 1:
        figure.suptitle(scope_labels["ap_cohort"])
    return figure


def build_matched_ap_cu(pairs):
    figure = _figure()
    axes = figure.subplots(1, 2)
    available = pairs.loc[pairs.common_valid_cu_duration_us.gt(0)]
    if not available.empty:
        axes[0].scatter(available.matched_mean_5_cu_percent, available.matched_mean_6_cu_percent, s=10, alpha=.5)
        edges = np.arange(-100, 102.5, 2.5)
        axes[1].hist(available.matched_6_minus_5_cu_percentage_points, bins=edges, color="#9467bd", alpha=.8)
        if {f"paired_window_6_minus_5_cu_percentage_points_{bound}_bound" for bound in ("lower", "upper")}.issubset(available.columns):
            draw_histogram_bounds(axes[1], available.paired_window_6_minus_5_cu_percentage_points_lower_bound,
                                  available.paired_window_6_minus_5_cu_percentage_points_upper_bound, edges, color="#9467bd")
            axes[1].text(.02, .98, "Shading: possible count range per bin", transform=axes[1].transAxes, va="top", fontsize=9)
    else:
        for axis in axes:
            axis.text(.5, .5, "No matched valid CU", transform=axis.transAxes, ha="center")
    axes[0].plot([0, 100], [0, 100], "--", color="gray", linewidth=1)
    axes[0].set(xlabel="Matched 5 GHz mean CU (%)", ylabel="Matched 6 GHz mean CU (%)", xlim=(0, 100), ylim=(0, 100))
    axes[1].set(xlabel="Matched 6 minus 5 GHz CU (percentage points)", ylabel="Radio-pair/session comparisons", xlim=(-100, 100))
    figure.suptitle("Matched 5 and 6 GHz CU on the same AP/class-session")
    return figure


def build_station_adoption(adoption):
    figure = _figure()
    axes = figure.subplots(1, 2, sharey=True)
    for axis, scope in zip(axes, ("ap", "classroom")):
        scope_members = adoption.loc[adoption.scope.eq(scope)]
        enabled = scope_members.six_ghz_enabled.map(optional_enabled).eq(True)
        members = scope_members.loc[enabled]
        excluded = int((~enabled).sum())
        values = members["5_6_six_station_share_percent"].dropna()
        counts = _adoption_plot_counts(members)
        axis.hist(values, bins=np.arange(0, 102.5, 2.5), color="#2ca02c")
        if not len(values):
            axis.text(.5, .5, "No eligible station totals where 6 GHz is enabled", transform=axis.transAxes, ha="center")
        axis.text(.98, .98,
            f"Eligible {scope}/sessions: {counts['eligible']:,}; unavailable: {counts['unknown']:,}\n"
            f"Exact 0% use where enabled: {counts['zero']:,}; excluded non-enabled sessions: {excluded:,}\n"
            "First histogram bin includes 0% to <2.5%",
            transform=axis.transAxes, ha="right", va="top", fontsize=10,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": .85})
        axis.set(title=f"6 GHz-enabled {scope.upper()}", xlabel="6 GHz share of 5 + 6 GHz association-time (%)", ylabel="Enabled entity/session count", xlim=(0, 100))
    figure.suptitle("6 GHz associated station-time where 6 GHz is enabled")
    return figure


def build_cohort_cu_station_density(grids, tables):
    labels, scopes, scope_labels = _cohort_plot_context(tables)
    figure = _figure(15, 3.5 * len(scopes))
    axes = figure.subplots(len(scopes), 3, squeeze=False)
    display_grids = {}
    for scope in scopes:
        for population in labels:
            grid = grids[scope, population].astype(float)
            display_grids[scope, population] = grid / grid.sum() * 100 if grid.sum() else grid
    # Keep the existing five-station buckets and overflow bucket unchanged.
    # Use one shared display range, with room for the highest occupied bucket.
    highest_bucket = max((int(np.flatnonzero(grid.sum(axis=0))[-1])
                          for grid in display_grids.values() if grid.sum()), default=-1)
    station_limit = max(100, int(np.ceil((highest_bucket + 1) * 5 / 50)) * 50)
    if highest_bucket == 49:
        summary = tables["population_summary.csv"]
        maxima = pd.to_numeric(summary.loc[
            summary.population_scope.isin(scopes) & summary.population_id.isin(labels),
            "maximum_radio_station_count"], errors="coerce")
        if maxima.notna().any():
            station_limit = max(station_limit, int(np.ceil(float(maxima.max()) / 50)) * 50)
    station_edges = np.arange(0, 251, 5, dtype=float)
    station_edges[-1] = max(250, station_limit)
    occupied = np.concatenate([grid[grid > 0] for grid in display_grids.values()])
    norm = LogNorm(vmin=float(occupied.min()), vmax=max(float(occupied.max()), float(occupied.min()) * 1.01)) if len(occupied) else None
    plot = None
    for row, scope in zip(axes, scopes):
        for axis, (population, label) in zip(row, labels.items()):
            grid = display_grids[scope, population]
            if grid.sum():
                plot = axis.pcolormesh(np.linspace(0, 100, 41), station_edges,
                    np.ma.masked_equal(grid.T, 0), norm=norm, cmap="viridis")
            else:
                axis.text(.5, .5, "No valid pairs", transform=axis.transAxes, ha="center")
            title_scope = scope_labels[scope] if len(scopes) > 1 else "AP/classroom cohort"
            ylabel = "Associated stations (last bin ≥245)" if highest_bucket == 49 else "Associated stations"
            axis.set(title=f"{title_scope}: {label}", xlabel="CU (%)", ylabel=ylabel, xlim=(0, 100), ylim=(0, station_limit))
    if plot is not None:
        figure.colorbar(plot, ax=axes.ravel().tolist(), label="Percent of valid paired radio time within cohort")
    figure.suptitle("CU versus station load; shared color scale, normalized within cohort; statistics retain unclipped counts")
    if len(scopes) == 1:
        figure.suptitle("CU versus station load; no mixed classrooms, AP and classroom cohorts coincide\n"
                        "Shared color scale, normalized within cohort; statistics retain unclipped counts")
    return figure


def _plots(root, accumulator, tables):
    build_cohort_cu_cdf(accumulator.histograms, tables).savefig(root / PLOTS[0], dpi=140)
    build_cohort_metrics(tables).savefig(root / PLOTS[1], dpi=140)
    build_matched_ap_cu(tables["matched_ap_radio_sessions.csv"]).savefig(root / PLOTS[2], dpi=140)
    build_station_adoption(tables["station_adoption_sessions.csv"]).savefig(root / PLOTS[3], dpi=140)
    build_cohort_cu_station_density(accumulator.cu_station_grids, tables).savefig(root / PLOTS[4], dpi=140)


METHODOLOGY = """Band comparison and longitudinal exports — ADR-0015 v1
The central question is how 6 GHz deployment and actual use relate to observed
channel utilization, station load and advertised radio admission capacity.
CU is sensed channel busy time, not throughput, latency, per-client airtime or
proof of an infrastructure effect. Station counts describe associations, not
unique devices/people or devices capable of 6 GHz. No causal inference is supplied.

AP enablement defaults to whether a resolved 6 GHz radio is observed anywhere
in the project, not whether it appears in an individual session. Optional AP
input 6GHz_Enabled explicitly overrides this default; false plus observed 6 GHz
is rejected as contradictory. Explicit true without observed 6 GHz is retained
as an enabled-but-unobserved warning, never synthesized as a measured zero.
Radio band always comes from accepted capture evidence, never the AP flag.
Classroom enablement means at least one configured AP has 6 GHz enabled; mixed
rooms are flagged. AP and classroom cohorts are separate overlapping views.
Observed radio counts are sample topology, not proof of complete hardware inventory.
Both cohort definitions classify the SAME radio/bin measurements; the classroom
cohort panels are not summed classroom station counts or a classroom CU sensor.
If every classroom is uniform in AP 6 GHz enablement, these classifications
coincide. CDF, metric-bar and density graphics then show a single shared view.
When mixed classrooms exist, both views are shown. CSVs always retain both scopes.

radio_class_comparison.csv preserves every physical radio/session separately.
population_summary.csv includes overall, each band, AP cohorts and classroom
cohorts. Overall CU is a pooled mean of observed radio measurements, not a sum
of channel utilization or a new classroom-wide sensor value. Primary means and
CDFs use valid radio duration, including actual terminal-bin duration. Missing
CU and no-beacon are separate and never zero-filled. High/very-high use unchanged
raw thresholds 128/192; threshold shares use valid time. Equal AP/session and
classroom/session mean CU views are also supplied to expose weighting effects.
Ordinary mean_cu_percent columns remain observed. Companion
mean_cu_percent_lower_bound and mean_cu_percent_upper_bound fields assign
missing-CU and no-beacon duration 0% and 100% CU over total expected radio
duration. cu_bounds_metric_id identifies this additive calculation. These are
missing-bin bounds, not confidence intervals or inferred/fill values. A
nonempty all-missing population has bounds 0-100%; an absent population has
unavailable bounds. They do not cover missed variation inside observed bins.
Equal AP/session and classroom/session bounds average entity/session intervals
with the same equal weights and eligible groups as their observed means.
Groups with no valid CU remain excluded from these equal-session statistics;
their ordinary population bounds remain separately available.
Zero-station bounds use zero_station_available_duration_us: time with a complete
available zero station count, including missing CU and accepted stale counts.
Unknown station counts do not establish zero stations. No zero-station time
means unavailable bounds; zero-station time with no CU has bounds 0-100%.
Matched observed means and differences still use common valid bins only.
Companion paired_window_* bounds apply to possible_duration_us. Common CU
informs both radios; unpaired time is treated as unknown for both sides in
this conservative comparison, without inferring or overwriting either value.
The paired-window difference range is lower_6 - upper_5 to upper_6 - lower_5.
With no common CU the observed comparison is unavailable, while the ranges
are 0-100% for each radio and -100 to 100 percentage points for the difference.
These bounds describe a complete paired-window scenario, not uncertainty in
the conditional common-valid-only mean. Matched figures retain the observed
points and histogram counts. The scatter plot shows observed points only;
histogram shading shows the separate paired-window scenarios for those same
eligible comparisons. Each histogram bucket's
minimum counts intervals wholly inside it, and its maximum counts intervals
intersecting it. Bucket maxima need not occur together. The radio/class-session
mean histogram uses the same per-bucket rule; cohort mean-CU bars use whiskers.
CU CDFs retain their observed curves and add minimum/maximum curves and shading,
with one infrastructure cohort per panel to avoid overlapping ranges.
For cumulative valid duration A, expected duration T and missing duration M,
the range below 100% CU is A/T to (A+M)/T; both endpoints reach one at 100%.
An active population with no usable CU has a range but no observed curve;
an absent population has neither. cu_cdf_bounds.csv records observed/minimum/
maximum cumulative percentages at raw CU <128 and <192 (50%/75% markers),
with exact expected/valid duration weights for every population. This differs
from ../cu_coverage_streak/cu_cdf_bounds.csv, whose weights count radio/bins.
No beacon is filled and no other CDF is changed. HTML/text presentations retain
the observed values alongside minimum/maximum scenarios. Saved-run refresh adds
supported companions to a separate bundle, records source hashes and leaves
unsupported bounds unavailable; source runs and manifests remain unchanged.
model_population_summary.csv preserves model and sampled 5/6 radio-count strata.
classroom_band_summary.csv supports manual same-space comparisons across years.
Density plots normalize each cohort by its valid paired radio time and use one
shared logarithmic color scale. Empty cells are blank; the last station display
bucket includes all counts >=245. Display bucketing never clips statistical inputs.

Radio station means use available duration; accepted complete radio totals and
30-second stale assistance are unchanged and their coverage/stale durations are
reported. CU relative to associated station count is sum(raw_CU*duration)*100/255
divided by sum(stations*duration), on same-radio/bin valid pairs with stations>0.
Do not average bin ratios or divide CU by independently measured station peaks.
Zero-client CU is reported separately. Admission capacity remains an independent
radio allowance; it is not summed across radios and retains its own valid duration.

matched_ap_radio_sessions.csv compares every 5/6 radio pair on the same AP and
physical session, using only common valid CU bins. Coverage and signed 6-minus-5
differences remain explicit. Multiple 5 GHz radios generate multiple pairs;
these are descriptive, not independent samples or an AP-level causal estimate.

Station adoption uses existing complete band/classroom/bin totals. AP/band/bin
totals sum accepted active radio rollups, with no partial numerical sums. A band
outside observed scope (and not declared 6 GHz enabled) contributes structural
zero only to the adoption denominator. An expected band without a valid total
stays unknown. Report 6/(5+6) and 6/(2.4+5+6) separately on simultaneous complete
totals with positive denominator. Shares are ratios of station-duration sums,
not ratios of independent peaks, mean bin ratios, or counts of unique devices.
Adoption histograms show use where available: only 6 GHz-enabled APs/classrooms
participate. Genuine 0% use where enabled remains; non-enabled entities are
excluded, not relabelled as missing. An enabled classroom includes all its APs,
including any 5-only APs. AP/session and classroom/session counts remain separate
even without mixed rooms: a room can have multiple APs. Annotations show exact
0%, unavailable enabled sessions and excluded non-enabled sessions; the first
bin also includes positive shares below 2.5%. Missing shares never become zero.
Detailed adoption tables retain all entities and infrastructure status. The
overall project share includes all sampled infrastructure in its denominator,
so it measures combined infrastructure deployment and actual use, not just use
where enabled. Existing with_6 cohort summaries provide enabled-only shares.
Overall adoption accumulates classroom rows only, never AP and classroom rows
together. Radio station means, AP/band peaks and classroom/band peaks are distinct.
Incomplete session membership is still limited by the accepted sampled-data scope.

2.4 GHz participates when sampled; the main charts emphasize the three 5/6 GHz
populations. Tables and context retain run/project IDs, units, component sums,
coverage, AP model, band/channel, schedule/enrollment, and metric/schema versions.
Original CU/station reports keep their definitions. This additive package is
built while existing session parts stream; no extra decode or whole-project bin
table is retained. Detailed radio rows link back by radio/session to the existing
radio-bin Parquet, then source BSSID/session/bin to selected observations.
Numeric lookup: population_summary.csv contains the calculated overall/band/
cohort values and component sums (mean_cu_percent, mean_radio_station_count,
coverage, high-CU shares, capacity, and CU relative to stations). Model and room
breakdowns are in model_population_summary.csv and classroom_band_summary.csv;
individual radio/session values are in radio_class_comparison.csv. Classroom
band station means/peaks are in station_adoption_sessions.csv with scope=classroom.
The run-wide reports/run_summary.txt provides concise headline values; metrics
from other report families remain in their own CSVs and methodology guides.

Year-over-year joins are manual initially: use stable classroom labels and
review schedule, enrollment, capture method, SSID/sample scope, radio density,
channel configuration, firmware and deployment changes. AP/radio IDs are project
scoped, not lifetime hardware IDs. Bandwidth/power not present in inputs remain
unknown; do not infer them from primary channel. Capture-quality bias is not
removed by cohort matching. Paired ERM/native calibration and causal models are
deferred. Existing immutable runs are not rewritten.
"""
