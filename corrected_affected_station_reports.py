"""ADR-0016 descriptive station exposure, streamed one physical session at a time."""
from __future__ import annotations

import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from corrected_band_comparison_reports import _figure, _ratio, _weighted
from corrected_beacon_selection import build_class_bins
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID, station_time_cu_bounds_fields
from corrected_cu_plot_bounds import draw_vertical_mean_bounds

METRIC_ID = "beacon.metric/potentially-affected-stations/v1"
REPORT_SCHEMA_ID = "beacon.schema/affected-station-reports/v1"
REPORT_DIRECTORY = Path("reports/affected_stations")
SUM_FIELDS = (
    "radio_duration_us", "valid_cu_duration_us", "missing_cu_duration_us", "no_beacon_duration_us",
    "station_available_duration_us", "pair_duration_us", "stale_pair_duration_us",
    "available_station_time_us", "paired_station_time_us", "stale_paired_station_time_us",
    "below_high_station_time_us", "high_only_station_time_us", "very_high_station_time_us",
    "high_station_time_us", "stale_high_station_time_us", "stale_very_high_station_time_us",
)


def _derived(record):
    paired = record["paired_station_time_us"]
    return {
        "pair_coverage_percent": _ratio(record["pair_duration_us"], record["radio_duration_us"], 100),
        "cu_coverage_percent": _ratio(record["valid_cu_duration_us"], record["radio_duration_us"], 100),
        "station_coverage_percent": _ratio(record["station_available_duration_us"], record["radio_duration_us"], 100),
        "known_station_time_cu_coverage_percent": _ratio(paired, record["available_station_time_us"], 100),
        "stale_pair_percent": _ratio(record["stale_pair_duration_us"], record["pair_duration_us"], 100),
        **{f"{name}_exposure_percent": _ratio(record[f"{name}_station_time_us"], paired, 100)
           for name in ("below_high", "high_only", "high", "very_high")},
        **station_time_cu_bounds_fields(record["available_station_time_us"], paired,
                                       record["high_station_time_us"], record["very_high_station_time_us"]),
    }


def _components(frame):
    duration = frame.bin_duration_us.to_numpy(dtype=np.int64)
    cu = pd.to_numeric(frame.qbss_cu_raw, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    station = pd.to_numeric(frame.station_count, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    valid = frame.cu_state.eq("valid").to_numpy() & np.isfinite(cu)
    available = frame.station_available.to_numpy(dtype=bool) & np.isfinite(station)
    pair = valid & available
    stale = pair & frame.station_stale_assisted.to_numpy(dtype=bool)
    high, very = pair & (cu >= 128), pair & (cu >= 192)
    def station_time(mask):
        return _weighted(station[mask], duration[mask])
    record = dict(zip(SUM_FIELDS, (
        int(duration.sum()), int(duration[valid].sum()),
        int(duration[frame.cu_state.eq("missing_cu")].sum()), int(duration[frame.cu_state.eq("no_beacon")].sum()),
        int(duration[available].sum()), int(duration[pair].sum()), int(duration[stale].sum()),
        station_time(available), station_time(pair), station_time(stale),
        station_time(pair & ~high), station_time(high & ~very), station_time(very), station_time(high),
        station_time(stale & high), station_time(stale & very),
    )))
    return record, pair, stale, high, very, station


class AffectedStationAccumulator:
    """Keep only entity/session records; never retain whole-project bin data."""

    def __init__(self, *, class_sessions, ap_infrastructure, classroom_infrastructure,
                 run_id, project_name, display_timezone="UTC"):
        self.sessions = class_sessions.set_index("class_session_id")
        self.aps = ap_infrastructure.set_index("ap_id")
        self.rooms = classroom_infrastructure.set_index("classroom")
        self.timezone = display_timezone
        self.provenance = dict(run_id=run_id, project_name=project_name,
                               report_metric_id=METRIC_ID, report_schema_id=REPORT_SCHEMA_ID)
        self.rows, self.seen = [], set()

    def _record(self, frame, sid, scope, entity, band, bins):
        session = self.sessions.loc[sid]
        room = str(session.classroom)
        ap = str(frame.ap_id.iloc[0]) if scope in ("radio", "ap") else None
        radio_count = frame.radio_group_id.nunique()
        record, pair, stale, high, very, station = _components(frame)
        record.update(self.provenance)
        record.update(class_session_id=sid, classroom=room, scope=scope, entity_id=entity, band=band,
                      ap_id=ap, radio_group_id=entity if scope == "radio" else None,
                      ap_model=self.aps.loc[ap].ap_model if ap is not None else None,
                      six_ghz_enabled=bool(self.aps.loc[ap].six_ghz_enabled if ap is not None else self.rooms.loc[room].six_ghz_enabled),
                      classroom_six_ghz_enabled=bool(self.rooms.loc[room].six_ghz_enabled),
                      classroom_mixed_ap_enablement=bool(self.rooms.loc[room].mixed_ap_enablement),
                      radio_count=radio_count, class_duration_us=int(bins.bin_duration_us.sum()),
                      class_start_utc=pd.Timestamp(session.class_start).isoformat(),
                      class_end_utc=pd.Timestamp(session.class_end).isoformat(),
                      class_start_local=pd.Timestamp(session.class_start).tz_convert(self.timezone).isoformat(),
                      class_end_local=pd.Timestamp(session.class_end).tz_convert(self.timezone).isoformat(),
                      course_labels=session.get("course_labels"), combined_enrollment=session.get("enrolled"))
        # One row per active radio/bin is validated before grouping. Missing data
        # invalidates the entity's bin, never becomes a partial numeric total.
        values = pd.DataFrame({"bin_number": frame.bin_number.to_numpy(), "pair": pair,
                               "stale": stale, "high": np.where(high, station, 0),
                               "very_high": np.where(very, station, 0)})
        grouped = values.groupby("bin_number", sort=True).agg(
            pairs=("pair", "sum"), stale=("stale", "any"), high=("high", "sum"), very_high=("very_high", "sum"))
        complete = grouped.index[grouped.pairs.eq(radio_count)] if radio_count else []
        complete_duration = int(bins.loc[bins.bin_number.isin(complete), "bin_duration_us"].sum())
        record.update(complete_bin_count=len(complete), complete_bin_duration_us=complete_duration,
                      unavailable_bin_duration_us=record["class_duration_us"] - complete_duration,
                      complete_bin_coverage_percent=_ratio(complete_duration, record["class_duration_us"], 100))
        for name in ("high", "very_high"):
            peak = grouped.loc[complete, name]
            number = int(peak.idxmax()) if len(peak) else None
            record.update({f"peak_{name}_associations": int(peak.loc[number]) if number is not None else None,
                           f"peak_{name}_bin_number": number,
                           f"peak_{name}_bin_start_utc": bins.set_index("bin_number").loc[number, "bin_start"].isoformat() if number is not None else None,
                           f"peak_{name}_stale_assisted": bool(grouped.loc[number, "stale"]) if number is not None else None})
        record.update(_derived(record))
        self.rows.append(record)

    def add_detail(self, frame):
        if frame.empty:
            return
        if frame.class_session_id.nunique() != 1:
            raise ValueError("affected stations require one complete physical session per part")
        sid = str(frame.class_session_id.iloc[0])
        if sid in self.seen:
            raise ValueError("duplicate affected-station session")
        session = self.sessions.loc[sid]
        bins = build_class_bins(session.class_start, session.class_end, bin_width_us=int(session.bin_width_us))
        expected = bins.set_index("bin_number").bin_duration_us
        if (frame.duplicated(["radio_group_id", "bin_number"]).any()
                or len(frame) != frame.radio_group_id.nunique() * len(bins)
                or not frame.bin_duration_us.eq(frame.bin_number.map(expected)).all()
                or not frame.classroom.eq(session.classroom).all()):
            raise ValueError("affected stations require complete unique radio/bin geometry for the session")
        self.seen.add(sid)
        for scope, key in (("radio", "radio_group_id"), ("ap", "ap_id"), ("classroom", "classroom")):
            for entity, group in frame.groupby(key, sort=True):
                if scope == "radio":
                    self._record(group, sid, scope, entity, str(group.band.iloc[0]), bins)
                else:
                    self._record(group, sid, scope, entity, "all", bins)
                    for band, part in group.groupby("band", sort=True):
                        self._record(part, sid, scope, entity, str(band), bins)

    def tables(self):
        # Sessions with no active radio have no fabricated radio/AP membership.
        empty = pd.DataFrame(columns=["bin_duration_us", "qbss_cu_raw", "station_count", "cu_state",
                                      "station_available", "station_stale_assisted", "radio_group_id", "bin_number"])
        for sid in sorted(set(self.sessions.index) - self.seen):
            session = self.sessions.loc[sid]
            bins = build_class_bins(session.class_start, session.class_end, bin_width_us=int(session.bin_width_us))
            self._record(empty, sid, "classroom", str(session.classroom), "all", bins)
            self.seen.add(sid)
        entities = pd.DataFrame(self.rows).sort_values(["scope", "entity_id", "class_session_id", "band"], kind="stable").reset_index(drop=True)
        radios = entities.loc[entities.scope.eq("radio")]
        populations = [("overall", "all", radios)]
        for band in ("2.4", "5", "6"):
            members = radios.loc[radios.band.eq(band)]
            populations.append(("band", band, members))
            for scope, field in (("ap_cohort", "six_ghz_enabled"), ("classroom_cohort", "classroom_six_ghz_enabled")):
                for enabled, label in ((False, "without_6"), (True, "with_6")):
                    populations.append((scope, f"{band}_on_{label}", members.loc[members[field].eq(enabled)]))
        summary = []
        for scope, population, group in populations:
            record = {**self.provenance, "population_scope": scope, "population_id": population,
                      "radio_session_count": len(group), "class_session_count": group.class_session_id.nunique()}
            record.update({field: sum(int(value) for value in group[field]) for field in SUM_FIELDS})
            record.update(_derived(record))
            summary.append(record)
        distributions = []
        rooms = entities.loc[entities.scope.eq("classroom")]
        for band, group in rooms.groupby("band", sort=True):
            for name in ("high", "very_high"):
                values = pd.to_numeric(group[f"peak_{name}_associations"], errors="coerce").dropna()
                distributions.append({**self.provenance, "band": band, "threshold": name,
                    "class_session_count": len(group), "available_peak_count": len(values),
                    "unavailable_peak_count": len(group) - len(values),
                    "minimum": float(values.min()) if len(values) else None,
                    "median": float(values.median()) if len(values) else None,
                    "p90": float(values.quantile(.9)) if len(values) else None,
                    "maximum": float(values.max()) if len(values) else None})
        return {"entity_session_exposure.csv": entities, "population_exposure.csv": pd.DataFrame(summary),
                "classroom_peak_distribution.csv": pd.DataFrame(distributions)}

    def write(self, run_root):
        root = Path(run_root) / REPORT_DIRECTORY
        root.mkdir(parents=True, exist_ok=False)
        try:
            tables = self.tables()
            for name, frame in tables.items():
                frame.to_csv(root / name, index=False, lineterminator="\n")
            _plots(root, tables)
            (root / "methodology.txt").write_text(METHODOLOGY, encoding="utf-8")
            (root / "context.json").write_text(json.dumps({**self.provenance, "display_timezone": self.timezone,
                "manifest": "../../manifest.json", "source": "../radio_class_detail/radio_bin_detail.parquet",
                "high_minimum_raw": 128, "very_high_minimum_raw": 192,
                "station_time_unit": "association-microseconds", "peak_policy": "complete_pairs_all_session_active_radios",
                "cu_filled": False, "stale_station_counts_included": True,
                "station_time_cu_bounds": {"metric_id": CU_BOUNDS_METRIC_ID,
                    "denominator": "available_station_time_us",
                    "unknown_weight": "available_station_time_us - paired_station_time_us",
                    "missing_cu_scenarios_percent": [0, 100],
                    "unknown_station_counts_bounded": False, "observed_fields_unchanged": True}}, indent=2) + "\n", encoding="utf-8")
            return tuple(sorted(root.iterdir()))
        except BaseException:
            shutil.rmtree(root)
            raise


def build_station_time_exposure(populations):
    """Keep observed bars and add available-station-time bounds when saved."""
    population = populations.set_index(["population_scope", "population_id"])
    figure = _figure(12, 5)
    axes = figure.subplots(1, 2)
    keys = [("overall", "all"), *(
        ("band", band) for band in ("2.4", "5", "6")
        if ("band", band) in population.index and population.loc[("band", band), "radio_duration_us"] > 0)]
    plotted_values = [population.loc[key, f"{name}_exposure_percent"] for key in keys for name in ("high", "very_high")]
    bound_fields = [f"{name}_exposure_percent_{bound}_bound"
                    for name in ("high", "very_high") for bound in ("lower", "upper")]
    has_bounds = set(bound_fields).issubset(population.columns)
    if has_bounds:
        plotted_values.extend(population.loc[key, field] for key in keys for field in bound_fields)
    finite = [float(value) for value in plotted_values if pd.notna(value)]
    upper = min(105, max(1, np.ceil(max(finite, default=0) * 1.2)))
    range_shown = False
    for axis, name in zip(axes, ("high", "very_high")):
        values = [population.loc[key, f"{name}_exposure_percent"] for key in keys]
        axis.bar(range(len(keys)), [float(value) if pd.notna(value) else np.nan for value in values], color="#d95f02")
        ranged = False
        if has_bounds:
            ranged = draw_vertical_mean_bounds(axis, range(len(keys)),
                [population.loc[key, f"{name}_exposure_percent_lower_bound"] for key in keys],
                [population.loc[key, f"{name}_exposure_percent_upper_bound"] for key in keys],
                color="black", cap_width=.12)
        range_shown |= ranged
        for index, value in enumerate(values):
            axis.annotate(f"{value:.2f}%" if pd.notna(value) else "Observed unavailable", (index, value if pd.notna(value) else 0),
                          xytext=(0, 4), textcoords="offset points", ha="center", fontsize=9)
        axis.set(xticks=range(len(keys)), xticklabels=["Overall" if scope == "overall" else band + " GHz" for scope, band in keys], ylim=(0, upper),
                 ylabel="Station association-time (%)", title=f"{name.replace('_', ' ').title()} CU exposure")
    handles = [Patch(facecolor="#d95f02", label="Observed (% of paired station-time)")]
    if range_shown:
        handles.append(Line2D([], [], color="black", marker="_", linewidth=1,
                              label="Min/max (% of available station-time)"))
    figure.legend(handles=handles, loc="outside lower center", ncol=len(handles), fontsize=9)
    figure.suptitle("Station-time with high and very high CU")
    return figure


def _plots(root, tables):
    build_station_time_exposure(tables["population_exposure.csv"]).savefig(root / "station_time_exposure.png", dpi=140)
    figure = _figure(12, 5)
    axes = figure.subplots(1, 2)
    rooms = tables["entity_session_exposure.csv"]
    rooms = rooms.loc[rooms.scope.eq("classroom")]
    for axis, name in zip(axes, ("high", "very_high")):
        for band in ("all", "2.4", "5", "6"):
            values = pd.to_numeric(rooms.loc[rooms.band.eq(band), f"peak_{name}_associations"], errors="coerce").dropna().sort_values()
            if len(values):
                x, counts = np.unique(values, return_counts=True)
                # Begin at zero cumulative mass, including any observed zero peaks.
                axis.step(np.r_[x[0], x], np.r_[0, counts.cumsum() / len(values)], where="post", label=band if band == "all" else band + " GHz")
        if axis.lines:
            axis.legend()
        else:
            axis.text(.5, .5, "No complete-bin peaks", transform=axis.transAxes, ha="center")
        axis.set(xlabel="Peak simultaneous potentially affected associations", ylabel="Fraction of available classroom/session peaks",
                 ylim=(0, 1), title=name.replace("_", " ").title() + " CU")
    figure.suptitle("Classroom/class-session peak association CDFs")
    figure.savefig(root / "classroom_peak_cdf.png", dpi=140)


METHODOLOGY = """Potentially affected station reporting -- ADR-0016 v1
Start with population_exposure.csv for overall/band/infrastructure-cohort shares.
entity_session_exposure.csv provides scope=radio/ap/classroom rows, session and
source IDs, all-band/band views, local/UTC class bounds, course/enrollment context,
coverage, exact integer components and peak bin/time provenance. Radio rows
already identify band. Never sum overlapping scopes or all-band/band views.

Station-time is associated station count times bin duration in microseconds.
Observed percentages divide category station-time by all paired station-time. High uses
raw CU >=128 and includes very high (>=192). Below-high, high-only and very-high
components are disjoint. Zero denominator is unavailable. Accepted stale station
assistance participates; missing CU is never filled. Missing CU/no-beacon and
station availability are explicit. Pair coverage uses pooled radio time;
known_station_time_cu_coverage_percent is limited to available station counts,
not an estimate of unknown associations. No coverage metric removes capture bias.

ADR-0020 adds conservative missing-bin bounds; all observed fields stay unchanged.
Let A be available_station_time_us, P be paired_station_time_us, and H the
observed high/very-high station-time. missing_cu_station_time_us is A-P,
including both no-beacon bins and beacons without usable CU when station counts
are available. Companion *_station_time_us_lower_bound/upper_bound fields are
H and H+(A-P). *_exposure_percent_lower_bound/upper_bound fields are 100*H/A
and 100*(H+A-P)/A. The existing observed percentage remains 100*H/P.
All-missing CU with positive available station-time has bounds 0-100% and no
observed percentage. Zero available station-time has unavailable percentages
and zero bounded association-time within that empty known-count population.
Accepted stale station counts participate. Unknown station counts cannot
contribute to these bounds: they do not bound exposure over all expected
radio time. Station coverage and known_station_time_cu_coverage_percent remain
visible. These ranges are not confidence intervals or imputed CU. Both high
and very-high bounds use the same missing association-time, retaining nesting.
cu_bounds_metric_id identifies the additive component. The chart retains
observed bar heights and adds min/max whiskers over available station-time;
the two denominator populations are stated separately in its legend.

Peaks sum qualifying stations in the SAME bin, then take a maximum over bins
with usable CU AND station totals for every session-active radio in that view.
Any unavailable member makes that peak bin unavailable. Complete-bin coverage
uses entity/session duration, not pooled radio duration. Earliest tied peak is
retained with its source bin/UTC time and stale-assistance flag. No complete bin
means null peak, not zero. Absent project/session radios are not invented.

Project exposures pool radio components once, not averages of ratios and never
radio plus AP plus classroom sums. AP-cohort and classroom-cohort definitions
follow ADR-0015. classroom_peak_distribution.csv summarizes available physical
session peaks equally (linear-interpolated median/p90); peaks are not added.
Stale pair and high/very-high station-time components remain explicit.

Potentially affected associations are not unique devices, people, active clients
or evidence of impairment. CU describes channel conditions, possibly including
other BSSs. Missing data and incomplete sample membership limit interpretation.
Tables trace to radio_class_detail/radio_bin_detail.parquet by session/radio/bin,
then to selected observations. This derived package adds no authoritative artifact
and no capture decode; only compact entity/session records survive streaming.
"""
