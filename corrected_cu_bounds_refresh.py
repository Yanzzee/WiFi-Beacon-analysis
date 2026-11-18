"""Add CU companions to copied reports without changing saved observations."""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from band_comparison_inputs import optional_enabled
from corrected_cu_bounds import (
    CU_BOUNDS_METRIC_ID, bounded_mean_bounds, cu_mean_bounds_fields,
    cu_cdf_summary_rows, station_time_cu_bounds_fields,
)
from run_lifecycle import sha256_file


def _integer(value):
    if value is None or value is pd.NA or str(value) in {"", "nan", "None"}:
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid saved integer component: {value}") from exc
    if not number.is_finite() or number != number.to_integral_value():
        raise ValueError(f"invalid saved integer component: {value}")
    return int(number)


def _finite(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _mean(row, *, prefix="mean_cu_percent", total="radio_duration_us", valid="valid_cu_duration_us", raw="cu_raw_duration_sum"):
    expected, usable, weighted = (_integer(row.get(field)) for field in (total, valid, raw))
    if usable == 0:
        weighted = 0
    if None in (expected, usable, weighted):
        return {f"{prefix}_{bound}_bound": None for bound in ("lower", "upper")}
    return {key: value for key, value in cu_mean_bounds_fields(weighted, usable, expected, prefix=prefix).items()
            if key != "cu_bounds_metric_id"}


def _persisted_mean(mean, valid, total, *, maximum=100):
    """Use an unrounded stored mean; never invent an exact integer numerator."""
    if valid is None or total is None or (valid and _finite(mean) is None):
        return None, None
    result = bounded_mean_bounds(float(mean) * valid if valid else 0, valid, total, maximum=maximum)
    return (float(mean), float(mean)) if valid and valid == total else result


def _members(row, radios):
    scope, population = row.get("population_scope"), row.get("population_id")
    if scope == "overall":
        result = radios
    elif scope == "classroom_band":
        if not {"classroom", "band"}.issubset(radios):
            return None
        result = radios.loc[radios.classroom.eq(row.get("classroom")) & radios.band.eq(row.get("band"))]
    elif scope == "band":
        if "band" not in radios:
            return None
        result = radios.loc[radios.band.eq(population)]
    elif scope in {"ap_cohort", "classroom_cohort"}:
        field = "ap_six_ghz_enabled" if scope == "ap_cohort" else "classroom_six_ghz_enabled"
        if not {"band", field}.issubset(radios) or "_on_" not in str(population):
            return None
        band, enabled = population.split("_on_", 1)
        result = radios.loc[radios.band.eq(band) & radios[field].map(optional_enabled).eq(enabled == "with_6")]
    else:
        return None
    for field in ("ap_model", "ap_observed_5_ghz_radio_count", "ap_observed_6_ghz_radio_count"):
        if field in row:
            if field not in result:
                return None
            result = result.loc[result[field].eq(row[field])]
    return result


def _equal_bounds(row, members):
    result = {}
    for entity, keys in (("ap", ["ap_id", "class_session_id"]), ("classroom", ["classroom", "class_session_id"])):
        prefix = f"equal_{entity}_session_mean_cu_percent"
        if prefix not in row:
            continue
        intervals, complete = [], True
        needed = {*keys, "cu_raw_duration_sum", "valid_cu_duration_us", "radio_duration_us"}
        if members is not None and needed.issubset(members):
            for _, group in members.groupby(keys, sort=True, dropna=True):
                components = {field: [_integer(value) for value in group[field]]
                              for field in ("cu_raw_duration_sum", "valid_cu_duration_us", "radio_duration_us")}
                if any(None in values for values in components.values()):
                    intervals, complete = [], False
                    break
                raw, valid, total = (sum(components[field]) for field in components)
                if valid:
                    interval = cu_mean_bounds_fields(raw, valid, total)
                    intervals.append((interval["mean_cu_percent_lower_bound"], interval["mean_cu_percent_upper_bound"]))
                    complete &= valid == total
        for index, bound in enumerate(("lower", "upper")):
            result[f"{prefix}_{bound}_bound"] = (
                _finite(row[prefix]) if intervals and complete else
                float(np.mean([pair[index] for pair in intervals])) if intervals else None)
    return result


def _detail_components(path, pairs):
    """One narrow pass, retaining at most one class session for paired joins."""
    zeros, common = {}, {}
    if not path.is_file():
        return zeros, common
    parquet = pq.ParquetFile(path)
    zero_fields = {"class_session_id", "radio_group_id", "bin_duration_us", "station_available", "station_count"}
    pair_fields = {"class_session_id", "radio_group_id", "bin_duration_us", "bin_number", "cu_state", "qbss_cu_raw"}
    can_zero = zero_fields.issubset(parquet.schema_arrow.names)
    can_pair = len(pairs) and pair_fields.issubset(parquet.schema_arrow.names)
    columns = (zero_fields if can_zero else set()) | (pair_fields if can_pair else set())
    if not columns:
        return zeros, common
    requested = {sid: group for sid, group in pairs.groupby("class_session_id", sort=False)} if can_pair else {}
    pending, last_sid, closed = [], None, set()

    def finish(sid, chunks):
        if sid not in requested:
            return
        frame = pd.concat(chunks, ignore_index=True)
        valid = frame.loc[frame.cu_state.eq("valid") & frame.qbss_cu_raw.notna()]
        for row in requested[sid].to_dict("records"):
            left = valid.loc[valid.radio_group_id.eq(row["radio_5_id"]), ["bin_number", "bin_duration_us", "qbss_cu_raw"]]
            right = valid.loc[valid.radio_group_id.eq(row["radio_6_id"]), ["bin_number", "bin_duration_us", "qbss_cu_raw"]]
            joined = left.merge(right, on="bin_number", suffixes=("_5", "_6"), validate="one_to_one")
            duration = sum(int(value) for value in joined.bin_duration_us_5)
            if duration != _integer(row.get("common_valid_cu_duration_us")) or not joined.bin_duration_us_5.eq(joined.bin_duration_us_6).all():
                raise ValueError("saved matched-radio duration disagrees with radio detail")
            common[sid, row["radio_5_id"], row["radio_6_id"]] = tuple(
                sum(int(raw) * int(duration) for raw, duration in zip(joined[f"qbss_cu_raw_{band}"], joined[f"bin_duration_us_{band}"]))
                for band in (5, 6))

    for batch in parquet.iter_batches(batch_size=65_536, columns=sorted(columns)):
        frame = batch.to_pandas()
        if can_zero:
            for key, group in frame.groupby(["class_session_id", "radio_group_id"], sort=False):
                mask = group.station_available.fillna(False) & group.station_count.eq(0)
                zeros[key] = zeros.get(key, 0) + sum(int(value) for value in group.loc[mask, "bin_duration_us"])
        if can_pair:
            segments = frame.class_session_id.ne(frame.class_session_id.shift()).cumsum()
            for _, group in frame.groupby(segments, sort=False):
                sid = str(group.class_session_id.iloc[0])
                if sid != last_sid:
                    if last_sid is not None:
                        finish(last_sid, pending)
                        closed.add(last_sid)
                    if sid in closed:
                        raise ValueError("saved radio detail must keep class sessions contiguous for bounded refresh")
                    pending, last_sid = [], sid
                if sid in requested:
                    pending.append(group)
    if last_sid is not None:
        finish(last_sid, pending)
    return zeros, common


def backfill_cu_bounds(root, *, source_root, configuration):
    """Update only the refresh copy; return source hashes and added columns."""
    root, source_root = Path(root), Path(source_root)
    records, cache = [], {}

    def read(relative):
        if relative not in cache:
            path = root / relative
            cache[relative] = pd.read_csv(path, dtype=str, keep_default_na=False) if path.is_file() else pd.DataFrame()
        return cache[relative]

    def write(relative, frame, sources, *, fields=None):
        path = root / relative
        source_records = [{"path": item, "sha256": sha256_file(source_root / item)}
                          for item in dict.fromkeys(sources) if (source_root / item).is_file()]
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False, lineterminator="\n")
        cache[relative] = frame.astype(object).where(pd.notna(frame), None)
        records.append({"path": relative, "added_columns": fields or list(frame.columns),
                        "sources": source_records, "sha256": sha256_file(path)})

    def add(relative, derived, sources):
        frame = read(relative)
        if frame.empty or not derived:
            return
        names = list(dict.fromkeys(name for row in derived for name in row if name not in frame))
        if not names:
            return
        if len(derived) != len(frame):
            raise ValueError("bounds refresh row alignment mismatch")
        result = frame.copy()
        for name in names:
            result[name] = pd.Series([row.get(name) for row in derived], dtype=object)
        write(relative, result, [relative, *sources], fields=names)

    band = "reports/band_comparison/"
    radio_path, pair_path = band + "radio_class_comparison.csv", band + "matched_ap_radio_sessions.csv"
    radios, pairs = read(radio_path), read(pair_path)
    needs_zero = len(radios) and "zero_station_available_duration_us" not in radios
    needs_common = len(pairs) and "common_5_cu_raw_duration_sum" not in pairs
    detail_path = "reports/radio_class_detail/radio_bin_detail.parquet"
    zeros, common = _detail_components(root / detail_path, pairs if needs_common else pd.DataFrame()) if needs_zero or needs_common else ({}, {})
    if len(radios) and "mean_cu_percent" in radios:
        derived = []
        for row in radios.to_dict("records"):
            zero = _integer(row.get("zero_station_available_duration_us"))
            if zero is None:
                zero = zeros.get((row.get("class_session_id"), row.get("radio_group_id")))
            row["zero_station_available_duration_us"] = zero
            derived.append({**_mean(row), **_mean(row, prefix="mean_cu_with_zero_stations_percent",
                total="zero_station_available_duration_us", valid="zero_station_valid_cu_duration_us", raw="zero_station_cu_raw_duration_sum"),
                "zero_station_available_duration_us": zero, "cu_bounds_metric_id": CU_BOUNDS_METRIC_ID})
        add(radio_path, derived, [detail_path] if needs_zero else [])
        radios = read(radio_path)

    for filename in ("population_summary.csv", "model_population_summary.csv", "classroom_band_summary.csv"):
        path = band + filename
        frame, derived = read(path), []
        for row in frame.to_dict("records"):
            if "mean_cu_percent" not in row:
                continue
            members = _members(row, radios) if len(radios) else None
            zero = _integer(row.get("zero_station_available_duration_us"))
            if zero is None and members is not None and "zero_station_available_duration_us" in members:
                values = [_integer(value) for value in members.zero_station_available_duration_us]
                zero = sum(values) if None not in values else None
            row["zero_station_available_duration_us"] = zero
            derived.append({**_mean(row), **_equal_bounds(row, members),
                **_mean(row, prefix="mean_cu_with_zero_stations_percent", total="zero_station_available_duration_us",
                        valid="zero_station_valid_cu_duration_us", raw="zero_station_cu_raw_duration_sum"),
                "zero_station_available_duration_us": zero, "cu_bounds_metric_id": CU_BOUNDS_METRIC_ID})
        add(path, derived, [radio_path])

    pair_rows = []
    for row in pairs.to_dict("records"):
        total, usable = (_integer(row.get(field)) for field in ("possible_duration_us", "common_valid_cu_duration_us"))
        sums = common.get((row.get("class_session_id"), row.get("radio_5_id"), row.get("radio_6_id")))
        fields = {"cu_bounds_metric_id": CU_BOUNDS_METRIC_ID}
        intervals = []
        for index, frequency in enumerate((5, 6)):
            weighted = _integer(row.get(f"common_{frequency}_cu_raw_duration_sum"))
            if weighted is None and sums is not None:
                weighted = sums[index]
            fields[f"common_{frequency}_cu_raw_duration_sum"] = weighted
            if None not in (weighted, usable, total):
                limits = cu_mean_bounds_fields(weighted, usable, total)
                interval = (limits["mean_cu_percent_lower_bound"], limits["mean_cu_percent_upper_bound"])
            else:
                interval = _persisted_mean(row.get(f"matched_mean_{frequency}_cu_percent"), usable, total)
            intervals.append(interval)
            fields.update({f"paired_window_mean_{frequency}_cu_percent_{bound}_bound": interval[i] for i, bound in enumerate(("lower", "upper"))})
        for i, bound in enumerate(("lower", "upper")):
            fields[f"paired_window_6_minus_5_cu_percentage_points_{bound}_bound"] = (
                intervals[1][i] - intervals[0][1-i] if intervals[1][i] is not None and intervals[0][1-i] is not None else None)
        pair_rows.append(fields)
    add(pair_path, pair_rows, [detail_path] if common else [])

    for filename in ("entity_session_exposure.csv", "population_exposure.csv"):
        path = "reports/affected_stations/" + filename
        derived = []
        for row in read(path).to_dict("records"):
            values = [_integer(row.get(field)) for field in ("available_station_time_us", "paired_station_time_us", "high_station_time_us", "very_high_station_time_us")]
            derived.append(station_time_cu_bounds_fields(*values) if None not in values else {
                **{f"{name}_{measure}_{bound}_bound": None for name in ("high", "very_high")
                   for measure in ("station_time_us", "exposure_percent") for bound in ("lower", "upper")},
                "missing_cu_station_time_us": None, "cu_bounds_metric_id": CU_BOUNDS_METRIC_ID})
        add(path, derived, [])

    _backfill_core(root, source_root, configuration, read, add)
    _backfill_cdfs(root, read, write)
    _backfill_windows(root, source_root, records)
    if records:
        note = ("\n\nCU missing-bin bounds in this refreshed report -- ADR-0020\n"
                "Observed source fields remain unchanged. Minimum/maximum CU means and CDFs allow missing bins 0%/100% CU. "
                "Each statistic retains its weighting; station-time bounds condition on available station counts and exclude unknown counts. "
                "These are not confidence intervals or imputed CU. Exact saved sums/durations are used when available; "
                "unrounded persisted means may supply floating-point weighted sums without fabricating exact integer numerators. "
                "Missing supporting components leave bounds unavailable. report_refresh.json records updated tables and source hashes. "
                "This rendering note supersedes any earlier staged-presentation status in the source methodology below/above.\n")
        families = {Path(record["path"]).parent for record in records if record["path"].startswith("reports/")}
        for family in families:
            methodology = root / family / "methodology.txt"
            if methodology.is_file():
                text = methodology.read_text(encoding="utf-8")
                if "CU missing-bin bounds in this refreshed report" not in text:
                    methodology.write_text(text + note, encoding="utf-8", newline="\n")
    return records


def _backfill_core(root, source_root, configuration, read, add):
    review_path = "exports/csv/radio_class_summary.csv"
    core = read(review_path)
    sources = [review_path]
    required = {"class_session_id", "radio_group_id", "average_cu_raw", "class_duration_us", "valid_cu_duration_us"}
    canonical = source_root / "radio_class_summary.parquet"
    if not required.issubset(core) and canonical.is_file():
        available = pq.read_schema(canonical).names
        if required.issubset(available):
            core = pq.read_table(canonical, columns=sorted(required | ({"average_cu_ratio", "band"} & set(available)))).to_pandas()
            sources = ["radio_class_summary.parquet"]
    lookup, review_rows = {}, []
    for row in core.to_dict("records") if required.issubset(core) else []:
        valid, total = _integer(row["valid_cu_duration_us"]), _integer(row["class_duration_us"])
        interval = _persisted_mean(row["average_cu_raw"], valid, total, maximum=255)
        fields = {"cu_bounds_metric_id": CU_BOUNDS_METRIC_ID}
        for index, bound in enumerate(("lower", "upper")):
            raw = interval[index]
            fields[f"average_cu_raw_{bound}_bound"] = raw
            fields[f"average_cu_ratio_{bound}_bound"] = (
                _finite(row.get("average_cu_ratio")) if valid and valid == total and _finite(row.get("average_cu_ratio")) is not None
                else raw / 255 if raw is not None else None)
        lookup[str(row["class_session_id"]), str(row["radio_group_id"])] = {**row, **fields}
        review_rows.append(fields)
    if required.issubset(read(review_path)):
        add(review_path, review_rows, sources)
    band_radios = read("reports/band_comparison/radio_class_comparison.csv")
    counts = {(row["class_session_id"], row["radio_group_id"]): _integer(row.get("radio_bin_rows"))
              for row in band_radios.to_dict("records") if {"class_session_id", "radio_group_id"}.issubset(row)}
    widths = configuration.get("bin_widths_us")
    if widths is None:
        widths = [configuration.get("bin_width_us")]
    width = None
    if isinstance(widths, (list, tuple)) and widths:
        parsed = [_integer(value) for value in widths]
        if None not in parsed and len(set(parsed)) == 1 and parsed[0] > 0:
            width = parsed[0]
    path = "reports/cu_coverage_streak/radio_class_report.csv"
    derived = []
    for row in read(path).to_dict("records"):
        if "average_cu_percent" not in row:
            continue
        key = row.get("class_session_id"), row.get("radio_group_id")
        source = lookup.get(key, {})
        expected = counts.get(key)
        duration = _integer(source.get("class_duration_us"))
        if expected is None and width and duration is not None:
            expected = (duration + width - 1) // width
        derived.append({**{f"average_cu_percent_{bound}_bound": source.get(f"average_cu_ratio_{bound}_bound") * 100
                          if source.get(f"average_cu_ratio_{bound}_bound") is not None else None for bound in ("lower", "upper")},
                        "expected_radio_bin_count": expected, "cu_bounds_metric_id": CU_BOUNDS_METRIC_ID})
    add(path, derived, [*sources, "reports/band_comparison/radio_class_comparison.csv", "manifest.json"])
    report = read(path)
    population_path = "reports/cu_coverage_streak/band_population_report.csv"
    population_rows = []
    for row in read(population_path).to_dict("records"):
        members = None
        if {"band", "ap_has_6_ghz", "class_session_id", "radio_group_id"}.issubset(report):
            population = row.get("population_id")
            if population == "all_5_ghz":
                members = report.loc[report.band.eq("5")]
            elif population in {"5_ghz_on_6_ghz_ap", "6_ghz_on_6_ghz_ap"}:
                members = report.loc[report.band.eq(population[0]) & report.ap_has_6_ghz.map(optional_enabled).eq(True)]
        lower = upper = expected = None
        if members is not None:
            components = [lookup.get((part.class_session_id, part.radio_group_id)) for part in members.itertuples(index=False)]
            if all(item is not None for item in components):
                total = sum(_integer(item["class_duration_us"]) for item in components)
                valid = sum(_integer(item["valid_cu_duration_us"]) for item in components)
                weighted = sum(float(item["average_cu_raw"]) * _integer(item["valid_cu_duration_us"])
                               for item in components if _integer(item["valid_cu_duration_us"]))
                lo, hi = bounded_mean_bounds(weighted, valid, total, maximum=255)
                lower, upper = (lo * (100 / 255), hi * (100 / 255)) if lo is not None else (None, None)
                if valid and valid == total:
                    lower = upper = _finite(row.get("average_cu_percent"))
            if "expected_radio_bin_count" in members:
                numbers = [_integer(value) for value in members.expected_radio_bin_count]
                expected = sum(numbers) if None not in numbers else None
        population_rows.append({"average_cu_percent_lower_bound": lower, "average_cu_percent_upper_bound": upper,
                                "expected_radio_bin_values": expected, "cu_bounds_metric_id": CU_BOUNDS_METRIC_ID})
    add(population_path, population_rows, [path, *sources])


def _backfill_cdfs(root, read, write):
    distribution_path = "reports/band_comparison/cu_distribution.csv"
    distribution = read(distribution_path)
    required = {"population_scope", "population_id", "cu_raw", "valid_radio_bin_count", "valid_radio_duration_us"}
    histograms = defaultdict(lambda: [[0] * 256, [0] * 256])
    has_durations = required.issubset(distribution)
    for row in distribution.to_dict("records") if has_durations else []:
        raw, bins, duration = (_integer(row[field]) for field in ("cu_raw", "valid_radio_bin_count", "valid_radio_duration_us"))
        if not 0 <= raw <= 255 or bins < 0 or duration < 0:
            raise ValueError("invalid saved CU distribution component")
        values = histograms[row["population_scope"], row["population_id"]]
        values[0][raw] += bins
        values[1][raw] += duration
    if not has_durations:
        core = read("exports/csv/radio_class_summary.csv")
        if not {"valid_cu_raw_counts", "band"}.issubset(core):
            return
        distribution_path = "exports/csv/radio_class_summary.csv"
        for row in core.to_dict("records"):
            counts = json.loads(row["valid_cu_raw_counts"])
            # The shared helper validates support and integer weights.
            cu_cdf_summary_rows(counts, None, scope="", population="", weighting="radio_bins")
            for key in (("overall", "all"), ("band", row["band"])):
                target = histograms[key][0]
                for raw, value in enumerate(counts):
                    target[raw] += value
    populations_path = "reports/band_comparison/population_summary.csv"
    populations = read(populations_path)
    count_rows, duration_rows = [], []
    known = {}
    known_valid = {}
    for row in populations.to_dict("records"):
        key = row.get("population_scope"), row.get("population_id")
        known[key] = _integer(row.get("radio_bin_rows"))
        known_valid[key] = _integer(row.get("valid_cu_duration_us"))
        if has_durations and (key in histograms or known_valid[key] == 0):
            duration_rows.extend(cu_cdf_summary_rows(histograms[key][1], _integer(row.get("radio_duration_us")),
                scope=key[0], population=key[1], weighting="radio_duration_us"))
    radio_path = "reports/cu_coverage_streak/radio_class_report.csv"
    radios = read(radio_path)
    if {"band", "expected_radio_bin_count"}.issubset(radios):
        for key, frame in [(('overall', 'all'), radios), *[(('band', band), radios.loc[radios.band.eq(band)]) for band in ("2.4", "5", "6", "unknown")]]:
            if known.get(key) is None:
                counts = [_integer(value) for value in frame.expected_radio_bin_count]
                known[key] = sum(counts) if None not in counts else None
    for scope, population in [("overall", "all"), *[("band", band) for band in ("2.4", "5", "6", "unknown")]]:
        key = scope, population
        if key not in histograms and known.get(key) != 0 and known_valid.get(key) != 0:
            continue
        count_rows.extend(cu_cdf_summary_rows(histograms[scope, population][0], known.get((scope, population)),
            scope=scope, population=population, weighting="radio_bins"))
    for path, rows in (("reports/cu_coverage_streak/cu_cdf_bounds.csv", count_rows),
                       ("reports/band_comparison/cu_cdf_bounds.csv", duration_rows)):
        if rows and not (root / path).exists():
            write(path, pd.DataFrame(rows), [distribution_path, populations_path, radio_path])


def _backfill_windows(root, source_root, records):
    relative = "reports/beacon_quality/beacon_quality_windows.parquet"
    path = root / relative
    if not path.is_file():
        return
    new_types = {"cu_bounds_metric_id": pa.string(), "cu_raw_duration_sum": pa.int64(),
                 "previous_window_cu_raw_duration_sum": pa.int64(), "previous_window_duration_us": pa.int64(),
                 **{f"{prefix}_{bound}_bound": pa.float64() for prefix in ("cu_mean_percent", "previous_window_cu_mean_percent")
                    for bound in ("lower", "upper")}}
    temporary = path.with_suffix(".bounds.tmp")
    original_hash = sha256_file(source_root / relative)
    previous = {}
    try:
        with pq.ParquetFile(path) as parquet:
            additions = {name: kind for name, kind in new_types.items() if name not in parquet.schema_arrow.names}
            if not additions:
                return
            schema = pa.schema([*parquet.schema_arrow, *[pa.field(name, kind) for name, kind in additions.items()]], metadata=parquet.schema_arrow.metadata)
            with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
                for batch in parquet.iter_batches(batch_size=65_536):
                    calculated = []
                    for row in batch.to_pylist():
                        key = tuple(row.get(field) for field in ("class_session_id", "radio_group_id", "capture_family"))
                        number = _integer(row.get("window_number"))
                        total = _integer(row.get("window_duration_us"))
                        preceding = previous.get(key)
                        lag_duration = _integer(row.get("previous_window_duration_us"))
                        if lag_duration is None and preceding is not None and preceding[0] is not None and number == preceding[0] + 1:
                            lag_duration = preceding[1]
                        if number == 0:
                            lag_duration = None
                        previous[key] = (number, total)
                        fields = {"cu_bounds_metric_id": CU_BOUNDS_METRIC_ID, "previous_window_duration_us": lag_duration}
                        for prefix, duration in (("", total), ("previous_window_", lag_duration)):
                            valid = _integer(row.get(prefix + "cu_valid_duration_us"))
                            weighted = _integer(row.get(prefix + "cu_raw_duration_sum"))
                            if None not in (duration, valid, weighted):
                                limits = cu_mean_bounds_fields(weighted, valid, duration)
                                interval = (limits["mean_cu_percent_lower_bound"], limits["mean_cu_percent_upper_bound"])
                                if valid and valid == duration:
                                    interval = (row.get(prefix + "cu_mean_percent"),) * 2
                            else:
                                interval = _persisted_mean(row.get(prefix + "cu_mean_percent"), valid, duration)
                            fields.update({f"{prefix}cu_mean_percent_{bound}_bound": interval[i] for i, bound in enumerate(("lower", "upper"))})
                        calculated.append(fields)
                    table = pa.Table.from_batches([batch])
                    for name, kind in additions.items():
                        table = table.append_column(pa.field(name, kind), pa.array([row.get(name) for row in calculated], type=kind))
                    writer.write_table(table)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    records.append({"path": relative, "added_columns": list(additions), "sources": [{"path": relative, "sha256": original_hash}],
                    "mean_source": "exact_raw_sum_when_saved; otherwise_full_precision_persisted_Parquet_mean",
                    "sha256": sha256_file(path)})
