from __future__ import annotations

from collections import Counter
from pathlib import Path
from zoneinfo import ZoneInfo

from matplotlib.figure import Figure
import pandas as pd
import pyarrow.parquet as pq

from corrected_radio_bin_reports import (
    CORRELATION_FILENAME,
    METHODOLOGY_FILENAME,
    PLOT_INDEX_FILENAME,
    RADIO_BIN_FILENAME,
    RELATIONSHIP_BAND_PLOT_FILENAME,
    RELATIONSHIP_PLOT_FILENAME,
    REPORT_DIRECTORY,
    _relationship_axis,
    _radio_plot_title,
    _station_axis_limit,
    _write_radio_plot,
    build_radio_bin_detail,
    write_radio_bin_report_package,
)
from corrected_summary_output import RADIO_BIN_CU_COLUMNS
from corrected_admission_selection import empty_admission


def _cu_rows() -> pd.DataFrame:
    start = pd.Timestamp("2026-10-03T15:00:00Z")
    rows = []
    for bin_number, (state, cu) in enumerate(
        (("valid", 80), ("missing_cu", None), ("valid", 160))
    ):
        bin_start = start + pd.Timedelta(seconds=bin_number)
        values = {
            "run_id": "run-1",
            "schema_id": "beacon.schema/analysis-results/v1",
            "metric_id": "beacon.metric/corrected-radio-bin/v1",
            "class_session_id": "C1",
            "classroom": "ROOM1",
            "radio_group_id": "AP-01/radio/5",
            "ap_id": "AP-01",
            "band": "5",
            "channel": 36,
            "bin_number": bin_number,
            "bin_start": bin_start,
            "bin_end": bin_start + pd.Timedelta(seconds=1),
            "bin_duration_us": 1_000_000,
            "width_status": "complete",
            "cu_state": state,
            "qbss_cu_raw": cu,
            "source_ssid": "eduroam" if cu is not None else None,
            "source_bssid": "aa:00:00:00:00:01" if cu is not None else None,
            "source_selection_method": "primary_ssid" if cu is not None else None,
            "source_observation_time": (
                bin_start + pd.Timedelta(milliseconds=100)
                if cu is not None
                else None
            ),
            "observed_member_bssid_count": 1,
            "valid_cu_candidate_count": int(cu is not None),
            **empty_admission("missing_capacity"),
        }
        rows.append(values)
    return pd.DataFrame(rows, columns=RADIO_BIN_CU_COLUMNS)


def _station_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "class_session_id": ["C1", "C1"],
            "classroom": ["ROOM1", "ROOM1"],
            "bin_number": [0, 1],
            "rollup_type": ["radio", "radio"],
            "rollup_id": ["AP-01/radio/5", "AP-01/radio/5"],
            "station_count": [10, 10],
            "available": [True, True],
            "stale_assisted": [False, True],
        }
    )


def test_radio_bin_detail_joins_station_state_without_inventing_zero():
    detail = build_radio_bin_detail(_cu_rows(), _station_rows())

    assert detail["station_count"].iloc[:2].astype("UInt64").tolist() == [10, 10]
    assert detail["station_available"].tolist() == [True, True, False]
    assert detail["station_stale_assisted"].tolist() == [False, True, False]
    assert pd.isna(detail.iloc[2]["station_count"])
    assert detail["cu_state"].tolist() == ["valid", "missing_cu", "valid"]


def test_radio_plot_uses_stable_station_scale_and_local_compact_title():
    detail = build_radio_bin_detail(_cu_rows(), _station_rows())

    assert _station_axis_limit(pd.Series([0, 99, 100])) == 100
    assert _station_axis_limit(pd.Series([0, 101])) == 110
    assert _station_axis_limit(pd.Series([125])) == 130
    assert _radio_plot_title(detail, ZoneInfo("America/Denver")) == (
        "AP-01 | Radio 5 | 2026-10-03 | 09:00–09:00"
    )


def test_relationship_plot_coarsens_display_without_losing_pair_weights(monkeypatch):
    axis = Figure().subplots()
    captured = {}
    original_hexbin = axis.hexbin

    def capture_hexbin(*args, **kwargs):
        captured.update(kwargs)
        return original_hexbin(*args, **kwargs)

    monkeypatch.setattr(axis, "hexbin", capture_hexbin)
    counts = Counter({(0, 0): 7, (128, 12): 20, (255, 100): 3})
    artist = _relationship_axis(
        axis, counts, title="Density", station_limit=100,
    )

    assert captured["gridsize"] == (40, 30)
    assert artist.get_array().sum() == sum(counts.values())
    assert counts == {(0, 0): 7, (128, 12): 20, (255, 100): 3}


def test_radio_plot_format_has_fill_no_cu_markers_and_local_time_axis(
    tmp_path: Path,
    monkeypatch,
):
    detail = build_radio_bin_detail(_cu_rows(), _station_rows())
    captured: dict[str, Figure] = {}

    def capture_figure(figure, *_args, **_kwargs):
        captured["figure"] = figure

    monkeypatch.setattr(Figure, "savefig", capture_figure)
    _write_radio_plot(
        detail,
        tmp_path / "ignored.png",
        display_timezone="America/Denver",
    )

    cu_axis, station_axis = captured["figure"].axes
    assert tuple(captured["figure"].get_size_inches()) == (24.0, 10.0)
    measured = next(
        line for line in cu_axis.lines if line.get_label() == "Measured CU"
    )
    bridge = next(
        line
        for line in cu_axis.lines
        if line.get_label() == "Unknown CU"
    )
    station_line = next(
        line
        for line in station_axis.lines
        if line.get_label() == "Station count"
    )
    assert measured.get_marker() in {None, "None"}
    assert cu_axis.collections  # measured CU fill_between collection
    assert not cu_axis.patches  # no vertical unknown-CU shading
    assert bridge.get_linestyle() == "--"
    assert bridge.get_color() == "#1f77b4"
    assert station_line.get_color() == "#f58518"
    assert not any(
        line.get_label() == "Unknown station count"
        for line in station_axis.lines
    )  # trailing gaps have no next available endpoint
    assert station_axis.get_ylim() == (0.0, 100.0)
    assert cu_axis.get_xlabel() == "Time (America/Denver)"
    assert cu_axis.get_title() == (
        "AP-01 | Radio 5 | 2026-10-03 | 09:00–09:00"
    )


def test_radio_plot_bridges_unavailable_station_bins_from_stale_endpoint(
    tmp_path: Path,
    monkeypatch,
):
    detail = build_radio_bin_detail(_cu_rows(), _station_rows())
    detail["station_count"] = [10.0, float("nan"), 20.0]
    detail["station_available"] = [True, False, True]
    detail["station_stale_assisted"] = [True, False, False]
    captured = []
    monkeypatch.setattr(Figure, "savefig", lambda figure, *_a, **_k: captured.append(figure))

    _write_radio_plot(detail, tmp_path / "gap.png", display_timezone="America/Denver")

    station_axis = captured[0].axes[1]
    bridge = next(
        line for line in station_axis.lines
        if line.get_label() == "Unknown station count"
    )
    assert bridge.get_ydata().tolist() == [10.0, 20.0]
    assert bridge.get_color() == "#f58518"
    assert bridge.get_linestyle() == "--"
    assert pd.isna(detail.loc[1, "station_count"])
    assert not detail.loc[1, "station_available"]


def test_radio_bin_package_writes_opt_in_plot_and_unknown_cu_methodology(
    tmp_path: Path,
    monkeypatch,
):
    detail = build_radio_bin_detail(_cu_rows(), _station_rows())
    captured = []
    original_savefig = Figure.savefig

    def capture_figure(figure, *args, **kwargs):
        captured.append(figure)
        return original_savefig(figure, *args, **kwargs)

    monkeypatch.setattr(Figure, "savefig", capture_figure)

    written = write_radio_bin_report_package(
        tmp_path,
        detail_parts=[detail],
        create_plots=True,
        primary_ssid="eduroam",
        display_timezone="America/Denver",
        class_sessions=pd.DataFrame([{
            "class_session_id": "C1",
            "course_labels": "COURSE-A + COURSE-B",
            "capacity": 65,
            "enrolled": 42,
        }]),
        configured_aps=pd.DataFrame({
            "Hostname": ["AP-01", "AP-02", "AP-02"],
            "Building-Room": ["ROOM1", "ROOM1", "ROOM1"],
            "Model": ["AP-555", "AP-635", "AP-635"],
        }),
    )

    root = tmp_path / REPORT_DIRECTORY
    assert written.rows == 3
    assert written.plot_count == 1
    assert written.correlation_rows == 2
    annotation = captured[0].axes[1].texts[0].get_text()
    assert "Building-room: ROOM1" in annotation
    assert "Subject: COURSE-A + COURSE-B" in annotation
    assert "Seats: 65 | Enrollment: 42" in annotation
    assert "Room AP count (configured): 2" in annotation
    assert "AP model: AP-555" in annotation
    assert "Radio band: 5 GHz" in annotation
    assert pq.read_table(root / RADIO_BIN_FILENAME).num_rows == 3
    index = pd.read_csv(root / PLOT_INDEX_FILENAME)
    assert len(index) == 1
    assert (root / index.iloc[0]["path"]).is_file()
    correlation = pd.read_csv(root / CORRELATION_FILENAME)
    overall = correlation.loc[correlation["population_type"].eq("overall")].iloc[0]
    assert overall["total_radio_bin_rows"] == 3
    assert overall["valid_pair_rows"] == 1
    assert overall["valid_pair_coverage"] == 1 / 3
    assert overall["stale_assisted_pair_rows"] == 0
    assert pd.isna(overall["pearson_correlation"])
    assert pd.isna(overall["spearman_correlation"])
    assert (root / RELATIONSHIP_PLOT_FILENAME).is_file()
    assert (root / RELATIONSHIP_BAND_PLOT_FILENAME).is_file()
    methodology = (root / METHODOLOGY_FILENAME).read_text(encoding="utf-8")
    assert "Unknown CU" in methodology
    assert "remains blank" in methodology
    assert "visual interpolation" in methodology
    assert "not data or an imputed metric value" in methodology
    assert "Plot display timezone: America/Denver" in methodology
    assert "station axis is 0-100" in methodology
    assert "Orange station count" in methodology
    assert "tie-aware Spearman" in methodology
    assert "No p-values" in methodology
    assert "40 by 30 display grid" in methodology


def test_radio_bin_relationship_reports_exact_descriptive_correlations(
    tmp_path: Path,
):
    detail = build_radio_bin_detail(_cu_rows(), _station_rows())
    detail.loc[:, "cu_state"] = "valid"
    detail.loc[:, "qbss_cu_raw"] = [40, 80, 120]
    detail.loc[:, "station_available"] = True
    detail.loc[:, "station_count"] = [5, 10, 15]
    detail.loc[:, "station_stale_assisted"] = [False, True, False]

    write_radio_bin_report_package(tmp_path, detail_parts=[detail])

    report = pd.read_csv(tmp_path / REPORT_DIRECTORY / CORRELATION_FILENAME)
    assert set(report["population_type"]) == {"overall", "band"}
    assert report["valid_pair_rows"].eq(3).all()
    assert report["stale_assisted_pair_rows"].eq(1).all()
    assert report["pearson_correlation"].eq(1.0).all()
    assert report["spearman_correlation"].eq(1.0).all()
