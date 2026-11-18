from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from corrected_beacon_selection import select_bssid_session_observations
import selected_observation_output as selected_output
from selected_observation_output import (
    CORRECTED_METRIC_ID,
    RESULT_SCHEMA_ID,
    SELECTED_OBSERVATION_ARROW_SCHEMA,
    build_selected_observations,
    read_selected_observations,
    write_selected_observation_partitions,
    write_selected_observations,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "corrected_semantics"


def _integrated_selection() -> pd.DataFrame:
    observations = pd.read_csv(
        FIXTURE_ROOT / "input" / "observations.csv",
        dtype="string",
        keep_default_na=False,
    )
    observations = observations.loc[
        observations["fixture_id"].eq("CF05_10")
    ].drop(columns=["expected_observation_time", "expected_observation_source"])
    identity = pd.read_csv(
        FIXTURE_ROOT / "input" / "identity_evidence.csv",
        dtype="string",
        keep_default_na=False,
    )
    identity = identity.loc[identity["fixture_id"].eq("CF05_10")]
    observations = observations.merge(
        identity[["bssid", "band", "channel"]],
        on="bssid",
        how="left",
        validate="many_to_one",
    )
    class_row = pd.read_csv(
        FIXTURE_ROOT / "input" / "classes.csv", dtype="string"
    ).loc[lambda frame: frame["fixture_id"].eq("CF05_10")].iloc[0]
    result = select_bssid_session_observations(
        observations,
        class_session_id=class_row["class_session_id"],
        class_start=class_row["class_start"],
        class_end=class_row["class_end"],
        bin_width_us=int(class_row["bin_width_us"]),
    )
    selected = result.selected.copy()
    selected["classroom"] = class_row["classroom"]
    return selected


def test_selected_observation_builder_matches_accepted_oracle():
    actual = build_selected_observations(
        _integrated_selection(), run_id="oracle-corrected-v1"
    )
    expected = pd.read_csv(
        FIXTURE_ROOT / "expected" / "selected_observations.csv",
        dtype="string",
        keep_default_na=False,
    )

    assert len(actual) == len(expected) == 15
    assert actual.columns.tolist() == SELECTED_OBSERVATION_ARROW_SCHEMA.names
    assert set(actual["run_id"]) == {"oracle-corrected-v1"}
    assert set(actual["schema_id"]) == {RESULT_SCHEMA_ID}
    assert set(actual["metric_id"]) == {CORRECTED_METRIC_ID}
    assert {value.isoformat() for value in actual["class_date"]} == {
        "2026-01-15"
    }
    assert actual[["class_session_id", "bssid", "bin_number"]].duplicated().sum() == 0
    assert set(actual["source_frame"]) == set(expected["source_frame"])
    assert actual["qbss_cu_raw"].isna().sum() == 3
    assert 0 in set(actual["qbss_cu_raw"].dropna())
    assert "skipped_intervals_before" in actual

    oracle_columns = expected.columns.tolist()
    actual_oracle = actual[oracle_columns].copy()
    timestamp_columns = [
        "bin_start",
        "bin_end",
        "observation_time",
        "tsf_origin",
        "tsf_derived_time",
    ]
    integer_columns = [
        "bin_number",
        "bin_duration_us",
        "tsf_us",
        "duplicate_views_collapsed",
        "qbss_cu_raw",
        "qbss_station_count",
        "qbss_admission_capacity",
    ]
    for frame in (actual_oracle, expected):
        for column in timestamp_columns:
            frame[column] = pd.to_datetime(frame[column], utc=True)
        for column in integer_columns:
            frame[column] = pd.to_numeric(frame[column]).astype("Int64")
        for column in set(oracle_columns).difference(
            timestamp_columns, integer_columns
        ):
            frame[column] = frame[column].fillna("").astype("string")
        frame.sort_values(
            ["class_session_id", "bssid", "bin_number"],
            kind="stable",
            inplace=True,
        )
        frame.reset_index(drop=True, inplace=True)
    historical_timing = {"metric_id", "tsf_origin", "tsf_derived_time", "timing_method",
                         "tsf_status", "unusable_tsf_reason", "duplicate_views_collapsed"}
    compare = [column for column in actual_oracle if column not in historical_timing]
    pd.testing.assert_frame_equal(actual_oracle[compare], expected[compare])
    assert actual_oracle.timing_method.eq("observation_time").all()
    assert actual_oracle.tsf_derived_time.isna().all()


def test_selected_observation_writer_partitions_and_round_trips(tmp_path: Path):
    selected = _integrated_selection()
    path = write_selected_observations(
        selected,
        output_root=tmp_path / "output" / "runs",
        run_id="test-run",
    )

    assert path == (
        tmp_path / "output" / "runs" / "test-run" / "selected_observations"
    )
    partition = path / "class_date=2026-01-15" / "part-00000.parquet"
    assert partition.is_file()
    assert "class_date" not in pq.read_schema(partition).names

    table = read_selected_observations(path)
    assert table.schema == SELECTED_OBSERVATION_ARROW_SCHEMA
    assert table.num_rows == 15
    assert set(table.column("class_date").to_pylist()) == {
        pd.Timestamp("2026-01-15").date()
    }
    assert table.column("qbss_cu_raw").null_count == 3

    with pytest.raises(FileExistsError, match="already exists"):
        write_selected_observations(
            selected,
            output_root=tmp_path / "output" / "runs",
            run_id="test-run",
        )


def test_bounded_writer_streams_distinct_class_session_parts(tmp_path: Path):
    selected = _integrated_selection()
    first = selected.iloc[:8].copy()
    second = selected.iloc[8:].copy()
    first["class_session_id"] = "bounded-a"
    second["class_session_id"] = "bounded-b"

    written = write_selected_observation_partitions(
        iter([first, second]),
        output_root=tmp_path,
        run_id="bounded-run",
    )

    partition = written.path / "class_date=2026-01-15"
    partition_file = partition / "part-00000.parquet"
    assert partition_file.is_file()
    assert pq.ParquetFile(partition_file).num_row_groups == 2
    assert written.rows == 15
    assert written.first_observation_time is not None
    assert written.last_observation_time is not None
    assert read_selected_observations(written.path).num_rows == 15


def test_bounded_writer_rejects_one_session_across_multiple_parts(
    tmp_path: Path,
):
    selected = _integrated_selection()
    with pytest.raises(ValueError, match="more than one bounded part"):
        write_selected_observation_partitions(
            [selected.iloc[:8], selected.iloc[8:]],
            output_root=tmp_path,
            run_id="repeated-session-run",
        )

    run_directory = tmp_path / "repeated-session-run"
    assert not (run_directory / "selected_observations").exists()
    assert list(run_directory.glob(".selected_observations.*.tmp")) == []


def test_writer_reuses_one_validated_arrow_table_across_date_partitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    first_date = _integrated_selection()
    second_date = first_date.copy()
    second_date["class_session_id"] = (
        second_date["class_session_id"].astype(str) + "-next-day"
    )
    for column in [
        "bin_start",
        "bin_end",
        "observation_time",
        "tsf_origin",
        "tsf_derived_time",
    ]:
        second_date[column] = pd.to_datetime(
            second_date[column], utc=True
        ) + pd.Timedelta(days=1)
    selected = pd.concat([first_date, second_date], ignore_index=True)

    calls = 0
    original_rows_to_table = selected_output._rows_to_table

    def counting_rows_to_table(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original_rows_to_table(*args, **kwargs)

    monkeypatch.setattr(selected_output, "_rows_to_table", counting_rows_to_table)
    path = write_selected_observations(
        selected,
        output_root=tmp_path,
        run_id="two-date-run",
    )

    assert calls == 1
    assert sorted(item.name for item in path.iterdir()) == [
        "class_date=2026-01-15",
        "class_date=2026-01-16",
    ]
    assert read_selected_observations(path).num_rows == 30


def test_selected_observation_writer_supports_typed_empty_dataset(tmp_path: Path):
    selected = _integrated_selection().iloc[0:0]
    path = write_selected_observations(
        selected, output_root=tmp_path, run_id="empty-run"
    )

    table = read_selected_observations(path)
    assert table.schema == SELECTED_OBSERVATION_ARROW_SCHEMA
    assert table.num_rows == 0


def test_selected_observation_builder_rejects_duplicate_key_and_bad_values():
    selected = _integrated_selection()
    duplicate = pd.concat([selected.iloc[[0]], selected.iloc[[0]]])
    with pytest.raises(ValueError, match="one row per class/BSSID/bin"):
        build_selected_observations(duplicate, run_id="duplicate-run")

    missing_lineage = selected.iloc[[0]].copy()
    missing_lineage.loc[:, "source_capture"] = ""
    with pytest.raises(ValueError, match="source_capture"):
        build_selected_observations(missing_lineage, run_id="lineage-run")

    unselected = selected.iloc[[0]].copy()
    unselected.loc[:, "is_selected"] = False
    with pytest.raises(ValueError, match="unselected rows"):
        build_selected_observations(unselected, run_id="unselected-run")

    bad_cu = selected.iloc[[0]].copy()
    bad_cu.loc[:, "qbss_cu_raw"] = "256"
    with pytest.raises(ValueError, match="qbss_cu_raw"):
        build_selected_observations(bad_cu, run_id="bad-cu-run")


def test_selected_observation_writer_cleans_temporary_dataset_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    selected = _integrated_selection()

    def fail_write(*args, **kwargs):
        raise OSError("simulated write failure")

    monkeypatch.setattr(
        "selected_observation_output.pq.ParquetWriter", fail_write
    )
    with pytest.raises(OSError, match="simulated"):
        write_selected_observations(
            selected, output_root=tmp_path, run_id="failed-run"
        )

    run_directory = tmp_path / "failed-run"
    assert not (run_directory / "selected_observations").exists()
    assert list(run_directory.glob(".selected_observations.*.tmp")) == []
