"""Helpers for cleaning and processing sample schedule data."""

from dataclasses import dataclass
from datetime import datetime, timedelta
import re

import pandas as pd
from pytz.exceptions import NonExistentTimeError

from timestamp_normalization import (
    DiagnosticStatus,
    EvidenceKind,
    EvidenceScope,
    NormalizationResult,
    TimestampEvidence,
    TimestampInput,
    normalize_timestamps,
)


DAY_MAPPING = {"M": 0, "T": 1, "W": 2, "R": 3, "F": 4}
SCHEDULE_NORMALIZER_NAME = "sample_processing.schedule"
SCHEDULE_NORMALIZER_VERSION = "1"


@dataclass(frozen=True)
class ProcessedSampleTables:
    """Cleaned sample tables without requiring intermediate files."""

    sample_aps: pd.DataFrame
    sample_classrooms: pd.DataFrame
    sample_classes: pd.DataFrame


class ScheduleTimestampNormalizationError(ValueError):
    """A schedule value cannot be represented by the legacy output contract."""

    def __init__(
        self,
        column: str,
        position: int,
        result: NormalizationResult,
    ) -> None:
        self.column = column
        self.position = position
        self.result = result
        super().__init__(
            f"{column}[{position}] timestamp normalization failed: "
            f"{result.diagnostic_code}: {result.diagnostic_detail}"
        )


def normalize_schedule_timestamp_series(
    series: pd.Series,
    timezone_name: str,
) -> tuple[pd.Series, tuple[NormalizationResult, ...]]:
    """Normalize one parsed schedule column and derive its legacy timezone.

    Canonical UTC results and provenance remain transient in this slice.  The
    returned Series preserves the existing persisted timezone/unit contract.
    Neither the supplied Series nor its values are mutated.
    """

    schedule_evidence = TimestampEvidence(
        kind=EvidenceKind.SCHEDULE_TIMEZONE,
        timezone_name=timezone_name,
        source_reference="process_samples.timezone argument",
        scope=EvidenceScope.SCHEDULE,
        adapter_or_normalizer_name=SCHEDULE_NORMALIZER_NAME,
        method_version=SCHEDULE_NORMALIZER_VERSION,
    )
    requests = tuple(
        TimestampInput(
            value=value,
            evidence=_schedule_evidence_for_value(
                value,
                schedule_evidence,
            ),
        )
        for value in series.tolist()
    )
    results = normalize_timestamps(requests)
    canonical_values: list[pd.Timestamp | None] = []
    for position, result in enumerate(results):
        if result.status is DiagnosticStatus.VALID:
            canonical_values.append(result.canonical_instant)
        elif (
            result.status is DiagnosticStatus.INVALID_VALUE
            and result.diagnostic_code == DiagnosticStatus.INVALID_VALUE.value
        ):
            canonical_values.append(pd.NaT)
        elif result.status is DiagnosticStatus.AMBIGUOUS_REPEATED_LOCAL_TIME:
            canonical_values.append(pd.NaT)
        elif result.status is DiagnosticStatus.NONEXISTENT_LOCAL_TIME:
            raise NonExistentTimeError(result.original_parsed_value)
        else:
            raise ScheduleTimestampNormalizationError(
                str(series.name),
                position,
                result,
            )

    canonical_utc = pd.Series(
        pd.array(canonical_values, dtype="datetime64[ns, UTC]"),
        index=series.index,
        name=series.name,
    )
    legacy_dtype = pd.DatetimeTZDtype(
        unit=_datetime_unit(series.dtype),
        tz=timezone_name,
    )
    legacy_series = canonical_utc.dt.tz_convert(timezone_name).astype(
        legacy_dtype
    )
    return legacy_series, results


def _schedule_evidence_for_value(
    value: object,
    schedule_evidence: TimestampEvidence,
) -> tuple[TimestampEvidence, ...]:
    if pd.isna(value):
        return (schedule_evidence,)
    if isinstance(value, (pd.Timestamp, datetime)):
        if pd.Timestamp(value).tzinfo is not None:
            return ()
    return (schedule_evidence,)


def _datetime_unit(dtype) -> str:
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return getattr(dtype, "unit", "ns")
    return "ns"


def clean_cell(value):
    # Ensure the value is a string, then clean it
    value = str(value)  # Convert to string if necessary
    value = re.sub(
        r"[\xa0]+",
        " ",
        value,
    )  # Replace non-breaking spaces with regular spaces
    value = re.sub(
        r"[\x00-\x1F\x7F-\x9F]",
        "",
        value,
    )  # Remove non-printable characters
    return value.strip()  # Remove leading/trailing spaces


def parse_class_period(class_period):
    start_time_str, end_time_str = class_period.split(" - ")
    start_time_str += "m"
    end_time_str += "m"
    start_time_dt = datetime.strptime(start_time_str, "%I:%M%p").time()
    end_time_dt = datetime.strptime(end_time_str, "%I:%M%p").time()
    return start_time_dt, end_time_dt


def expand_schedule_rows(
    sample_schedule_df,
    sample_classes_df,
    schedule_days,
):
    # Iterate through sample_schedule_df
    for _, row in sample_schedule_df.iterrows():
        location = row["Location"]
        course = row["Course"]
        enrolled = int(row["Enrl"])
        class_period = row["Class Period"]
        days = row["Days"]

        # Split class_period into start and end times
        try:
            start_time_dt, end_time_dt = parse_class_period(class_period)
        except ValueError:
            print(
                f"Malformed Class Period: {class_period} for Course: "
                f"{course} in Location: {location}"
            )
            print("Check the format of the Class Period.")
            # Handle the case where class_period is malformed
            continue  # Skip if Class Period is malformed

        # Determine which days the class meets and create entries for each
        # valid day
        if days:
            for day_abbreviation in DAY_MAPPING.keys():
                if day_abbreviation in days:
                    day_index = DAY_MAPPING[day_abbreviation]

                    for date in schedule_days:
                        if date.weekday() == day_index:
                            # Combine the date and time for start_time and
                            # end_time
                            start_datetime = datetime.combine(
                                date,
                                start_time_dt,
                            )
                            end_datetime = datetime.combine(
                                date,
                                end_time_dt,
                            )

                            # Add the entry to sample_classes_df
                            sample_classes_df = pd.concat(
                                [
                                    sample_classes_df,
                                    pd.DataFrame(
                                        {
                                            "location": [location],
                                            "course": [course],
                                            "start_time": [start_datetime],
                                            "end_time": [end_datetime],
                                            "enrolled": [enrolled],
                                            "capacity": [None],
                                        }
                                    ),
                                ],
                                ignore_index=True,
                            )

    return sample_classes_df


def build_sample_tables(
    sample_aps_df: pd.DataFrame,
    sample_classrooms_df: pd.DataFrame,
    sample_schedule_df: pd.DataFrame,
    timezone: str,
    start_date_str: str,
    end_date_str: str,
    *,
    reject_malformed_periods: bool = False,
    report_dates: bool = False,
) -> ProcessedSampleTables:
    """Clean and expand the three user sample tables in memory.

    ``reject_malformed_periods`` is used by the corrected one-command runner
    so a malformed schedule cannot be silently omitted.  The legacy
    ``process_samples`` wrapper retains its established skip-and-report
    behavior.
    """

    sample_aps_df = sample_aps_df.copy(deep=True)
    sample_classrooms_df = sample_classrooms_df.copy(deep=True)
    sample_schedule_df = sample_schedule_df.copy(deep=True)

    sample_classes_df = pd.DataFrame(
        {
            "location": pd.Series(dtype="string"),
            "course": pd.Series(dtype="string"),
            "start_time": pd.Series(dtype=f"datetime64[us, {timezone}]"),
            "end_time": pd.Series(dtype=f"datetime64[us, {timezone}]"),
            "enrolled": pd.Series(dtype="int"),
            "capacity": pd.Series(dtype="int"),
        }
    )

    sample_schedule_df.columns = sample_schedule_df.columns.str.strip()
    sample_schedule_df = sample_schedule_df.apply(
        lambda col: col.map(clean_cell)
    )
    sample_schedule_df = sample_schedule_df.astype(
        {
            "Location": "string",
            "Course": "string",
            "Sec": "string",
            "LQ": "string",
            "H": "string",
            "S": "string",
            "Cr Hr": "string",
            "Sec Type": "string",
            "Class Period": "string",
            "Days": "string",
            "Instructor": "string",
            "Status": "string",
            "Assign To": "string",
            "Begin Date": "string",
            "End Date": "string",
            "sample": "string",
            "Size": "int",
            "Enrl": "int",
        }
    )

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    if reject_malformed_periods and end_date < start_date:
        raise ValueError("end date must be on or after start date")
    if report_dates:
        print("Start Date:", start_date)
        print("End Date:", end_date)
    schedule_days = [
        start_date + timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    if reject_malformed_periods:
        for position, row in sample_schedule_df.iterrows():
            try:
                parse_class_period(row["Class Period"])
            except ValueError as exc:
                raise ValueError(
                    "Malformed Class Period at schedule row "
                    f"{position}: {row['Class Period']} for Course: "
                    f"{row['Course']} in Location: {row['Location']}"
                ) from exc

    sample_classes_df = expand_schedule_rows(
        sample_schedule_df,
        sample_classes_df,
        schedule_days,
    )

    sample_classes_df.columns = sample_classes_df.columns.str.strip()
    sample_classrooms_df.columns = sample_classrooms_df.columns.str.strip()
    location_capacity_dict = dict(
        zip(
            sample_classrooms_df["Location"],
            sample_classrooms_df["Capacity"],
        )
    )
    for index, row in sample_classes_df.iterrows():
        location = row["location"]
        if location in location_capacity_dict:
            sample_classes_df.at[index, "capacity"] = (
                location_capacity_dict[location]
            )

    for column in ["start_time", "end_time"]:
        legacy_series, _results = normalize_schedule_timestamp_series(
            sample_classes_df[column],
            timezone,
        )
        sample_classes_df[column] = legacy_series

    sample_classes_df["enrolled"] = (
        sample_classes_df["enrolled"].fillna(0).astype(int)
    )
    capacity = sample_classes_df["capacity"].infer_objects(copy=False)
    sample_classes_df["capacity"] = capacity.fillna(0).astype(int)
    sample_classes_df = sample_classes_df.astype(
        {
            "location": "string",
            "course": "string",
        }
    )

    sample_classes_df.columns = sample_classes_df.columns.str.strip()
    sample_aps_df.columns = sample_aps_df.columns.str.strip()
    sample_classes_df = sample_classes_df.sort_values(
        by=["location", "start_time"]
    ).reset_index(drop=True)
    sample_classrooms_df["ap_count"] = sample_classrooms_df[
        "Location"
    ].apply(
        lambda location: (
            sample_aps_df["Building-Room"] == location
        ).sum()
    )
    return ProcessedSampleTables(
        sample_aps=sample_aps_df,
        sample_classrooms=sample_classrooms_df,
        sample_classes=sample_classes_df,
    )


def process_samples(
    sample_folder,
    timezone,
    start_date_str,
    end_date_str,
):
    # Import sample files
    sample_aps_df = pd.read_csv(f"{sample_folder}/Sample_APs.csv")
    sample_classrooms_df = pd.read_csv(
        f"{sample_folder}/Sample_classrooms.csv"
    )
    sample_schedule_df = pd.read_csv(
        f"{sample_folder}/Sample_schedule.csv"
    )

    tables = build_sample_tables(
        sample_aps_df,
        sample_classrooms_df,
        sample_schedule_df,
        timezone,
        start_date_str,
        end_date_str,
        report_dates=True,
    )

    tables.sample_classrooms.to_parquet(
        f"{sample_folder}/sample_classrooms.parquet"
    )
    tables.sample_aps.to_parquet(f"{sample_folder}/sample_aps.parquet")
    tables.sample_classes.to_parquet(
        f"{sample_folder}/sample_classes.parquet"
    )

    print(f"Sample data cleaned and saved in {sample_folder}/.")
