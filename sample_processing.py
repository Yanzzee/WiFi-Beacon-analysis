"""Helpers for cleaning and processing sample schedule data."""

from datetime import datetime, timedelta
import re

import pandas as pd


DAY_MAPPING = {"M": 0, "T": 1, "W": 2, "R": 3, "F": 4}


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

    # Build sample_classes_df with each class period as a record.
    sample_classes_df = pd.DataFrame(
        {
            "location": pd.Series(dtype="string"),
            "course": pd.Series(dtype="string"),
            "start_time": pd.Series(
                dtype=f"datetime64[us, {timezone}]"
            ),
            "end_time": pd.Series(dtype=f"datetime64[us, {timezone}]"),
            "enrolled": pd.Series(dtype="int"),
            "capacity": pd.Series(dtype="int"),
        }
    )

    # Strip extra spaces from column names.
    sample_schedule_df.columns = sample_schedule_df.columns.str.strip()

    # Clean every column before strict dtype conversion.
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

    print("Start Date:", start_date)
    print("End Date:", end_date)
    schedule_days = [
        start_date + timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    sample_classes_df = expand_schedule_rows(
        sample_schedule_df,
        sample_classes_df,
        schedule_days,
    )

    # Find room capacity values and add them to sample_classes_df.
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

    # Ensure start_time and end_time are datetime objects.
    sample_classes_df["start_time"] = pd.to_datetime(
        sample_classes_df["start_time"],
        errors="coerce",
    )
    sample_classes_df["end_time"] = pd.to_datetime(
        sample_classes_df["end_time"],
        errors="coerce",
    )

    # Convert start_time and end_time to the configured timezone.
    for column in ["start_time", "end_time"]:
        series = sample_classes_df[column]
        if getattr(series.dt, "tz", None) is None:
            sample_classes_df[column] = series.dt.tz_localize(
                timezone,
                ambiguous="NaT",
            )
        else:
            sample_classes_df[column] = series.dt.tz_convert(timezone)

    sample_classes_df["enrolled"] = (
        sample_classes_df["enrolled"].fillna(0).astype(int)
    )
    sample_classes_df["capacity"] = (
        sample_classes_df["capacity"].fillna(0).astype(int)
    )
    sample_classes_df = sample_classes_df.astype(
        {
            "location": "string",
            "course": "string",
        }
    )

    # Clean up column names for consistency.
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

    sample_classrooms_df.to_parquet(
        f"{sample_folder}/sample_classrooms.parquet"
    )
    sample_aps_df.to_parquet(f"{sample_folder}/sample_aps.parquet")
    sample_classes_df.to_parquet(
        f"{sample_folder}/sample_classes.parquet"
    )

    print(f"Sample data cleaned and saved in {sample_folder}/.")
