"""Pure helpers for cleaning and expanding sample schedule data."""

from datetime import datetime
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
