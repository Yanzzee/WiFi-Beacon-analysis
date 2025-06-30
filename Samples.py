###
# Input sample information for classrooms, APs and class periods
# See example files for input format
# clean up the data and export .parquet files that can be easily imported for analysis later
# creates sample_classes_df and saves it for analysis - includes every class session
###

import pandas as pd
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="config.env")

TIMEZONE = os.getenv("TIMEZONE")
SAMPLE_FOLDER = os.getenv("SAMPLE_FOLDER")

if TIMEZONE is None:
    raise ValueError("TIMEZONE is not set in config.env")
if SAMPLE_FOLDER is None:
    raise ValueError("SAMPLE_FOLDER is not set in config.env")

if not os.path.exists(SAMPLE_FOLDER):
    os.makedirs(SAMPLE_FOLDER)

# Define a function to clean a single cell
def clean_cell(value):
    # Ensure the value is a string, then clean it
    value = str(value)  # Convert to string if necessary
    value = re.sub(r'[\xa0]+', ' ', value)  # Replace non-breaking spaces with regular spaces
    value = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', value)  # Remove non-printable characters
    return value.strip()  # Remove leading/trailing spaces


#Import sample files
sample_aps_df = pd.read_csv(f"{SAMPLE_FOLDER}/Sample_APs.csv")
sample_classrooms_df = pd.read_csv(f"{SAMPLE_FOLDER}/Sample_classrooms.csv")
sample_schedule_df = pd.read_csv(f"{SAMPLE_FOLDER}/Sample_schedule.csv")


#build sample_df with each class period for the 2 weeks as a record
# Create an empty DataFrame with specified columns and data types
sample_classes_df = pd.DataFrame({
    "location": pd.Series(dtype="string"),
    "course": pd.Series(dtype="string"),
    "start_time": pd.Series(dtype=f"datetime64[us, {TIMEZONE}]"),
    "end_time": pd.Series(dtype=f"datetime64[us, {TIMEZONE}]"),
    "enrolled": pd.Series(dtype="int"),
    "capacity": pd.Series(dtype="int")
})



# Strip extra spaces from column names
sample_schedule_df.columns = sample_schedule_df.columns.str.strip()

# Verify the cleaned column names
#print(sample_schedule_df.columns)
#print(sample_classes_df.dtypes)


# Change the data types of the columns
sample_schedule_df = sample_schedule_df.astype({
    'Location': 'string',
    'Course': 'string',
    'Sec': 'string',
    'LQ': 'string',
    'H': 'string',
    'S': 'string',
    'Cr Hr': 'string',
    'Sec Type': 'string',
    'Class Period': 'string',
    'Days': 'string',
    'Instructor': 'string',
    'Status': 'string',
    'Assign To': 'string',
    'Begin Date': 'string',
    'End Date': 'string',
    'sample': 'string',
    'Size': 'int',   # Change 'Size' to integer
    'Enrl': 'int'    # Change 'Enrl' to integer
})

# Check the updated data types
#print(sample_schedule_df.dtypes)


# Apply the cleaning function to every column in the DataFrame
sample_schedule_df = sample_schedule_df.apply(lambda col: col.map(clean_cell))

# Check the cleaned DataFrame
#print(sample_schedule_df.head())


# verify clean data
# Access the first record of the DataFrame
first_record = sample_schedule_df.iloc[10]

# Print the first record, including hidden characters
#for column, value in first_record.items():
#    print(f"{column}: {repr(value)}")



# build the sample_classes_df
# Define the two-week schedule
# This is the sample dates
#start_date = datetime(2024, 11, 4) # update to use env file
#end_date = datetime(2024, 11, 18) # update to use env file
load_dotenv(dotenv_path="config.env")

start_date_str = os.getenv("START_DATE")
end_date_str = os.getenv("END_DATE")

# # Convert strings to datetime objects
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

print("Start Date:", start_date)
print("End Date:", end_date)
schedule_days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# Mapping of day abbreviations to Python weekday indices
day_mapping = {"M": 0, "T": 1, "W": 2, "R": 3, "F": 4}

# Iterate through sample_schedule_df
for _, row in sample_schedule_df.iterrows():
    location = row["Location"]
    course = row["Course"]
    enrolled = int(row["Enrl"])
    class_period = row["Class Period"]
    days = row["Days"]

    # Split class_period into start and end times
    try:
        start_time_str, end_time_str = class_period.split(" - ")
        start_time_str += "m"
        end_time_str += "m"
        start_time_dt = datetime.strptime(start_time_str, "%I:%M%p").time()
        end_time_dt = datetime.strptime(end_time_str, "%I:%M%p").time()
    except ValueError:
        print(f"Malformed Class Period: {class_period} for Course: {course} in Location: {location}")
        print("Check the format of the Class Period.")
        # Handle the case where class_period is malformed
        continue  # Skip if Class Period is malformed

    # Determine which days the class meets and create entries for each valid day
    if days:
        for day_abbreviation in day_mapping.keys():
            if day_abbreviation in days:
                day_index = day_mapping[day_abbreviation]

                for date in schedule_days:
                    if date.weekday() == day_index:
                        # Combine the date and time for start_time and end_time
                        start_datetime = datetime.combine(date, start_time_dt)
                        end_datetime = datetime.combine(date, end_time_dt)

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
                                        "capacity": [None],  # Capacity is not provided in sample_schedule_df
                                    }
                                ),
                            ],
                            ignore_index=True,
                        )

# Print the result
#print(sample_classes_df.head())


#find room capacity values and add to sample_classes_df
# Ensure both DataFrames are stripped of any leading/trailing spaces in their column names
sample_classes_df.columns = sample_classes_df.columns.str.strip()
sample_classrooms_df.columns = sample_classrooms_df.columns.str.strip()

# Create a dictionary from the sample_classrooms_df for fast look-up of 'Capacity' based on 'Location'
location_capacity_dict = dict(zip(sample_classrooms_df['Location'], sample_classrooms_df['Capacity']))

# Iterate through the rows of sample_classes_df and update the 'capacity' column based on the 'Location'
for index, row in sample_classes_df.iterrows():
    location = row['location']  # Assuming 'location' column is lowercase in sample_classes_df
    if location in location_capacity_dict:
        sample_classes_df.at[index, 'capacity'] = location_capacity_dict[location]

# Now the 'capacity' column in sample_classes_df is filled with the corresponding values from sample_classrooms_df
#print(sample_classes_df[['location', 'capacity']].head())


# Ensure start_time and end_time are datetime objects
sample_classes_df['start_time'] = pd.to_datetime(sample_classes_df['start_time'], errors='coerce')
sample_classes_df['end_time'] = pd.to_datetime(sample_classes_df['end_time'], errors='coerce')
# Convert the start_time and end_time columns to 'Etc/GMT+7' timezone



sample_classes_df["start_time"] = sample_classes_df["start_time"].dt.tz_localize(TIMEZONE, ambiguous="NaT")
sample_classes_df["end_time"] = sample_classes_df["end_time"].dt.tz_localize(TIMEZONE, ambiguous="NaT")


#change datatypes
# Fill missing values in 'enrolled' and 'capacity' with 0
sample_classes_df['enrolled'] = sample_classes_df['enrolled'].fillna(0).astype(int)
sample_classes_df['capacity'] = sample_classes_df['capacity'].fillna(0).astype(int)

# Change the data types of other columns
sample_classes_df = sample_classes_df.astype({
    'location': 'string',
    'course': 'string'
})

# Print the updated data types to verify
#print(sample_classes_df.dtypes)


# Clean up column names for consistency (strip spaces)
sample_classes_df.columns = sample_classes_df.columns.str.strip()
sample_aps_df.columns = sample_aps_df.columns.str.strip()

# sort by class time
sample_classes_df = sample_classes_df.sort_values(by=['location', 'start_time']).reset_index(drop=True)
#print(sample_classes_df.head())

# Create the new 'ap_count' column in sample_classes_df
sample_classrooms_df['ap_count'] = sample_classrooms_df['Location'].apply(
    lambda location: (sample_aps_df['Building-Room'] == location).sum()
)


# write sample dataframes to files so they don't have to be cleaned again
sample_classrooms_df.to_parquet(f"{SAMPLE_FOLDER}/sample_classrooms.parquet")
sample_aps_df.to_parquet(f"{SAMPLE_FOLDER}/sample_aps.parquet")
sample_classes_df.to_parquet(f"{SAMPLE_FOLDER}/sample_classes.parquet")

print(f"Sample data cleaned and saved in {SAMPLE_FOLDER}/.")