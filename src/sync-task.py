import requests
from io import StringIO
import pandas as pd
from dotenv import dotenv_values
import os
import math
import re
import unicodedata


config = {
    **dotenv_values(".env.shared"),  # load shared development variables
    **dotenv_values(".env"),  # load sensitive variables
    **os.environ,  # override loaded values with environment variables
}

# Define your Airtable credentials
AIRTABLE_ACCESS_TOKEN = config["AIRTABLE_ACCESS_TOKEN"]
AIRTABLE_BASE_ID = config["AIRTABLE_BASE_ID_TASK"]
AIRTABLE_TABLE_NAME = config["AIRTABLE_TABLE_NAME_TASK"]
AIRTABLE_TABLE_NAME_USERSTORY = config["AIRTABLE_TABLE_NAME_USERSTORY"]

# Airtable API URL
airtable_base_url = (
    f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
)

# CSV Export URL
CSV_EXPORT_URL = config["CSV_EXPORT_URL_TASK"]

# Headers for authorization
headers = {"Authorization": f"Bearer {AIRTABLE_ACCESS_TOKEN}"}

PIVOT_COLUMN = config["PIVOT_COLUMN_TASK"]
COLUMNS_TO_CHECK = config["COLUMNS_TO_CHECK_TASK"].split(",")


def normalize_string(s):
    if pd.isna(s):
        return ""
    s = str(s)

    # Normalize unicode (e.g., smart quotes, accented characters)
    s = unicodedata.normalize("NFKC", s)

    # Replace non-breaking spaces and normalize whitespace
    s = s.replace("\xa0", " ").replace("\u200b", "")
    s = re.sub(r"\s+", " ", s)  # collapse all whitespace to single space

    # Standardize line endings
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    return s.strip()


def retrieve_ref_record_ids(airtable_base_url):

    all_records = []
    offset = None

    while True:
        params = {}
        if offset:
            params["offset"] = offset

        # Request to Airtable API
        response = requests.get(airtable_base_url, headers=headers, params=params)
        response_data = response.json()

        # Append the records to the list
        records = response_data.get("records", [])
        all_records.extend(records)

        # Check if there's more data to fetch (pagination)
        offset = response_data.get("offset")
        if not offset:
            break

    print(f"Found {len(all_records)} records in Airtable")

    # Extract the data from the records
    data = []
    for record in all_records:
        record_data = record.get("fields", {})
        record_data["airtable_record_id"] = record.get("id")
        data.append(record_data)

    # Convert the data to a DataFrame
    df = pd.DataFrame(data)

    return df


def get_airtable_data(airtable_base_url):
    """
    Fetches data from Airtable

    Parameters
    ----------
    airtable_base_url : str
        The base URL for the Airtable API

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the data from Airtable
    """

    all_records = []
    offset = None

    while True:
        params = {}
        if offset:
            params["offset"] = offset

        # Request to Airtable API
        response = requests.get(airtable_base_url, headers=headers, params=params)
        response_data = response.json()

        # Append the records to the list
        records = response_data.get("records", [])
        all_records.extend(records)

        # Check if there's more data to fetch (pagination)
        offset = response_data.get("offset")
        if not offset:
            break

    print(f"Found {len(all_records)} records in Airtable")

    # Extract the data from the records
    data = []
    for record in all_records:
        record_data = record.get("fields", {})
        record_data["airtable_record_id"] = record.get("id")
        data.append(record_data)

    # Convert the data to a DataFrame
    df = pd.DataFrame(data)

    return df


def get_csv_export_data(csv_export_url):
    """Download and read the CSV export from Taiga

    Parameters
    ----------
    csv_export_url : str
        The URL to download the CSV export from Taiga

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the data from the CSV export
    """

    # Step 1: Download the CSV file
    response = requests.get(csv_export_url)

    # Check if the request was successful
    if response.status_code != 200:
        return f"Error: Unable to download CSV file, status code {response.status_code}"

    # Step 2: Read the CSV file
    csv_data = response.text

    # Step 3: Convert CSV data to DataFrame
    df = pd.read_csv(StringIO(csv_data))

    print(f"Found {len(df)} records in Taiga CSV export")

    return df


def synchronize_different_records(airtable_df, taiga_df):
    """Synchronize records with differences between Airtable and Taiga

    For each record, check if any columns to check are different
    between Airtable and Taiga. If they are different, update the record in Airtable with the
    values from Taiga.

    Parameters
    ----------
    airtable_df : pd.DataFrame
        The DataFrame containing the data from Airtable

    taiga_df : pd.DataFrame
        The DataFrame containing the data from the Taiga CSV export
    """

    print("Synchronizing different records...")

    # Ensure both DataFrames have the same PIVOT_COLUMN values
    common_ids = airtable_df[PIVOT_COLUMN].isin(taiga_df[PIVOT_COLUMN])
    airtable_df = airtable_df[common_ids]
    taiga_df = taiga_df[taiga_df[PIVOT_COLUMN].isin(airtable_df[PIVOT_COLUMN])]

    # Set index to PIVOT_COLUMN for both DataFrames
    airtable_df = airtable_df.set_index(PIVOT_COLUMN)
    taiga_df = taiga_df.set_index(PIVOT_COLUMN)

    # Align the DataFrames to ensure they have the same index
    airtable_df, taiga_df = airtable_df.align(taiga_df, join="inner", axis=0)

    # Identify records where columns differ
    different_records = airtable_df[
        ~airtable_df[COLUMNS_TO_CHECK].eq(taiga_df[COLUMNS_TO_CHECK]).all(axis=1)
    ]

    # Call Airtable API to update the records
    for index, row in different_records.iterrows():

        # find which column is different and print it
        for column in COLUMNS_TO_CHECK:
            if not row[column] == taiga_df.at[index, column]:

                print(f"Column '{column}' is different for record {index}")
                print(f"Airtable: |{row[column]}|")
                print(f"Taiga: |{taiga_df.at[index, column]}|")
                print("-" * 50)

                airtable_record_id = row["airtable_record_id"]

                # Prepare the updated fields from taiga_df
                updated_data = taiga_df.loc[index, COLUMNS_TO_CHECK].to_dict()

                record_url = f"{airtable_base_url}/{airtable_record_id}"
                data = {"fields": updated_data}

                # PATCH request instead of PUT to keep the existing fields not in Taiga
                response = requests.patch(record_url, headers=headers, json=data)

                if response.status_code != 200:
                    print(
                        f"Error updating record {airtable_record_id}, status code {response.status_code}"
                    )
                    print(response.json())
                    print(response.text)
                    print()
                else:
                    print(f"Successfully updated record {airtable_record_id}")

                break


def synchronize_missing_records(airtable_df, taiga_df):
    """Synchronize records missing in Airtable

    For each record in Taiga that is missing in Airtable, create a new record in Airtable.

    Parameters
    ----------
    airtable_df : pd.DataFrame
        The DataFrame containing the data from Airtable

    taiga_df : pd.DataFrame
        The DataFrame containing the data from the Taiga CSV export
    """

    print("Synchronizing missing records...")

    # Find records in Taiga that are missing in Airtable
    missing_records = taiga_df[~taiga_df[PIVOT_COLUMN].isin(airtable_df[PIVOT_COLUMN])]

    print(f"Found {len(missing_records)} records missing in Airtable")

    # Call Airtable API to create the missing records
    for _, row in missing_records.iterrows():
        record_url = airtable_base_url

        # drop all the data for which we don't have a column in Airtable
        data = row[COLUMNS_TO_CHECK].to_dict()
        data[PIVOT_COLUMN] = row[PIVOT_COLUMN]

        data = {
            "records": [
                {
                    "fields": data,
                }
            ]
        }

        print(f"Creating new record with data: {data}")

        response = requests.post(record_url, headers=headers, json=data)

        if response.status_code != 200:
            print(f"Error creating record, status code {response.status_code}")
            print(response.json())
        else:
            print(f"Successfully created record")


def synchronize_deleted_records(airtable_df, taiga_df):
    """Synchronize records deleted in Taiga

    For each record in Airtable that is missing in Taiga, print the record

    Parameters
    ----------
    airtable_df : pd.DataFrame
        The DataFrame containing the data from Airtable

    taiga_df : pd.DataFrame
        The DataFrame containing the data from the Taiga CSV export
    """

    print("Synchronizing deleted records...")

    # Find records in Airtable that are missing in Taiga
    deleted_records = airtable_df[
        ~airtable_df[PIVOT_COLUMN].isin(taiga_df[PIVOT_COLUMN])
    ]

    print(f"Found {len(deleted_records)} records missing in Taiga")

    if not deleted_records.empty:
        print(deleted_records.head())


def process_userstory_record_id(taiga_df, airtable_userstory_df):
    """Process user story record ID

    For each record in Taiga, find the corresponding user story in Airtable and update the record.

    Parameters
    ----------
    taiga_df : pd.DataFrame
        The DataFrame containing the data from the Taiga CSV export

    airtable_userstory_df : pd.DataFrame
        The DataFrame containing the data from Airtable user stories
    """

    # for each record in taiga_df, find the corresponding taiga_df.user_story in airtable_userstory_df.airtable_record_id with "ref" pivot column
    # update the taiga_df.user_story to the airtable_userstory_df.airtable_record_id
    taiga_df["user_story"] = taiga_df["user_story"].apply(
        lambda x: (
            [
                airtable_userstory_df[airtable_userstory_df["ref"] == x][
                    "airtable_record_id"
                ].values[0]
            ]
            if not pd.isna(x)
            else None
        )
    )

    return taiga_df


# Main process
if __name__ == "__main__":

    print("Step 1: Loading current data from Airtable...")

    airtable_df = get_airtable_data(airtable_base_url)

    airtable_df = airtable_df.where(pd.notnull(airtable_df), None)
    # tags to [] if None
    airtable_df["tags"] = airtable_df["tags"].apply(lambda x: x if x else [])
    print()

    airtable_userstory_df = get_airtable_data(
        f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME_USERSTORY}"
    )

    print("Step 2: Loading current data from taiga CSV export...")

    taiga_df = get_csv_export_data(CSV_EXPORT_URL)
    taiga_df["created_date"] = (
        pd.to_datetime(taiga_df["created_date"])
        .dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        .str[:-3]
        + "Z"
    )
    taiga_df["modified_date"] = (
        pd.to_datetime(taiga_df["modified_date"])
        .dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        .str[:-3]
        + "Z"
    )
    taiga_df["finished_date"] = (
        pd.to_datetime(taiga_df["finished_date"])
        .dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        .str[:-3]
        + "Z"
    )
    taiga_df = taiga_df.where(pd.notnull(taiga_df), None)
    taiga_df["tags"] = taiga_df["tags"].apply(
        lambda x: [tag.strip() for tag in str(x).split(",")] if pd.notna(x) else []
    )
    taiga_df = process_userstory_record_id(taiga_df, airtable_userstory_df)

    print()

    print("Step 3: Synchronize data between Airtable and Taiga...")

    synchronize_different_records(airtable_df, taiga_df)
    print()

    synchronize_missing_records(airtable_df, taiga_df)
    print()

    synchronize_deleted_records(airtable_df, taiga_df)
    print()

    print("Synchronization completed!")
