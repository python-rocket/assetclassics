import pandas as pd
from google.cloud import bigquery
import numpy as np
import json
import logging

logging.basicConfig(
    level=logging.DEBUG,  # Set the log level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log format
    handlers=[
        logging.FileHandler("app.log"),  # Log to a file
        logging.StreamHandler()  # Also log to the console
    ]
)
logger = logging.getLogger(__name__)


def clean_and_prepare_df(df):
    """Perform cleaning and preparation of the DataFrame."""
    # Example: Replace NaN with None (suitable for BigQuery)

    # Add missing keys
    df = add_missing_keys_for_result_schema(df)

    # Reorder columns
    df = reorder_columns(df)

    # Iterate through the list and convert necessary fields
    df = df.astype(str)
    df = df.replace(np.nan, None)
    df = df.replace("None", None)
    df = df.replace("nan", None)

    return df.where(pd.notnull(df), None)


def upload_to_bigquery(df, project, table_id):
    # Convert all columns in the DataFrame to strings
    logger.info("RESULT DF")
    logger.info(df)
    client = bigquery.Client(project=project)
    # set all columns to string
    schema = [bigquery.SchemaField(column, 'STRING') for column in df.columns]
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    response = {
        "job_id": job.job_id,
        "status": job.state,
        "errors": job.errors,
        "output_rows": job.output_rows
    }
    logger.info(response)


def get_existing_record_ids(project, dataset_id, table_id):
    column = "record_id"
    client = bigquery.Client(project=project)

    # Build the fully-qualified table identifier
    full_table_id = f"{project}.{dataset_id}.{table_id}"

    # Check if the table exists
    try:
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        client.get_table(table_ref)
    except Exception as e:
        # If the table doesn't exist, return an empty list
        print(f"Table {full_table_id} does not exist. Returning an empty list.")
        return []

    # If the table exists, run the query
    query = f"SELECT DISTINCT {column} FROM `{full_table_id}`"

    # Execute the query and return the result as a DataFrame
    df = client.query(query).to_dataframe()
    result_list = df[column].tolist()
    return result_list



def add_missing_keys_for_result_schema(df):
    with open("src/result_columns.json", "r") as f:
        result_schema = json.loads(f.read())
    # Add missing columns to DataFrame
    for key in result_schema.keys():
        if key not in df.columns:
            print("ADDING KEY", key)
            df[key] = None

    return df


def reorder_columns(df):
    with open("src/result_columns.json", "r") as f:
        result_schema = json.loads(f.read())

    # Reorder DataFrame columns according to the result_schema order
    ordered_columns = [key for key in result_schema.keys() if key in df.columns]

    # Ensure that all columns in result_schema are included in the DataFrame
    df = df[ordered_columns]

    return df



def get_empty_columns_from_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    # Build the fully-qualified table identifier
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Read the table into a DataFrame
    query = f"SELECT * FROM `{full_table_id}`"
    df = client.query(query).to_dataframe()

    # Check for columns where all values are empty (None, NaN, etc.)
    empty_columns = []
    for column in df.columns:
        if df[column].isna().all():  # Check if all values in the column are NaN/None
            empty_columns.append(column)

    # Print the columns that are completely empty
    if empty_columns:
        print("Columns where all values are empty:")
        for col in empty_columns:
            print(col)
    else:
        print("No columns with all empty values were found.")


def read_from_bigquery(project_id, dataset_id, table_id, columns=['*'], where_condition=''):
    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Read the table into a DataFrame
    columns_str = ",".join(columns)
    query = f"SELECT {columns_str} FROM `{full_table_id}`{where_condition}"
    df = client.query(query).to_dataframe()
    return df


def upload_unique_to_bigquery(csv_path, bigquery_project, bigquery_dataset_id, bigquery_table_id):
    existing_record_ids = get_existing_record_ids(bigquery_project, bigquery_dataset_id, bigquery_table_id)
    df = pd.read_csv(csv_path)
    num_rows_before = df.shape[0]
    df = df[~df['record_id'].isin(existing_record_ids)]
    num_rows_after = df.shape[0]
    logger.info(f"Removed this number of duplicate record ids: {num_rows_after - num_rows_before}")
    df = clean_and_prepare_df(df)
    bigquery_table = f"{bigquery_dataset_id}.{bigquery_table_id}"
    upload_to_bigquery(df, bigquery_project, bigquery_table)


def upload_to_bigquery_from_csv(csv_path, bigquery_project, bigquery_dataset_id, bigquery_table_id):
    try:
        df = pd.read_csv(csv_path)
    except pd.errors.EmptyDataError:
        logger.info("The CSV file is empty, not writing to db.")
        return

    df = clean_and_prepare_df(df)
    bigquery_table = f"{bigquery_dataset_id}.{bigquery_table_id}"
    upload_to_bigquery(df, bigquery_project, bigquery_table)
