import pandas as pd
from google.cloud import bigquery
import numpy as np

def clean_and_prepare_df(df):
    """Perform cleaning and preparation of the DataFrame."""
    # Example: Replace NaN with None (suitable for BigQuery)

    # Iterate through the list and convert necessary fields

    df = df.astype(str)
    df = df.replace(np.nan, None)
    df = df.replace("None", None)
    df = df.replace("nan", None)
    return df.where(pd.notnull(df), None)


def upload_to_bigquery(df, project, table_id):
    client = bigquery.Client(project=project)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    response = {
        "job_id": job.job_id,
        "status": job.state,
        "errors": job.errors,
        "output_rows": job.output_rows
    }
    print(response)