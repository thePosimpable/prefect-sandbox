import os
from google.cloud import storage
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import json
from datetime import datetime

dotenv_path = Path('../.env')
load_dotenv(dotenv_path = dotenv_path)

TEMP_FLOW_RUN = datetime.now()

CSV_FILE_NAME = OS.getenv('CSV_FILE_NAME')

GCP_PROJECT_NAME = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET')

BQ_METADATA = {
    'landing': {
        'dataset': os.getenv('BQ_LANDING_DATASET'),
        'table': os.getenv('BQ_LANDING_TABLE')
    }
}

def get_json_data():
    df = pd.read_csv(f"{CSV_FILE_NAME}.csv")
    df = df.loc[:, ~df.columns.isin(['Unnamed: 0'])]
    data = df.to_json(orient = 'records', lines=True)
    df.to_json('test.json', orient = 'records', lines=True)

    return data

def upload_json_to_gcs(json_data: str, bucket_name, destination_blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"{destination_blob_name} uploaded to {bucket_name}.")

def load_from_gcs_to_bq():
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField('drid', 'INTEGER'),
            bigquery.SchemaField('drno', 'INTEGER'),
            bigquery.SchemaField('drdate', 'DATE'),
            bigquery.SchemaField('dramount', 'FLOAT'),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True
    )

    uri = f"gs://{GCS_BUCKET_NAME}/{TEMP_FLOW_RUN}/{CSV_FILE_NAME}.json"

    table_id = f"{GCP_PROJECT_ID}.{BQ_LANDING_DATASET}.{BQ_LANDING_TABLE}"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="asia-southeast1",
        job_config=job_config,
    )

    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def main():
    json_data = get_json_data()
    upload_json_to_gcs(json_data, GCS_BUCKET_NAME, f"{TEMP_FLOW_RUN}/{CSV_FILE_NAME}.json")
    load_from_gcs_to_bq()

if __name__ == "__main__":
    main()