import imp
import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import Table

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    chunksize = 100000
    csv_stream = pd.read_csv(src_file,chunksize=chunksize)

    for i, chunk in enumerate(csv_stream):
        print("Writing chunk", i)
        if i == 0:
            # Guess the schema of the CSV file from the first chunk
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            # Open a Parquet file for writing
            parquet_writer = pq.ParquetWriter(src_file.replace('.csv', '.parquet'), parquet_schema, compression='snappy')
        # Write CSV chunk to the parquet file
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)
    parquet_writer.close()


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="nyc_taxi_data_ingestion_gcs_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    remove_dataset_file_task = BashOperator(
        task_id="remove_dataset_file_task",
        bash_command=f"rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_dataset_file_task
