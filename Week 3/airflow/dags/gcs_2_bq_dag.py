import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATASET = "tripdata"
TAXI_TYPE = {'yellow': 'tpep_pickup_datetime', 'fhv': 'pickup_datetime','green':'lpep_pickup_datetime'}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"
YEAR = "{{ execution_date.strftime(\'%Y\') }}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@yearly",
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 12, 12),
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    for ttype, ds_col in TAXI_TYPE.items():
        gcs_2_gcs_task = GCSToGCSOperator(
            task_id = f"gcs_2_gcs_task_{ttype}",
            source_bucket=BUCKET,
            source_object=f'raw/{ttype}_{DATASET}/{YEAR}/*.parquet',
            destination_bucket=BUCKET,
            destination_object=f"{ttype}_{DATASET}/{YEAR}/",
            move_object=True,

        )

        gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
        task_id = f"gcs_2_bq_ext_task_{ttype}",
        table_resource={
            "tableReference":{
                "projectId":PROJECT_ID,
                "datasetId":BIGQUERY_DATASET,
                "tableId":f"external_{ttype}_{DATASET}_{YEAR}"
            },
             
            "externalDataConfiguration":{
                "autodetect":True,
                "sourceFormat":"PARQUET",
                "sourceUris":[f"gs://{BUCKET}/{ttype}_{DATASET}/{YEAR}/*"],       
            }
        })

        CREATE_PART_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{ttype}_{DATASET}_{YEAR} \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.external_{ttype}_{DATASET}_{YEAR}"
        )

        bg_ext_2_partition = BigQueryInsertJobOperator(
            task_id=f"bg_ext_2_partition_{ttype}",
            configuration={
                "query":{
                    "query": CREATE_PART_TBL_QUERY,
                    "useLegacySql": False
                }
            }
        )

        gcs_2_gcs_task >> gcs_2_bq_ext_task >> bg_ext_2_partition 