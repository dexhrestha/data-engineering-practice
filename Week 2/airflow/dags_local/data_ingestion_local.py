import os

from datetime import datetime

from ingest_data import ingest_data

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
local_file_name = "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"

MAIN_URL = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL = MAIN_URL + '/'+ local_file_name
local_file_path = AIRFLOW_HOME+'/'+local_file_name
TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}"

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


default_args = {
    "owner": "airflow",
    "start_date":datetime(2019,1,1),
    "depends_on_past": False,
    "retries": 1,
}

local_workflow =DAG(
    dag_id = "local_ingestion_dag",
    schedule_interval='0 6 2 * *', #Ref: http://crontab.guru Every 2nd day of month at 6 am
    default_args=default_args,
) 

with local_workflow:

    echo_task =BashOperator(
        task_id='echotask',
        bash_command='echo "hello world"'
    )
    
    wget_task =  BashOperator(
        task_id='fetch_data_from_wget',
        # bash_command=f' wget {url} -O {AIRFLOW_HOME}/output.csv'
        # bash_command=f' curl -sSL {URL} > {local_file_path}'
        bash_command=f'ls -lh'
    )
    
    ingest_task =  PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
        op_kwargs={'user':PG_USER,
            'password':PG_PASSWORD,
            'host':PG_HOST,
            'port':PG_PORT,
            'db':PG_DATABASE,
            'table_name':TABLE_NAME_TEMPLATE,
            'csv_file':local_file_path},
    )
    echo_task 
    
