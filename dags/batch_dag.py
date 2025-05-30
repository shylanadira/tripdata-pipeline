from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from batch_pipeline.merge_tripdata import merge_tripdata
from batch_pipeline.extractor import extract_to_gcs
from batch_pipeline.loader import load_to_bigquery

# Default args
default_args = {
    'owner': 'shyla',
    'start_date': datetime(2024, 5, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='capstone3_full_batch_dbt_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['capstone3', 'batch', 'gcs', 'bigquery', 'dbt']
) as dag:

    # Step 1: Merge raw CSV + JSON files into one CSV
    merge_files = PythonOperator(
        task_id='merge_tripdata_files',
        python_callable=merge_tripdata
    )

    # Step 2: Upload merged trip data + lookup files to GCS
    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=extract_to_gcs
    )

    # Step 3: Load data from GCS into BigQuery
    load_bq = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )

    # Step 4: Run DBT debug (optional)
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt_capstone && dbt debug'
    )

    # Step 5: Run DBT models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_capstone && dbt run'
    )

    # DAG Task Dependency Chain
    merge_files >> upload_to_gcs >> load_bq >> dbt_debug >> dbt_run
