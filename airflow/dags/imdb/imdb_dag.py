import inspect
import logging
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'imdb_data_pipeline',
    default_args=default_args,
    description='DAG to process data from IMDB: Landing -> Raw -> Processed -> Curated',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    landing_to_raw = GlueJobOperator(
        task_id='imdb_landing_to_raw',
        job_name='imdb_landing_to_raw',
        script_location='/opt/airflow/dags/imdb/glue_jobs/glue_job_imdb_landing_to_raw.py',
        job_desc='ETL Job to convert TSV files to Parquet',
        s3_bucket='project-cine-glue-bucket',
        concurrent_run_limit=1,
        iam_role_name= 'AwsGlueRole',
        region_name='us-east-1',
        create_job_kwargs={
            'GlueVersion': '2.0',
            'WorkerType': 'Standard',
            'NumberOfWorkers': 2,
        },
        replace_script_file=True,
        update_config=True,
        wait_for_completion=True
    )

    raw_to_processed = GlueJobOperator(
        task_id='imdb_raw_to_processed',
        job_name='imdb_raw_to_processed',
        script_location='/opt/airflow/dags/imdb/glue_jobs/glue_job_imdb_raw_to_processed.py',
        job_desc='ETL Job for IMDB data',
        s3_bucket='project-cine-glue-bucket',
        concurrent_run_limit=1,
        iam_role_name= 'AwsGlueRole',
        region_name='us-east-1',
        create_job_kwargs={
            'GlueVersion': '2.0',
            'WorkerType': 'Standard',
            'NumberOfWorkers': 2,
        },
        replace_script_file=True,
        update_config=True,
        wait_for_completion=True
    )

    processed_to_curated = GlueJobOperator(
        task_id='imdb_processed_to_curated',
        job_name='imdb_processed_to_curated',
        script_location='/opt/airflow/dags/imdb/glue_jobs/glue_job_imdb_processed_to_curated.py',
        job_desc='ETL Job for IMDB data',
        s3_bucket='project-cine-glue-bucket',
        concurrent_run_limit=1,
        iam_role_name= 'AwsGlueRole',
        region_name='us-east-1',
        create_job_kwargs={
            'GlueVersion': '2.0',
            'WorkerType': 'Standard',
            'NumberOfWorkers': 2,
        },
        replace_script_file=True,
        update_config=True,
        wait_for_completion=True
    )

    landing_to_raw >> raw_to_processed >> processed_to_curated
