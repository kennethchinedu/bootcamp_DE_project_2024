from airflow import DAG
from datetime import timedelta, datetime
import json, requests
from airflow.operators.python import PythonOperator 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
# from airflow.providers.snowflake.transfers.s3_to_snowflake.S3ToSnowflakeOperator import S3ToSnowflakeOperator
from airflow.hooks.S3_hook import S3Hook
from python_script import extract_titles, upload_titles_to_s3
from snowflake_squeries import create_table_query, load_title_query, create_staging_query, create_stream_query, load_stream_query



default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 12),
    'email': ['anamsken60@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'Netflix_dag',
    default_args=default_args,
    schedule_interval = '@daily',
    catchup=False ) as dag:

    create_raw_table_tsk = SnowflakeOperator(
        task_id = 'create_raw_table_tsk',
        sql = create_table_query,
        snowflake_conn_id='snowflake_con'
    )

    create_stg_table_tsk = SnowflakeOperator(
        task_id = 'create_staging_query',
        sql = create_staging_query,
        snowflake_conn_id='snowflake_con'
    )

    create_stream_task = SnowflakeOperator(
        task_id = 'create_stream_task',
        sql = create_stream_query,
        snowflake_conn_id='snowflake_con'
    )

    extract_movies_task = PythonOperator(
        task_id="extract_movies_task",
        python_callable= extract_titles,
    )

    upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_titles_to_s3,
    provide_context=True,  # This allows you to access task instance information like XCom
    )
    
    load_title_snowflake = SnowflakeOperator(
        task_id = 'load_title_snowflake',
        sql = load_title_query,
        snowflake_conn_id='snowflake_con'
    )

    load_stream_tsk = SnowflakeOperator(
        task_id = 'load_stream_tsk',
        sql = load_stream_query,
        snowflake_conn_id='snowflake_con'
    )



    [create_raw_table_tsk, create_stg_table_tsk, create_stream_task ] >> extract_movies_task >> upload_to_s3_task >> load_title_snowflake  >> load_stream_tsk