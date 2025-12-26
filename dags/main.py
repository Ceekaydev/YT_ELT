# import os
# import sys

from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

# sys.path.insert(0, os.path.join(os.environ['AIRFLOW_HOME'], 'dags'))
# sys.path.insert(0, os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))
from api.video_start import (get_playlist_id, get_video_ids, extract_video_data, save_to_json)

from datawarehouse.dwh import staging_table, core_table

from dataquality.soda import yt_elt_data_quality

# define local timezone
local_tz =  pendulum.timezone("Africa/Lagos")
start = pendulum.datetime(2024, 12, 1, tz=local_tz)

# define variables
staging_schema = 'staging'
core_schema = 'core'

# default Args
default_args = {
    "owner": "ceekaywrld",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "okeke@email.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}

# DAG 1: produce_json
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule= '0 14 * * *',
    catchup=False
) as dag_produce:
    
    # DEFINE TASK
    playlist_id_task = get_playlist_id()
    video_ids_task = get_video_ids(playlist_id_task)
    extract_data_task = extract_video_data(video_ids_task)
    save_to_json_format_task = save_to_json(extract_data_task)

    def push_conf_to_trigger(**context):
        # Retrieve the Task Instance (ti) object from the context
        ti = context['ti'] 
        
        # Pull the value pushed to XCom by the specified task ID
        saved = ti.xcom_pull(task_ids='save_to_json') 
        
        # Check if the XCom value exists, raise an error if not
        if not saved:
            raise ValueError("file_path XCom is missing") 
            
        # The return value is automatically pushed to XCom for this task
        return saved 

    # Define the PythonOperator within an Airflow DAG context
    push_conf_task = PythonOperator(
        task_id='prepare_trigger_conf',
        python_callable=push_conf_to_trigger,
        # Ensure this operator is part of a valid Airflow DAG definition
        # dag=my_dag, 
    )


    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_Update_db",
        trigger_dag_id="Update_db",
        conf="{{ ti.xcom_pull(task_ids='prepare_trigger_conf') | tojson }}",
    )

    # define dependencies
    playlist_id_task >> video_ids_task >> extract_data_task >> save_to_json_format_task >> push_conf_task >> trigger_update_db

# DAG 2: update db
with DAG(
    dag_id="Update_db",
    default_args=default_args,
    description="DAG to process JSON File and insert data into both staging and core schema",
    schedule=None,
    catchup=False
) as dag_update:
    
    
    # DEFINE TASK


    update_staging_table = staging_table()
    update_core_table = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # define dependencies
    update_staging_table >> update_core_table >> trigger_data_quality


# DAG 3: data quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to perform data quality checks using Soda on both dch schemas",
    schedule=None,
    catchup=False
) as dag_quality:
       
    # DEFINE TASK

    data_validate_staging = yt_elt_data_quality(staging_schema)
    data_validate_core = yt_elt_data_quality(core_schema)

    # define dependencies
    data_validate_staging >> data_validate_core  