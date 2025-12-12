# import os
# import sys

from airflow import DAG
import pendulum
from datetime import datetime, timedelta

# sys.path.insert(0, os.path.join(os.environ['AIRFLOW_HOME'], 'dags'))
# sys.path.insert(0, os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))
from api.video_start import get_playlist_id, get_video_ids, extract_video_data, save_to_json

# define local timezone
local_tz =  pendulum.timezone("Africa/Lagos")
start = pendulum.datetime(2024, 12, 1, tz=local_tz)

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

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule= '0 14 * * *',
    catchup=False
) as dag:
    
    # DEFINE TASK
    playlist_id_task = get_playlist_id()
    video_ids_task = get_video_ids(playlist_id_task)
    extract_data_task = extract_video_data(video_ids_task)
    save_to_json_format_task = save_to_json(extract_data_task)

    # define dependencies
    playlist_id_task >> video_ids_task >> extract_data_task >> save_to_json_format_task