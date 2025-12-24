from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import parse_duration, transform_data
from datawarehouse.data_loading import load_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"
HF_CALL_DELAY = 0.25
MAX_HF_CALLS = 1000
hf_call_count = 0


@task
def staging_table(**context):

    schema = "staging"

    ds = context["ds"]  # Airflow execution date

    file_path = f"/opt/airflow/data/Yt_data_{ds}.json"

    conn, cur = None, None

    try:

        conn, cur = get_conn_cursor()


        YT_data = load_data(file_path)

        create_schema(schema)
        create_table(schema)

        table_ids = set(get_video_ids(cur, schema, table))

        for row in YT_data:

            if len(table_ids) == 0 or row['video_id'] not in table_ids:

                insert_rows(cur, conn, schema, row)

            else:
                # Update existing row
                if row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                
                else:
                    insert_rows(cur, conn, schema, row)

        id_in_json  = {row['video_id'] for row in YT_data}

        ids_to_delete = set(table_ids) - id_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"Staging table '{schema}.{table}' updated successfully.")

    except Exception as e:
        logger.error(f"An error occurred while updating {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

    return None

@task
def core_table():

    schema = 'core'

    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = set(get_video_ids(cur, schema, table))

        current_video_ids = set() 

        cur.execute(
            f"""
            SELECT
                "Video_ID"        AS video_id,
                "Video_Title"     AS title,
                "Upload_Date"     AS "publishedAt",
                "Duration"        AS duration,
                "Video_Views"     AS "viewCount",
                "Likes_Count"     AS "likeCount",
                "Comments_Count"  AS "commentCount"
            FROM staging.{table};
            """
        ) 

        rows =  cur.fetchall() # Fetch all rows from staging table

        for row in rows:

            # Ensure all required keys exist
            row.setdefault('video_id', None)
            row.setdefault('title', '')
            row.setdefault('publishedAt', None)
            row.setdefault('duration', 'PT0S')  # Default to 0 seconds if missing
            row.setdefault('viewCount', 0)
            row.setdefault('likeCount', 0)
            row.setdefault('commentCount', 0)
                
            # # 🚫 Skip invalid rows
            # if not row.get("publishedAt"):
            #     logger.warning(
            #         f"Skipping video {row.get('video_id')} — missing Upload_Date"
            #     )
            #     continue

            if row.get('publishedAt') is None:
                logger.warning(f"Missing Upload_Date for video_id: {row.get('video_id')}")
                continue

            video_id  = row['video_id']


            current_video_ids.add(video_id) # Track current IDs

            transformed_row = transform_data(row)  # Transform the data

            if video_id not in table_ids:
                insert_rows(cur, conn, schema, transformed_row)  # Insert new row

            else:
                update_rows(cur, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:   
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"Core table '{schema}.{table}' updated successfully.")

    except Exception as e:
        logger.error(f"An error occurred while updating {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)



