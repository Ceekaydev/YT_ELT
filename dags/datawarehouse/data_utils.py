from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from airflow.models import Variable
# import os
import time
import requests
# from dotenv import load_dotenv
# load_dotenv()

HF_API_URL = Variable.get("HUGGING_FACE_URL")
HF_API_KEY =  Variable.get("HUGGINGFACE_API_TOKEN")
HF_HEADERS = {
    "Authorization": f"Bearer {HF_API_KEY}"
}


HF_CALL_DELAY = 0.25
MAX_HF_CALLS = 900
hf_call_count = 0
table = "yt_api"

def get_hf_sentiment(text: str) -> dict:
    """
    Docstring for get_hf_sentiment
    
    :param text: Description
    :type text: str
    :return: Description
    :rtype: dict
    """

    global hf_call_count

    if hf_call_count >= MAX_HF_CALLS:
        raise RuntimeError("Maximum Hugging Face API calls exceeded.")
    
    response = requests.post(
        HF_API_URL,
        headers=HF_HEADERS,
        json={"inputs": text},
        timeout=30
    )

    if response.status_code == 503:
        time.sleep(2)
        return get_hf_sentiment(text)
    
    response.raise_for_status()
    hf_call_count += 1
    time.sleep(HF_CALL_DELAY)

    result = response.json()[0]
    best = max(result, key=lambda x: x['score'])

    return {
        "label": best['label'].upper(),
        "score": round(best['score'], 4)
    }


def safe_sentiment(text: str) -> dict:
    """
    Docstring for safe_sentiment
    
    :param text: Description
    :type text: str
    :return: Description
    :rtype: dict
    """

    try:
        sentiment = get_hf_sentiment(text)
    except Exception as e:
        sentiment = {"label": "NEUTRAL  ", "score": 0.0}
    
    return sentiment

def get_conn_cursor() -> RealDictCursor:
    hooks = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database='elt_db')
    conn = hooks.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()
    return None


def create_schema(schema: str):

    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    conn.commit()

    close_conn_cursor(conn, cur)
    return None

def create_table(schema: str):

    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT
                );
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" TIME NOT NULL,
                    "Video_type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT,
                    "Title_Sentiment" VARCHAR(15),
                    "Sentiment_Score" NUMERIC(5,4)
                
                );
            """
        
    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema: str, table) -> list:

    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["Video_ID"] for row in ids]

    return video_ids

