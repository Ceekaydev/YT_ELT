import requests
import json
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="./.env")
import os

from datetime import datetime

from airflow.decorators import task
from airflow.models import Variable


maxResults = 50


@task
def get_playlist_id():
        
    channel_name = Variable.get("CHANNEL_HANDLE")
    API_KEY = Variable.get("API_KEY")

    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_name}&key={API_KEY}"

        response = requests.get(url=url)
        response.raise_for_status()
        data = response.json()

        converted_json = json.dumps(data, indent=4)

        # print(converted_json)

        items = data.get("items", [])

        if not "items":
            print("No items found, check key")
        else:
            upload = (
                items[0]
                .get("contentDetails", {})
                .get("relatedPlaylists")
                .get("uploads")
            )

            return upload
        
    except Exception as e:
        print(f"Error: {e}")
        return False
    
@task
def get_video_ids(playlist_id):

    API_KEY = Variable.get("API_KEY")

    video_ids = []

    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlist_id}&maxResults={maxResults}&key={API_KEY}"

    try:

        while True:

            url = base_url

            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            pageToken = data.get("nextPageToken")

            if not pageToken:
                break

        return video_ids

    except Exception as e:
        print(f"Error: {e}")


def batch_list(video_id_list, batch_size):
    for i in range(0, len(video_id_list), batch_size ):
        yield video_id_list[i: i + batch_size]

@task
def extract_video_data(video_ids):

    API_KEY = Variable.get("API_KEY")

    extracted_data = []

    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id" : video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }

                extracted_data.append(video_data)

        return extracted_data


    except Exception as e:
        print(f"Error: {e}")


@task
def save_to_json(extracted_data, **context):

    execution_date = context["logical_date"].strftime("%Y-%m-%d")

    # Make this configurable for testing
    data_dir = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")

    os.makedirs(data_dir, exist_ok=True)

    filepath = f"{data_dir}/YT_data_{execution_date}.json"

    # DEBUG: Check the data
    print(f"Type: {type(extracted_data)}")
    print(f"Sample: {extracted_data[:1] if extracted_data else 'Empty'}")
    
    with open(filepath, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

    # Verify the file is valid JSON immediately after writing
    with open(filepath, "r", encoding="utf-8") as verify_file:
        json.load(verify_file)  # This will fail if JSON is invalid

    return {

        "file_path": filepath,
        "execution_date": execution_date
    }




# if __name__ == "__main__":
#     playlist_id = get_playlist_id()
#     video_ids = get_video_ids(playlist_id)
#     extracted_video_result = extract_video_data(video_ids)
#     save_to_json(extracted_video_result)




