import requests
import json
from dotenv import load_dotenv
import os


load_dotenv()


API_KEY = os.getenv("API_KEY")
channel_name = "MrBeast"
maxResults = 50

def get_playlist_id():

    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_name}&key={API_KEY}"

        response = requests.get(url=url)
        response.raise_for_status
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
    

def get_video_ids(playlist_id):

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
    for video_id in range(0, len(video_id_list), batch_size ):
        yield video_id_list[video_id: video_id + batch_size]


def extract_video_data(video_ids):

    extracted_data = []

    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size ):
            yield video_id_list[video_id: video_id + batch_size]

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



if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_video_result = extract_video_data(video_ids)




