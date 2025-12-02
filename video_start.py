import requests
import json
from dotenv import load_dotenv
import os


load_dotenv()


API_KEY = os.getenv("API_KEY")
channel_name = "MrBeast"

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

if __name__ == "__main__":
    success = get_playlist_id()

    if success:
        print(f"successful. ID: {success}")
    else:
        print("Unsuccessful, Check code.")


