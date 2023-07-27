import os
import googleapiclient.discovery
import google_auth_oauthlib.flow
import googleapiclient.errors
from pathlib import Path
import json
from dotenv import load_dotenv
import time
import pandas as pd

load_dotenv()

API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
API_KEY = os.getenv("API_KEY")

def get_channels(list):
    youtube = init_youtube_client()

    all_channels = []

    # run until an exception is thrown
    for i, row in list.iterrows():
        request = youtube.channels().list(
            part="snippet, statistics",
            id=row["Channel Name"],
        )
        try:
            response = request.execute()
            for channel in response["items"]:
                all_channels.append(channel)

        except Exception as e:
            # when the end of data or quota limit is reached, save data and exit
            print(e)
            
    write_channel_file(all_channels)


def init_youtube_client():
    """ initialise youtube api client with api key """
    return googleapiclient.discovery.build(
        serviceName=API_SERVICE_NAME, version=API_VERSION, developerKey=API_KEY
    )

def write_channel_file(videos):
    """ save collected video data into json file"""
    path = Path(f"channel-infos-{time.time()}.json").open("w")
    path.write(json.dumps(videos, indent=4, separators=(',', ': ')))

if __name__ == "__main__":
    df = pd.read_excel("./channels.xlsx")
    print(len(df))
    
    get_channels(df)