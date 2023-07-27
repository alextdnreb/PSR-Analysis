import os
import googleapiclient.discovery
import google_auth_oauthlib.flow
import googleapiclient.errors
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from get_video_metadata import check_existence_key_in_dict

load_dotenv()

API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
API_KEY = os.getenv("API_KEY")

def get_video_comments(videos, channel_name):
    youtube = init_youtube_client()

    # read 'nextPageToken' from a previous run if it exists
    next_page_token_file = Path("page_token.txt")
    next_page_token = next_page_token_file.read_text() if next_page_token_file.exists() else ""
    
    for i, video in videos.iterrows():
        df_video_comments = pd.DataFrame(
            columns=["comment_id", "published_at", "comment", "author_channel_id", "author_display_name", "video_id"]
        )   
        # run until an exception is thrown
        while True:
            request = youtube.commentThreads().list(
                part="snippet",
                maxResults=100,
                videoId=video["video_id"],
                pageToken=next_page_token
            )
            try:
                response = request.execute()
                for comment_thread in response["items"]:
                    top_level_comment = comment_thread["snippet"]["topLevelComment"]
                    df_row = pd.DataFrame(
                        {
                            "comment_id": [check_existence_key_in_dict("id", top_level_comment)],
                            "published_at": [check_existence_key_in_dict("publishedAt", top_level_comment["snippet"])],
                            "comment": [check_existence_key_in_dict("textOriginal", top_level_comment["snippet"])],
                            "author_channel_id": [check_existence_key_in_dict("value", check_existence_key_in_dict("authorChannelId", top_level_comment["snippet"]))],
                            "author_display_name": [check_existence_key_in_dict("authorDisplayName", top_level_comment["snippet"])],
                            "video_id": [video["video_id"]]
                        }
                    ) 
                    df_video_comments = pd.concat([df_video_comments, df_row])
                next_page_token = response["nextPageToken"]

            except Exception as e:
                # when the end of data or quota limit is reached, save data and exit
                print(e)
                write_next_page_token(next_page_token)
                print(df_video_comments)
                save_video_file(df_video_comments, channel_name, video["video_id"])

                break


def init_youtube_client():
    """ initialise youtube api client with api key """
    return googleapiclient.discovery.build(
        serviceName=API_SERVICE_NAME, version=API_VERSION, developerKey=API_KEY
    )

def save_video_file(df_video_comments, channel_name, video_id):
    """ save collected video comments into parquet file"""
    file_name = f'video_comments_{channel_name}_{video_id}.parquet.gzip'
    df_video_comments.to_parquet(file_name, compression='gzip')

def write_next_page_token(token):
    """ save the nextPageToken into a txt file for later use """
    path = Path("page_token.txt")
    path.write_text(token)

if __name__ == "__main__":
    df = pd.read_json("video_channel.json")

    print(len(df))

    channel_name = "xxx"
    get_video_comments(df, channel_name)
    print(pd.read_parquet(f'video_comments_{channel_name}_iXMP7FBkA2g.parquet.gzip') )
