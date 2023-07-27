import os
import googleapiclient.discovery
import googleapiclient.errors
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
API_KEY = os.getenv("API_KEY")

def get_video_metadata(videos, channel_name):
    youtube = init_youtube_client()

    # Prepare dataframe for the video metadata
    df_video_metadata = pd.DataFrame(
        columns=["video_id", "published_at", "channel_id", "view_count", "comment_count", "like_count", "dislike_count", "favorite_count"]
    )

    # Run until an exception is thrown
    for i, row in videos.iterrows():
        
        # Get data from the YouTube Data API
        request = youtube.videos().list(
            part = "snippet,statistics",
            id = row["video_id"]
        )
        try:
            response = request.execute()
           
            if len(response["items"]) == 1:
                item = response["items"][0]

                # Save important information
                video_id = item['id']
                published_at = check_existence_key_in_dict('publishedAt', item['snippet'])
                channel_id = check_existence_key_in_dict('channelId', item['snippet'])
                view_count = check_existence_key_in_dict('viewCount', item['statistics'])
                comment_count = check_existence_key_in_dict('commentCount', item['statistics'])
                like_count = check_existence_key_in_dict('likeCount', item['statistics'])
                dislike_count = check_existence_key_in_dict('dislikeCount', item['statistics'])
                favorite_count = check_existence_key_in_dict('favoriteCount', item['statistics'])

                df_row = pd.DataFrame(
                    {
                        "video_id": [video_id], 
                        "published_at": [published_at], 
                        "channel_id": [channel_id], 
                        "view_count": [view_count], 
                        "comment_count": [comment_count],
                        "like_count": [like_count], 
                        "dislike_count": [dislike_count], 
                        "favorite_count": [favorite_count]
                    }
                )

                # Add the video metadata to the dataframe
                df_video_metadata = pd.concat([df_video_metadata, df_row])

        except Exception as e:
            # when the end of data or quota limit is reached, save data and exit
            print("Exception", e, video_id)

    save_video_file(df_video_metadata, channel_name)

def check_existence_key_in_dict(key, dict):
    if key in dict:
        return dict[key]
    else: 
        return None

def init_youtube_client():
    """ initialise youtube api client with api key """
    return googleapiclient.discovery.build(
        serviceName=API_SERVICE_NAME, version=API_VERSION, developerKey=API_KEY
    )

def save_video_file(df_video_metadata, channel_name):
    """ save collected video data into parquet file"""
    file_name = f'video_meta_data{channel_name}.parquet.gzip'
    df_video_metadata.to_parquet(file_name, compression='gzip')

if __name__ == "__main__":
    channel_name = "xxx"
    df = pd.read_json(f"scrape/data/output-{channel_name}.json")
    #df = pd.read_json("video_channel_test.json")
    print(len(df))
    get_video_metadata(df, channel_name)
    print(pd.read_parquet(f'video_meta_data{channel_name}.parquet.gzip') )