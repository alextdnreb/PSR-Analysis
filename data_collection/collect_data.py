import os
import pandas as pd
import re
from get_video_metadata import get_video_metadata
from get_comments_for_video import get_video_comments

def iterate_over_videos(directory):
    for file in os.listdir(directory):
        channel_name = "xxx"
        df = pd.read_json(f"scrape/data/output-{channel_name}.json")
        channel_name = re.split(r"[-.]", file)[1]
        print(channel_name)
        get_video_metadata(df, channel_name)
        get_video_comments(df, channel_name)

if __name__ == '__main__':
    directory = "./scrape/data"
    iterate_over_videos(directory)