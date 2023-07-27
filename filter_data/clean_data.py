import pandas as pd
import os
import hashlib

dir_path = "/home/ubuntu/ccs-project/data_collection/data/"
for index, folder in enumerate(os.listdir(dir_path)):
    all_comments = pd.DataFrame()
    for index, file in enumerate(os.listdir(os.path.join(dir_path, folder))):
        if file == "video_meta_data.parquet.gzip":
            continue
        file = pd.read_parquet(os.path.join(dir_path, folder, file))
        all_comments = pd.concat([all_comments, file])

    all_comments = all_comments.reset_index(drop=True)
    print("Total number of comments:", len(all_comments))

    filtered_comments = all_comments.dropna(subset=["comment"])
    print("Total number of comments after dropna:", len(filtered_comments))
    index = []
    for row in filtered_comments.itertuples():
        if len(str(row.comment).split()) < 10: 
            index.append(row.Index)

    df_new = filtered_comments.drop(index=index)
    print("Total number of comments with more than 10 words: ", len(df_new))

    # Delete same comments pro video
    df = df_new.drop_duplicates(subset=['video_id', 'comment'])

    # Delete same comments of one user (e.g., remove chat bots)
    df_no_duplicates = df.drop_duplicates(subset=['author_channel_id', 'comment'])
    print("Total number of comments after removing duplicates: ", len(df_no_duplicates))

    df_only_first_comment = df_no_duplicates.sort_values(by="published_at", ascending=True).drop_duplicates(subset=["author_channel_id", "video_id"], keep="first")
    print("Total number of comments after removing multiple comments under the same video:", len(df_only_first_comment))

    def has_n_comments(user, n, comment_counts):
        return comment_counts.get(user, default=0) > n

    comment_counts = df_only_first_comment["author_channel_id"].value_counts()

    df_min_3_comments = df_only_first_comment[df_only_first_comment["author_channel_id"].apply(has_n_comments, args=(3, comment_counts))]
    print("Total number of comments after filtering users with > 3 comments: ", len(df_min_3_comments))

    print(f"Number of users for channel: {folder}: {df_only_first_comment['author_channel_id'].nunique()}")

    df_min_3_comments.drop(columns=["author_display_name"], inplace=True)

    # Convert column to string
    df['author_channel_id'] = df['author_channel_id'].astype(str)
    # Apply hashing function to the column
    df['author_channel_id'] = df['author_channel_id'].apply(
        lambda x: 
            hashlib.sha256(x.encode()).hexdigest())
    
    df_min_3_comments.to_parquet(f"/home/ubuntu/filtered_data/{folder}")