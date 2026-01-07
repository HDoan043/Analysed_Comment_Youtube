'''
Problems:
    _ Currently, the data is set of videos'comments, stored in folder with tree structure:
        + <channel1>/
            |_channel_statistics.json
            |_videos/
                |_<video1>/
                    |_video_statistics.json
                    |_video_comments.json
                |_<video2>/
                ...
        + <channel2>/
            ...
        ...
    _ This may be difficult to divide data into batches of comments:
        + I expect to group some comments into a batch and send the whole batch to LLM to process
        --> This helps save the requests
        + But single comments are in file 'video_comments.json' of each folder video, if directly split batches:
            ~ For example: the file '<video1>/vide_comments.json' includes about 199 comments
            ~ But, batch size is 100
            ~ There are 99 comments else 
            -> to process, we have to group them with a comment in the file <video2>/'video_comments.json'
            --> Complicatly to read from different files and other folders
            
Target:
    _ A file json that contains a list of JSON object. Each object is an item with information of:
        + channel_statistics
        + video_statistics
        + information of only ONE comment
'''

import os
import json

DATA_DIR = "e://hung/project/project3/collecting_data/data/vietnamese/"

channel_list = os.listdir(DATA_DIR)
flatten_result = {}
for i, channel in enumerate(channel_list):
    channel_path = os.path.join(DATA_DIR, channel)
    channel_statistics = os.path.join(channel_path, "channel_statistics.json")
    if "videos" in os.listdir(channel_path):
        videos_path = os.path.join(channel_path, "videos")
        video_list = os.listdir(videos_path)
        for j, video in enumerate(video_list):
            print(f"\rProcessing {i+1}/{len(channel_list)} channels: {j+1}/{len(video_list)} videos ....     ", end="")
            video_path = os.path.join(videos_path, video)
            video_statistics = os.path.join(video_path, "video_statistics.json")
            if "video_comments.json" in os.listdir(video_path):
                video_comments = os.path.join(video_path, "video_comments.json")
                with open(channel_statistics, "r", encoding="utf-8") as f:
                    channel_json = json.load(f)
                with open(video_statistics, "r", encoding="utf-8") as f:
                    video_json = json.load(f)
                with open(video_comments, "r", encoding="utf-8") as f:
                    video_comment = json.load(f)
                
                for index, comment in enumerate(video_comment):
                    unique_id = channel_json["id"] + "-" + str(j) + "-" + str(index)
                    flatten_result[unique_id] = {
                            "channel_title": channel_json["title"],
                            "channel_views": channel_json["viewCount"],
                            "channel_subscribers": channel_json["subscriberCount"],
                            "channel_videos": channel_json["videoCount"],
                            "video_title": video_json["title"],
                            "video_description": video_json["description"],
                            "video_tags": video_json["tags"],
                            "video_views": video_json["views"],
                            "video_likes": video_json["likes"],
                            "video_favorites": video_json["favorites"],
                            "video_comments": video_json["comments"],
                            "comment": comment["comment"],
                            "comment_likes": comment["likes"],
                            "comment_replies_count": comment["repliesCount"],
                            "comment_replies": comment["replies"]
                        }
                if len(video_comment) == 0:
                    unique_id = channel_json["id"] + "-" + "0" + "-" + "0"
                    flatten_result[unique_id] = {
                            "channel_title": channel_json["title"],
                            "channel_views": channel_json["viewCount"],
                            "channel_subscribers": channel_json["subscriberCount"],
                            "channel_videos": channel_json["videoCount"],
                            "video_title": video_json["title"],
                            "video_description": video_json["description"],
                            "video_tags": video_json["tags"],
                            "video_views": video_json["views"],
                            "video_likes": video_json["likes"],
                            "video_favorites": video_json["favorites"],
                            "video_comments": video_json["comments"],
                            "comment": "",
                            "comment_likes": 0,
                            "comment_replies_count": 0,
                            "comment_replies": []
                        }
                    
print()
print(f"[DONE] Processed {len(list(flatten_result.keys()))} comments")

# Saving flatten
tmp = "e://hung/project/project3/build_dataset/tmp"
os.makedirs(tmp, exist_ok=True)

with open(os.path.join(tmp, "flatten_collection.json"), "w", encoding="utf-8") as f:
    json.dump(flatten_result, f)

print(f"[SAVE] The flatten result is saved in {os.path.join(tmp, "flatten_collection.json")}")
