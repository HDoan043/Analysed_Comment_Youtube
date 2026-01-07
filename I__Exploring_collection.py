import json
import os

DATA_DIR = "e://hung/project/project3/collecting_data/data/vietnamese/"

# [1] - Explore channels
print("="*50)
print("\t\tEXPLORING CHANNELS")
channel_list = os.listdir(DATA_DIR)
print("_ There are {} channels collected".format(len(channel_list)))
print("_ Each channel has: ")
print("\t+ Folder \'videos\'")
print("\t+ File \'channel_statistics.json\': File JSON has some fields")
print("\t\t~ \'id\'")
print("\t\t~ \'title\'")
print("\t\t~ \'viewCount\'")
print("\t\t~ \'subcribeCount\'")
print("\t\t~ \'videoCount\'")
print("\t\t~ \'playlistId\'")

# [2] - Explore videos
print("="*50)
print("\t\tEXPLORING VIDEOS")
print("_ All videos are in folder \'videos\' of folder channel")
print("_ Each video is corressponding with a folder, having:")
print("\t+ file \'video_statistic.json\': ")
print("\t\t~ \'title\'")
print("\t\t~ \'description\'")
print("\t\t~ \'tag\'")
print("\t\t~ \'views\'")
print("\t\t~ \'likes\'")
print("\t\t~ \'favourite\'")
print("\t\t~ \t'comments\'")
print("\t+ file \'video_comments\'")

# [3] - Explore comments
print("="*50)
print("\t\tEXPLORING COMMENTS")
total_comments = 0
for i, channel in enumerate(channel_list):
    channel_path = os.path.join(DATA_DIR, channel)
    if 'videos' in os.listdir(channel_path):
        videos_path = os.path.join(channel_path, "videos")
        videos_list = os.listdir(videos_path)
        for j, video in enumerate(videos_list):
            print("\rCounting comments in {}/{} channels: {}/{} videos...          ".format(i+1, len(channel_list), j+1, len(videos_list)), end="")
            video_path = os.path.join(videos_path, video)
            if "video_comments.json" in os.listdir(video_path):
                comment_path = os.path.join(video_path, "video_comments.json")
                with open(comment_path, "r", encoding="utf-8") as f:
                    total_comments += len(json.load(f))
print()
print("_ There are {} comments collected".format(total_comments))
print("_ Comments of a video in the file \'video_comments.json\', having:")
print("\t+ \'comment\'")
print("\t+ \'likes\'")
print("\t+ \'repliesCount\'")
print("\t+ \'replies\':")
print("\t\t~ \'text\'")
print("\t\t~ \'likes\'")

print("="*50)
print("\t\tCONCLUSION")
print("_ Next duty is to flatten all of the above tree structrure:")
print("\t+ Output: a list of JSON objects, each contains information of channel_statistics, video_statistics and a comment")
print("\t+ This is for more easily to divide comments into batchs with the same size")
