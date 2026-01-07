'''
The whole dataset is too large and costs so much time to process

--> Idea: Distributing them:
    + Distributing data (Each partition should less than 25MB so that it can be uploaded to GitHub)
    + Distributing API Key
    + Run executors to process many partitions at the same time 
    ( Use many Kaggle Notebooks to run )
'''
import json
import os
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, default="e://hung/project/project3/build_dataset/tmp/flatten_collection.json")
parser.add_argument("--history_dir", type=str, default="e://hung/project/project3/build_dataset/tmp/final_data.json")
parser.add_argument("--api_dir",type=str, default="e://hung/project/project3/build_dataset/config/api_key.json")
parser.add_argument("--partitions", type=int, default = 2, help="Number of partitions to be splitted")
args = parser.parse_args()

os.makedirs("./tmp", exist_ok=True)
os.makedirs("./config", exist_ok=True)

# Read data
data_dir = args.data_dir
with open(data_dir, "r") as f:
    full_data = json.load(f)
    
# Read history
history_dir = args.history_dir
with open(history_dir, "r") as f:
    history = json.load(f)
    
# Filter records that have not been processed yet
id_ls = list(id for id in full_data if id not in history)
random.shuffle(id_ls)
print("_ Full dataset: {}".format(len(list(full_data.keys()))))
print("_ History: {}".format(len(list(history.keys()))))
print("_ Records not be processed: {}".format(len(id_ls)))

# Split subsets
num_subsets = args.partitions
len_subset = len(id_ls)//num_subsets

print("_ Spliting dataset...")
for i in range(num_subsets):
    if i == num_subsets -1:
        sub_id_ls = id_ls[i*len_subset:]
    else:
        sub_id_ls = id_ls[i*len_subset: (i+1)*len_subset]    
    sub_id = {}
    for id in sub_id_ls:
        sub_id[id] = full_data[id]
    print(f"\t_ Len subset {i}: {len(sub_id_ls)}")
    filename = f"./tmp/flatten_collection_{i}.json"
    print(f"\t  Saving subset {i}...")
    with open(filename, "w", encoding = "utf-8") as f:
        json.dump(sub_id, f)
        
# Split api key
api_dir = args.api_dir
with open(api_dir, "r") as f:
    api_key = json.load(f)

project_ls = list(api_key.keys())
len_key_file = len(project_ls)//num_subsets
api_files = []
print("_ Spliting api keys ...")
for i in range(num_subsets):
    if i == num_subsets -1:
        project_in_partitions = project_ls[i*len_key_file:]
    else:
        project_in_partitions = project_ls[i*len_key_file: (i+1)*len_key_file]
    
    sub_api_key = {}
    for project in project_in_partitions:
        sub_api_key[project] = api_key[project]
    
    print(f"\t_ Num keys in partition {i}: {len(project_in_partitions)}")
    print(f"\t  Saving keys in partition {i} ...")
    filename = f"./config1/api_key_{i}.json"
    with open(filename, "w") as f:
        json.dump(sub_api_key, f)