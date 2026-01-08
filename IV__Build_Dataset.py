import json
import argparse
#########################################
# GOOGLE API
from google import genai
from google.genai import types
#########################################

#########################################
# GROQ
# from groq import Groq, RateLimitError
########################################

import time
import os
import random


parser = argparse.ArgumentParser()
parser.add_argument("--index_data", type=int, default=0)
parser.add_argument("--api_folder", type=str, default="./config")
parser.add_argument("--data_folder", type=str, default="./tmp")
parser.add_argument("--history_dir", type=str, default="/kaggle/input/process_comment_yt_INDEX/final_data_INDEX.json")
parser.add_argument("--result_folder", type=str, default="./tmp")
args = parser.parse_args()
    
# Load flatten collection
def load(dir = "./tmp/flatten_collection_0.json"):
    with open(dir, "r", encoding="utf-8") as f:
        return json.load(f)
    return None

# Load prompt( this prompt is for system)
def load_prompt(
    prompt_dir = "./config/prompt.txt"
    ):
    with open(prompt_dir,"r", encoding = "utf-8") as f:
        return f.read()

# Get non-structured data for LLM:
def extract_non_structured(batch_comments):
    '''
    This should extract:
    _ 'id' -> this is for mapping the result
    _ 'video_title', 'video_tags', 'video_description' -> this is for background
    _ 'comment', 'comment_replies' -> this is for main data
    '''
    batch = []
    for each in batch_comments:
        item = each["item"]
        id = each["id"]
        batch.append({
            "id": id,
            "video_title": item["video_title"],
            "video_tags": item["video_tags"],
            "video_description": item["video_description"],
            "comment": item["comment"],
            "comment_replies": item["comment_replies"]
        })
    return batch
    
# Full prompt
def full_prompt(prompt, batch_comments):
    user_comments = json.dumps(batch_comments, ensure_ascii=False, indent=2)
    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": user_comments}
    ]
    return messages

def init_chat(api_key_index = 0, api_dir = "./config/api_key_0.json"):
    with open(api_dir, "r") as f:
        api_key_dict = json.load(f)
        api_key_list = list(api_key_dict.values())
        if api_key_index >= len(api_key_list):
            return None
        api_key = api_key_list[api_key_index]
        if not api_key.startswith("AIzaSy"):
            api_key = "AIzaSy" + api_key
    ###############################################
    # GOOGLE API
    client = genai.Client(api_key = api_key)
    ##############################################
    
    ##############################################
    # GROQ
    # client = Groq(api_key = api_key)
    ##############################################
    return client
    
# Ask LLM
def ask(system_prompt, batch_comments, client):
# def ask(messages, client):
    max_retries = 5
    base_delay = 10
    
    #######################################################
    # GOOGLE API
    # Config
    config = types.GenerateContentConfig(
        temperature = 0.0,
        response_mime_type = "application/json",
        system_instruction = system_prompt
    )
    
    user_comments = json.dumps(batch_comments, ensure_ascii=False, indent=2)
    model = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]
    for attempt in range(len(model)):
        try:    
            # print("\nAsking...")
            response = client.models.generate_content(
                model = model[attempt],
                contents = user_comments,
                config = config
            )
            # print("Finish Asking")
            if response.text:
                try:
                    json_response = json.loads(response.text)
                    return json_response
                except:
                    print()
                    print("[ERROR] The returned response is not in JSON format")
                    return None
            else:
                print()
                print("[FAIL] Call API fail, response: {}".format(response))
                return None
            
        except Exception as e:
            # print("[ERROR] Somethings wrong when call api :(( : {}".format(e))
            continue
            
            # print()
            # if "RESOURCE_EXHAUSTED" in str(e):
            #     print(f"❌ Lỗi: mô hình {model[attempt]} hết hạn mức. ", end="")
            #     if attempt < len(model) - 1: print(f"Đang chuyển sang mô hình {model[attempt+1]}")  
            #     else: print()
    return -1
            
    ########################################################
    
    ################################################
    # GROQ
    # for attempt in range(max_retries):
    #     try:
    #         completion = client.chat.completions.create(
    #             model='llama-3.1-8b-instant',
    #             messages=messages, 
    #             temperature=0.0,
    #             response_format={"type": "json_object"}
    #         )
    #         return json.loads(completion.choices[0].message.content)
            
    #     except RateLimitError as e:
    #         # Phân tích thời gian cần chờ từ thông báo lỗi
    #         error_msg = str(e)
    #         wait_time = 60 # Mặc định chờ 60s
            
    #         # Cố gắng tìm số giây Groq bắt chờ (dạng "Please try again in 43.47s")
    #         match = re.search(r"in (\d+\.?\d*)s", error_msg)
    #         if match:
    #             wait_time = float(match.group(1)) + 2 # Cộng thêm 2s cho chắc
            
    #         print(f"\n⚠️ Hết Token (TPM). Đang nghỉ {wait_time:.1f}s rồi thử lại...")
    #         time.sleep(wait_time)
            
    #     except Exception as e:
    #         print(f"❌ Lỗi khác: {e}")
    #         return None
    # print()
    # print("❌ Đã thử quá nhiều lần, bỏ qua batch này.")
    ##################################################
    return None
    
def check_format(response):
    '''
    The response of AI may be in wrong format
    --> filter those wrong responses, remove them, keep the right format only
    '''
    right_response = {}
    wrong_response = 0
    for id, value in response.items():
        discussion_topic = value.get("discussion_topics", None)
        interest_type = value.get("interest_type", None)
        mentions_constraints = value.get("mentions_constraints", None)
        target_body_parts = value.get("target_body_parts", None)
        intent_signal = value.get("intent_signal", None)
        if discussion_topic and interest_type and mentions_constraints and intent_signal and target_body_parts:
            if not (("muscle_growth" in discussion_topic) and \
                ("fat_loss" in discussion_topic) and \
                ("training_program" in discussion_topic) and \
                ("exercise_technique" in discussion_topic) and \
                ("nutrition" in discussion_topic) and \
                ("supplementation" in discussion_topic) and \
                ("injury_recovery" in discussion_topic) and \
                ("motivation_mindset" in discussion_topic) and \
                ("power" in discussion_topic)):
                    # print("thiếu discussion_topic")
                    wrong_response+=1
                    continue
            
            if not( ("content_driven" in interest_type) and \
                ("fame_driven" in interest_type) and \
                ("entertainment_driven" in interest_type)):
                    # print("thiếu interest_type")
                    wrong_response +=1
                    continue
            
            if not (("time" in mentions_constraints) and \
                ("equipment" in mentions_constraints) and \
                ("experience_level" in mentions_constraints) and \
                ("physical_limitations" in mentions_constraints) and \
                ("specific_body_focus" in mentions_constraints) and \
                ("unbalace_muscle" in mentions_constraints) and \
                ("progressive" in mentions_constraints)):
                    # print("thiếu mentions_constraints")
                    wrong_response += 1
                    continue
            
            if not (("learning_intent" in intent_signal) and \
                ("buying_intent" in intent_signal) and \
                ("follow_up_intent" in intent_signal)):
                    # print("thiếu intent_signal")
                    wrong_response += 1
                    continue
            
            if not (("chest" in target_body_parts) and \
                ("back" in target_body_parts) and \
                ("legs" in target_body_parts) and \
                ("shoulders" in target_body_parts) and \
                ("arms" in target_body_parts) and \
                ("hands" in target_body_parts) and \
                ("abs" in target_body_parts) and \
                ("cardio" in target_body_parts)):
                    # print("thiếu target_body_parts")
                    wrong_format += 1
                    continue
    
            right_response[id] = value
        else: 
            # print("Thiếu trường lớn")
            wrong_response += 1
            continue
    return right_response, wrong_response
                
# Data after processing
def final_data(analysed, final, data):
    for comment_id, analysed_result in analysed.items():
        original_data = data[comment_id]
        final[comment_id] = {}
        final[comment_id]["channel_views"] = original_data["channel_views"]
        final[comment_id]["channel_subscibers"] = original_data["channel_subscribers"]
        final[comment_id]["channel_videos"] = original_data["channel_videos"]
        final[comment_id]["video_views"] = original_data["video_views"]
        final[comment_id]["video_likes"] = original_data["video_likes"]
        final[comment_id]["video_favorites"] = original_data["video_favorites"]
        final[comment_id]["comment_likes"] = original_data["comment_likes"]
        final[comment_id]["comment_replies_count"] = original_data["comment_replies_count"]
        final[comment_id]["discussion_topics"] = analysed_result["discussion_topics"]
        final[comment_id]["interest_type"] = analysed_result["interest_type"]
        final[comment_id]["mentions_constraints"] = analysed_result["mentions_constraints"]
        final[comment_id]["target_body_parts"] = analysed_result["target_body_parts"]
        final[comment_id]["intent_signal"] = analysed_result["intent_signal"]
    return final

if __name__  =="__main__":
    
    # Load data
    print("="*50)
    print("\t\tLOAD DATA")
    INDEX_DATA = args.index_data
    data_folder = args.data_folder
    data_dir = os.path.join(data_folder, f"flatten_collection_{INDEX_DATA}.json")
    data = load(data_dir)
    print()
    
    # Load prompt
    print("="*50)
    print("\t\tLOAD PROMPT")
    prompt = load_prompt()
    print()
    
    # Init chat
    print("="*50)
    print("\t\tINIT CHAT")
    api_key_index = 0
    api_folder = args.api_folder
    api_key_dir = os.path.join(api_folder, f"api_key_{INDEX_DATA%10}.json")
    client = init_chat(api_key_index, api_key_dir)
    with open(api_key_dir, "r") as f:
        api_dict = json.load(f)
    project_name_list = list(api_dict.keys())
    print("_ Start with api key of project : ", project_name_list[api_key_index])
    
    # Load history
    history_dir = args.history_dir
    history_dir = history_dir.replace("INDEX", str(INDEX_DATA))
    try:
        with open(history_dir, "r", encoding="utf-8") as f:
            final_result = json.load(f)
    except:
        final_result = {}
        
    # Result_dir
    result_folder = args.result_folder
    os.makedirs(result_folder, exist_ok=True)
    result_dir = os.path.join(result_folder, f"final_data_{INDEX_DATA}.json")

    # Filter which comments have not been processed yet    
    print() 
    print("="*50)
    print("\t\tSHUFFLE DATA")
    comments_id = list(data.keys())
    comments_id = [id for id in comments_id if id not in final_result]
    random.shuffle(comments_id)
    batch_size = 50
    num_batch = len(comments_id)//batch_size
    
    # Loop for each batch comments and process
    print("="*50)
    print("\t\tPROCESSING")
    total_comment = len(list(final_result))
    for i in range(num_batch+1):
        
        # Get batch
        start_batch_index = i*batch_size
        if i < num_batch:
            end_batch_index = (i+1)*batch_size
            batch_ids = comments_id[start_batch_index: end_batch_index]
        else:
            batch_ids = comments_id[start_batch_index: ]
        batch = [{"id": id, "item": data[id]} for id in batch_ids]
        total_comment += len(batch)
        
        # Extract non structred data in batch
        non_structured_data = extract_non_structured(batch)
        
        # Messages
        messages = full_prompt(prompt, non_structured_data)
        
        # Get response
        
        ##########################################
        # GOOGLE API
        check_end_quota = False
        while 1:
            response = ask(prompt, batch, client)
            if response == -1: # api_key hết hạn mức
                print(f"\n[END OF QUOTA] API Key of project {project_name_list[api_key_index]} is end of quota, changing to the other API Key...")
                api_key_index += 1
                # thử api_key khác
                client = init_chat(api_key_index, api_key_dir)
                if not client:
                    check_end_quota = True
                    break
            else:
                break
        if check_end_quota:
            print("\n[END] All of the api_keys are tried, but end of quota")
            break
        ##########################################
        
        ##########################################
        # GROQ
        # response = ask(messages, client)
        ##########################################
        
        try:
            # Check quality
            response, wrong_format = check_format(response)
            
            # Data after processed
            final_result = final_data(response, final_result, data)
            # print("\nWritting...")
            with open(result_dir, "w", encoding="utf-8") as f:
                json.dump(final_result, f)
            # print("\nFinish writting.")
            time.sleep(20)
        except:
            print()
            print("[ERROR] Error in batch {}".format(i))
            time.sleep(60)
        print(f"\rProcessing {total_comment}/{len(list(data))} comments ....  - {wrong_format} comments are wrong format      ", end="")            
        # break


    print(f"[DONE] Finish processing {total_comment}/{len(list(data))} comments.")


