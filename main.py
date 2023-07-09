import pandas
import requests
import json
import os
import time
import config


def getMessages(offset, limit):
    url = f'{config.SERVER_URL}:1234/api/v1/chat/{config.CHAT_GUID}/message?password={config.SERVER_PASS}&with=attachment&limit={limit}&offset={offset}'
    print(f"\nGET {url}")
    response = requests.get(
        url,
    )
    data_json = response.json()["data"]
    return data_json


# list of dfs to concatenate
all_dfs = []

# starting index
message_index = 1

# if it exists, load existing messages from file
file_name = config.EXISTING_DATA_PATH
existing_present = False
if os.path.exists(file_name):
    existing_present = True
    print(f"\n{config.EXISTING_DATA_PATH} discovered and loading...")
    old_df = pandas.read_json(file_name)
    all_dfs.append(old_df)
    # start at end of existing data
    message_index=len(old_df.index) + 1
    print("messages have been loaded from existing file")

while True:
    try:
        # read data into df
        data_json = getMessages(message_index, 1000)
        # print(data_json)
        df = pandas.DataFrame(data_json)

        # delete unused fields
        fields_to_delete = ["attributedBody", "handle", "handleId", "otherHandle", "subject", "isArchived", "itemType", "groupTitle", "groupActionType", "balloonBundleId", "expressiveSendStyleId", "country", "isDelayed", "isAutoReply", "isSystemMessage", "isServiceMessage", "isForward", "isCorrupt", "datePlayed", "cacheRoomnames", "isSpam", "isExpired", "chats", "messageSummaryInfo", "payloadData"]
        df.drop(fields_to_delete, axis=1, inplace=True)

        # add to list of dfs to concatenate
        all_dfs.append(df)
    except Exception as e:
        print(f"AN ERROR OCCURED: {str(e)}")
        print("exiting loop...")
        break
    else:
        # concatenate all dfs to append new messages
        df = pandas.concat(all_dfs)
        all_dfs = [df]
        # write to file
        print("writing to file...")
        df.to_json("messages-complete.json", orient="records", indent=2)
        print("wrote to file")
        # new starting point
        message_index=len(df.index) + 1
    time.sleep(0.2)


print("\n\nfinal df")
print(df)

