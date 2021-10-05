import argparse
import json
import os
import platform
import shutil
import sys
import time
import urllib.request
from datetime import datetime
from queue import Queue, Empty
from threading import Thread

import urllib3
from functools import partial, lru_cache
from json import JSONDecodeError
from multiprocessing import Pool, cpu_count
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


class Response:
    def __init__(self, content):
        self.raw = content
        self.body = json.loads(content)
        self.successful = self.body["ok"]
        self.error = self.body.get("error")

    def __str__(self):
        return json.dumps(self.body)


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def mkdir(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def change_name(u, name):
    for i in u:
        if i.get("real_name") == name:
            i["real_name"] = i["real_name"] + "2"
            i["profile"]["real_name"] = i["real_name"]
            i["profile"]["real_name_normalized"] = i["real_name"]
            break


def get_users():
    users_ = Response(
        requests.get("https://slack.com/api/users.list", headers=token).text
    ).body["members"]
    change_name(users_, "Team Standup")  # local
    change_name(users_, "calendar reminder Changyao")  # local
    print(f"Found {len(users_)} Users")
    return users_


def get_dms():
    dms_ = Response(
        requests.get(
            "https://slack.com/api/conversations.list",
            params={"types": "im", "limit": 1000},
            headers=token,
        ).text
    ).body
    # if dms_["response_metadata"]["next_cursor"]:
    #     new_page = get_next_page(dms_["response_metadata"], "im", "channels")
    #     dms_['channels'].extend(new_page)
    print(f"Found {len(dms_['channels'])} 1:1 DM conversations")
    return dms_["channels"]


def get_user_map():
    for user in users:
        user_names_by_id[user["id"]] = user["name"]
        user_id_by_names[user["name"]] = user["id"]


def test_auth():
    test_ = Response(
        requests.get("https://slack.com/api/auth.test", headers=token, proxies=urllib.request.getproxies()).text
    ).body
    team_name = test_["team"]
    current_user = test_["user"]
    print(f"Successfully authenticated for team {team_name} and user {current_user}")
    return test_


def fetch_dms(arr, user_token, room_name):
    c = []
    file = []
    dm_arr = [arr]
    for dm in dm_arr:
        dm_id = dm["id"]
        # chat_name["im"].extend(dm_id)
        sleep(.1)
        c.append(dm_id)
        mkdir(dm_id)
        messages = get_history2(dm_id, user_token)
        file.append(parse_messages1(dm_id, messages, "im", room_name))
    return c, file


def fetch_dms_sync():
    process_count = 0
    for dm in dms:
        dm_id = dm["id"]
        name = user_names_by_id[dm["user"]]
        progress_bar_kwargs = dict(prefix='Progress:', suffix=f"Fetching {name}", length=40)
        process_count += 1
        print_progress_bar(process_count, len(dms), **progress_bar_kwargs)
        # progress_bar(len(channels), total, )
        sleep(.1)
        # chat_name["im"].extend(dm_id)
        sleep(.1)
        mkdir(dm_id)
        messages = get_history2(dm_id)
        parse_messages(dm_id, messages, "im")


def fetch_groups_sync():
    process_count = 0
    for group in groups:
        group_name = group["name"]
        progress_bar_kwargs = dict(prefix='Progress:', suffix=f"Fetching {group_name}", length=40)
        process_count += 1
        print_progress_bar(process_count, len(groups), **progress_bar_kwargs)
        # progress_bar(len(channels), total, )
        sleep(.1)
        sleep(.1)
        mkdir(group_name)
        messages = get_history2(group["id"])
        parse_messages(group_name, messages, "group")


def fetch_groups(arr, user_token, room_name):
    c = []
    file = []
    group_arr = [arr]
    for group in group_arr:
        group_name = group["name"]
        sleep(.1)
        mkdir(group_name)
        c.append(group_name)
        messages = get_history2(group["id"], user_token)
        file.append(parse_messages1(group_name, messages, "group", room_name))
    return c, file


def fetch_public_channels_sync():
    process_count = 0
    print("Fetching Public Channel")
    for channel in channels:
        channel_name = channel["name"]
        progress_bar_kwargs = dict(prefix='Progress:', suffix=f"Fetching {channel_name}", length=40)
        process_count += 1
        print_progress_bar(process_count, len(channels), **progress_bar_kwargs)
        # progress_bar(len(channels), total, )
        sleep(.1)
        chat_name["channel"].append(channel_name)
        mkdir(channel_name)
        messages = get_history2(channel["id"])
        parse_messages(channel_name, messages, "channel")
    print("Complete Fetching Public Channel")


def fetch_public_channels(chats, user_token, room_name):
    c = []
    file = []
    chats = [chats]
    for channel in chats:
        channel_name = channel["name"]
        sleep(.4)
        c.append(channel_name)
        mkdir(channel_name)
        messages = get_history2(channel["id"], user_token)
        file.append(parse_messages1(channel_name, messages, "channel", room_name))

    return c, file


def dump_channel():
    for dm in dms:
        dm["members"] = [dm["user"], owner_id]
        dm["members_name"] = [user_names_by_id[dm["user"]], owner]

    for channel in channels:
        channel["members"] = [*user_names_by_id]

    with open("groups.json", "w", encoding="utf-8") as f:
        json.dump(groups, f, indent=4, ensure_ascii=False)

    with open("channels.json", "w", encoding="utf-8") as f:
        json.dump(channels, f, indent=4, ensure_ascii=False)

    with open("dms.json", "w", encoding="utf-8") as f:
        json.dump(dms, f, indent=4, ensure_ascii=False)


def parse_messages(room_id, messages, room_type):
    current_file_date = ""
    current_messages = []
    for message in messages:
        if message.get("files"):

            for i in message["files"]:
                # i["url_private"] = i.get("url_private") + f"?t=xoxe-2256472312918-2285466647201-2269788674549-749f8ce2f4ae5f17b05c1e8e956c1e69"
                if not room_type == "channel":
                    i["room_name"] = room_name_by_id[room_id]
            # files[room_type].extend(message["files"])
            files[room_type] = message["files"]
        ts = parse_timestamp(message["ts"])
        file_date = "{:%Y-%m-%d}".format(ts)
        if message.get("user"):
            message["user_name"] = user_names_by_id[message["user"]]

        if file_date != current_file_date:
            out_file_name = "{room}/{file}.json".format(room=room_id, file=file_date)
            dump_messages(out_file_name, current_messages)
            current_file_date = file_date
            current_messages = []

        current_messages.append(message)
    out_file_name = "{room}/{file}.json".format(room=room_id, file=current_file_date)
    dump_messages(out_file_name, current_messages)


def dump_reminder(data):
    with open("reminders.json", 'w', encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    convert_date = lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M')
    with open("reminders.txt", 'w', encoding="utf-8") as f:
        text = ''
        total = 0
        for i in data:
            # progress_bar(len(data), total + 1, "")
            # total += 1
            time.sleep(0.1)
            text += f'Created by {user_names_by_id[i["creator"]]}' \
                    f' to {user_names_by_id[i["user"]]}' \
                    f' about {i["text"]}' \
                    f' on date {convert_date(i["time"])}\n\n'
        f.write(text)
    shutil.move(os.path.join(".", "reminders.txt"), os.path.join("../", "reminders.txt"))


def dump_users():
    with open("users.json", "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=4)


def dump_ids():
    with open("chats_id.json", "w", encoding="utf-8") as f:
        json.dump(chat_name, f, indent=4, ensure_ascii=False)


def dump_messages(file_name, messages):
    directory = os.path.dirname(file_name)

    if not messages:
        return

    if not os.path.isdir(directory):
        mkdir(directory)

    with open(file_name, "w", encoding="utf-8-sig") as f:
        json.dump(messages, f, indent=4, ensure_ascii=False)


def dump_files():
    with open("files.json", "w", encoding="utf-8") as f:
        json.dump(files, f, indent=4, ensure_ascii=False)


# create datetime object from slack timestamp ('ts') string
def parse_timestamp(timestamp):
    if "." in timestamp:
        t_list = timestamp.split(".")
        if len(t_list) != 2:
            raise ValueError("Invalid time stamp")
        else:
            return datetime.utcfromtimestamp(float(t_list[0]))


def get_reminder():
    data = Response(
        requests.get("https://slack.com/api/reminders.list", headers=token).text
    ).body['reminders']
    now_timestamp = datetime.now().timestamp()
    future_reminders = list(filter(lambda i: i['time'] > now_timestamp, data))

    dump_reminder(future_reminders)


def get_private_channel():
    groups_ = Response(
        requests.get("https://slack.com/api/conversations.list",
                     params={"types": "private_channel", "limit": 1000},
                     headers=token).text
    ).body

    # if groups_["response_metadata"]["next_cursor"]:
    #     new_page = get_next_page(groups_["response_metadata"], "private_channel", "channels")
    #     groups_['channels'].extend(new_page)
    groups_ = groups_['channels']
    print(f"Found {len(groups_)} Private Channels")
    _ = "-" * 60
    print(_)
    for i in range(len(groups_)):
        groups_[i]["members"] = Response(
            requests.get("https://slack.com/api/conversations.members",
                         params={"channel": groups_[i]["id"]},
                         headers=token).text
        ).body["members"]
        txt = f"Retrieved members of {groups_[i]['name']}"
        print(f"{txt}{' ' * (60 - len(txt))}|")
    print(_)

    print("")

    return groups_


def get_next_page(page_token, types, resp_channel):
    response = []
    while 1:
        data = Response(
            requests.get(f"https://slack.com/api/conversations.list?types={types}&cursor={page_token['next_cursor'][:-1] + '%3D'}",
                         # params={"types": types, "cursor": page_token["next_cursor"][:-1] + "%3D"},
                         headers=token).text
        ).body
        if data.get("channels"):
            response.extend(data["channels"])
        if data.get("error"):
            continue
        if not data["response_metadata"]["next_cursor"]:
            break
        try:
            page_token = data["response_metadata"]
        except:
            continue
        sleep(1)
    return response


def get_public_channel():
    channels_ = Response(
        requests.get("https://slack.com/api/conversations.list",
                     params={"types": "public_channel", "limit": 1000},
                     headers=token).text
    ).body

    print(f"Found {len(channels_['channels'])} public channels")
    sleep(1)

    return channels_["channels"]


def thread_download():
    file_dir = "slack_files-" + owner
    mkdir(file_dir)
    os.chdir(file_dir)

    data = files["im"]
    data.extend(files["group"])
    data.sort(key=lambda x: x["room_name"])
    process_count = 0
    progress_bar_kwargs = dict(prefix='Progress:', suffix="Download Files", length=40)

    def callback(_):
        """Update process count and progress bar."""
        nonlocal process_count
        download_files(_)
        process_count += 1
        print_progress_bar(process_count, len(data), **progress_bar_kwargs)

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_process = {executor.submit(callback, i): i for i in data}
        for future in as_completed(future_to_process):
            process = future_to_process[future]
            try:
                _ = future.result()
            except Exception as e:
                print(f" generated an exception {e}")

    os.chdir('..')

    shutil.move(os.path.join(".", file_dir), os.path.join("../", file_dir))
    print("Complete")




def get_history2(chat_id, token, messages=None):
    if messages is None:
        messages = []
    last_timestamp = 0

    count = 0
    while 1:
        try:
            response = Response(
                requests.get(
                    url=f"https://slack.com/api/conversations.history",
                    params={
                        "channel": chat_id,
                        "latest": last_timestamp,
                        "oldest": 0,
                    },
                    headers=token,
                    proxies=urllib.request.getproxies()
                ).text
            ).body
        except requests.exceptions.ConnectionError as e:
            sleep(1.1)
            # print(e)
            continue
        count += 1
        if response.get("error") == "ratelimited":

            sleep(1.5)
            if count >= 15:
                sleep(7)
            continue
        try:
            messages.extend(response["messages"])
        except:
            continue
        if response["has_more"]:
            last_timestamp = messages[-1]["ts"]
            sleep(1.1)
        else:
            break

    if last_timestamp is not None:
        pass
    messages.sort(key=lambda message: message["ts"])
    return messages


def parse_messages1(room_id, messages, room_type, room_name):
    current_file_date = ""
    current_messages = []
    msg_files = []
    for message in messages:
        if message.get("files"):

            for i in message["files"]:
                # i["url_private"] = i.get("url_private") + f"?t=xoxe-2256472312918-2285466647201-2269788674549-749f8ce2f4ae5f17b05c1e8e956c1e69"
                if not room_type == "channel":
                    i["room_name"] = room_name[room_id]
                else:
                    i["room_name"] = room_id
            msg_files.append([room_type, message["files"]])
        ts = parse_timestamp(message["ts"])
        file_date = "{:%Y-%m-%d}".format(ts)


        if file_date != current_file_date:
            out_file_name = "{room}/{file}.json".format(room=room_id, file=file_date)
            dump_messages(out_file_name, current_messages)
            current_file_date = file_date
            current_messages = []

        current_messages.append(message)
    out_file_name = "{room}/{file}.json".format(room=room_id, file=current_file_date)
    dump_messages(out_file_name, current_messages)
    return msg_files


def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100,
                       fill='█', print_end="\r"):
    """ Print iterations progress.

        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            print_end   - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end=print_end, flush=True)

    if iteration == total:  # Print newline on completion.
        print(flush=True)


def download_files(data):
    if not data.get("created"):
        return
    dir_name = data["room_name"]
    url = data["url_private"]
    mkdir(dir_name)
    
    date = datetime.fromtimestamp(data["created"]).strftime('%Y-%m-%d_%H-%M')
    r = requests.get(url, headers=token, stream=True)

    file_name = "-".join([date, data["name"]])
   
    with open(f"{dir_name}/{file_name}", "wb") as f:
        f.write(r.content)
        f.flush()

def get_room_name():
    for i in dms:
        room_name_by_id[i["id"]] = "-".join(i["members_name"])

    for i in groups:
        room_name_by_id[i["name"]] = i["name"]


def finalize():
    os.chdir("..")

    if not zip_:
        shutil.make_archive(directory_name, "zip", directory_name, None)
        shutil.rmtree(directory_name, ignore_errors=True)
    end = datetime.now() - start
    print("")
    print("-" * 30)
    print(str(end.total_seconds()) + " total time run")
    print("-" * 30)
    exit()


# if platform.system() == "Windows":
def init_token(user_token):
    global token
    token = user_token


if __name__ == "__main__":
    t = 0
    start = datetime.now()
    parser = argparse.ArgumentParser(description="Export Slack history")
    parser.add_argument(
        "-t", "--token", required=True, help="Slack API OAuth User Token"
    )
    parser.add_argument("-nz", "--no_zip", help="Don't convert to zip", action="store_true")
    parser.add_argument("-r", help="export reminders to file", action="store_true")
    parser.add_argument("-p", help="Export public channels", action="store_true")
    parser.add_argument("-d", action="store_true", help="For download files to computer from chats")

    args = parser.parse_args()
    token = {"Authorization": "Bearer " + args.token}

    user_names_by_id = {}
    user_id_by_names = {}
    room_name_by_id = {}
    files = {"im": [], "group": [], "channel": []}
    chat_name = {"group": [], 'im': [], "channel": []}
    gr = []

    zip_ = args.no_zip
    download = args.d

    test = test_auth()
    owner = test["user"]
    owner_id = test["user_id"]
    channels = get_public_channel() if args.p else []
    dms = get_dms()
    users = get_users()
    get_user_map()
    groups = get_private_channel()

    directory_name = f"{owner}-slack_export"
    mkdir(directory_name)
    os.chdir(directory_name)
    a = os.getcwd()
    dump_channel()
    dump_users()
    get_room_name()


    def multi_process_fetch(func, arr, msg, room_type, user_token, room_name):
        workers = cpu_count()

        process_count = 0
        result = []
        progress_bar_kwargs = dict(prefix='Progress:', suffix=msg, length=40)
        print(msg)

        def callback(_):
            """Update process count and progress bar."""
            nonlocal process_count
            process_count += 1
            print_progress_bar(process_count, len(arr), **progress_bar_kwargs)

        with Pool(workers, init_token(user_token)) as p:
            for i in arr:
                data = p.apply_async(func, [i, dict(user_token), room_name], {}, callback)
                result.append(data)
            while result:
                for i in result:

                    if i.get()[1]:
                        for x in i.get()[1]:
                            for c in x:
                                files[c[0]].extend(c[1])
                    chat_name[room_type].extend(i.get()[0])

                result = [r for r in result if not r.ready()]

        print("")
        print(f"Complete {msg}")


    if args.p:
        multi_process_fetch(fetch_public_channels, channels, "Fetching Public Channel", "channel", token,
                            room_name_by_id)
        # fetch_public_channels_sync()
        sleep(1)
    multi_process_fetch(fetch_dms, dms, "Fetching 1:1 DMs", "im", token, room_name_by_id)
    # fetch_dms_sync()
    sleep(1)
    multi_process_fetch(fetch_groups, groups, "Fetching Private Channel", "group", token, room_name_by_id)
    # fetch_groups_sync()
    if args.r:
        get_reminder()
    dump_ids()
    dump_files()
    if download:
        thread_download()

    finalize()
