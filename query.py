import hashlib
import hmac
import time
import requests
import json
import datetime
import pandas as pd
from threading import Timer
import pytz
from raw_process import utc_to_local


def authToken(secret, project, ai, tm):
    message = ("POST\n/auth/token\nproject=" + project + "&ai=" + ai + "&tm=" + tm).encode('utf-8')
    signature = hmac.new(bytes(secret.encode('utf-8')), bytes(message), digestmod=hashlib.sha256).hexdigest()
    return signature


def get_code(prkey, pid, ai, tm, a):
    headers = {"X-Client-Id": prkey}
    r_code = requests.post(
        "https://gta.growingio.com/auth/token" + "?ai=" + ai + "&project=" + pid + "&tm=" + tm + "&auth=" + a,
        headers=headers)
    return json.loads(r_code.text)["code"]


def query_request(chart_id, startDate, endDate, prkey, code, interval):
    headers = {"X-Client-Id": prkey, "Authorization": code}
    url =  "https://gta.growingio.com/projects/nxog09md/charts/" + chart_id + ".json" + "?startTime=" + startDate + "&endTime=" + endDate + "&interval=" + interval
    return requests.get(url=url, headers=headers).json()


def scheduled_query(pkey, prkey, ai, pid,  start, end, sources, interval ):

    tm = str(int(time.time() * 1000))

    sig = authToken(secret=pkey, project=pid, ai=ai, tm=tm)
    code = get_code(prkey=prkey, pid=pid, ai=ai, tm=tm, a=sig)

    for source in sources:
        result = query_request(source["chart_id"], start, end, prkey, code, interval)
        names = result["name"]
        data =  pd.DataFrame(data=result["data"], columns=source["cols"])
        data = data.assign(Date=data["Date"].apply(lambda x: datetime.datetime.fromtimestamp(int(x) / 1000, tz=pytz.utc).astimezone(pytz.timezone("Asia/Shanghai")).date()))
        print(data)


if __name__ == "__main__":

    pkey = "9dahxn76jr9ghlrfequiua8cflnlwlxfy8krtl3s"
    prkey = "7f8it37dxdt91x4n5cvuvccc1cgaqe22"
    ai = "0a1b4118dd954ec3bcc69da5138bdb96"
    pid = "nxog09md"
    tm = str(int(time.time() * 1000))
    mins = 0.5

    view_col = ["Date", "user","chart_all_pv", "chart_list_pv", "create_chart_pv", "edit_chart_pv",
                  "dashboard_all_pv", "create_dashboard_pv", "edit_dashboard_pv","dashboard_list_pv",
                  "funnel_all_pv", "retention_all_pv", "user_details_pv", "realtime_pv","heatmap_use_imp",
                  "create_funnel_pv", "scene_list_pv", "scene_all_pv" ]

    click_col =  ["Date", "user", "dashboard_filter_clck", "dashboard_usercohort_clck",
                      "dashboard_time_clck", "dashboard_create_save_clck",
                      "dashboard_edit_update_clck",
                      "chart_detail_usercohort_clck", "chart_detail_filter_clck",
                      "chart_detail_time_clck", "chart_create_save_clck",
                      "chart_edit_savetoanother_clck", "chart_edit_update_clck",
                      "chart_list_filter_clck", "chart_list_time_clck",
                      "funnel_Dimension_clck", "funnel_time_clck",
                      "funnel_trend_clck", "funnel_create_save_clck",
                      "retention_time_clck", "retention_Dimension_clck",
                      "retention_detail_behavior_clck", "scene_time_clck", "scene_filter_clck", "scene_usercohort_clck"]

    session_col = ["Date", "user", "avg_duration", "visits"]

    sources =[
        {
            "name" : "user_访问量&访问时长",
            "chart_id" : "a9a5KAE9",
            "cols" : session_col
        },
        {
            "name" : "FQY_主要功能数据_U_user_table_action交互类_old",
            "chart_id" : "4PKq5AD9",
            "cols" : click_col
        },
        {
            "name" : "FQY_主要功能数据_U_user_table_PV浏览类",
            "chart_id" : "j9yDBAZo",
            "cols" : view_col
        }
    ]


    sig = authToken(secret=pkey, project=pid, ai=ai, tm=tm)
    code = get_code(prkey=prkey, pid=pid, ai=ai, tm=tm, a=sig)
    day_interval = "86400000"
    week_interval = "604800000"

    endDate = str(int(datetime.datetime(2017, 7, 8).timestamp() * 1000))
    startDate = str(int(datetime.datetime(2017, 7, 1).timestamp() * 1000))

    for source in sources:
        try:
            query_request(source["chart_id"], startDate, endDate, prkey, code, day_interval)
            Timer(15, requests.Session.close)
        except:
            print("Somthing goes wrong")

    t = Timer(mins * 60, scheduled_query, [pkey, prkey, ai, pid, startDate, endDate, sources, day_interval], )
    t.start()
