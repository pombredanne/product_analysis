import hashlib
import hmac
import time
import requests
import json
import datetime
import pandas as pd
from threading import Timer
import pytz
from user_use_behavior import get_tableau_raw_data_from_api


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
    url =  "https://gta.growingio.com/projects/nxog09md/charts/" + chart_id + ".json" + "?startTime=" + str(int(startDate.timestamp() *1000)) + "&endTime=" + str(int(endDate.timestamp() *1000)) + "&interval=" + interval
    return requests.get(url=url, headers=headers).json()


def scheduled_query(pkey, prkey, ai, pid, sources, ffile, umid):

    import os

    sds = str(datetime.datetime.now().isocalendar()[0]) + "-W" + str(datetime.datetime.now().isocalendar()[1]) + "-1"
    start_date = datetime.datetime.strptime(sds, "%Y-W%W-%w").strftime("%m%d")

    rdir = "week_data/" + start_date
    if not os.path.exists(rdir):
        os.makedirs(rdir)

    tm = str(int(time.time() * 1000))

    sig = authToken(secret=pkey, project=pid, ai=ai, tm=tm)
    code = get_code(prkey=prkey, pid=pid, ai=ai, tm=tm, a=sig)

    dfs = []

    for source in sources:
        result = query_request(source["chart_id"], source["start"], source["end"], prkey, code, source["interval"])
        data =  pd.DataFrame(data=result["data"], columns=source["cols"])
        data = data.assign(Date=data["Date"].apply(lambda x: datetime.datetime.fromtimestamp(int(x) / 1000, tz=pytz.utc).astimezone(pytz.timezone("Asia/Shanghai")).date()))
        dfs.append(data)

    result = get_tableau_raw_data_from_api(data=dfs, user_max_id=umid, ffile=ffile,  simStartDate=sources[0]["start"],simEndDate=sources[0]["end"] )

    file_name = "raw_data_" + start_date +".csv"
    result.to_csv(rdir + "/" + file_name, index=False)
    print("File " + file_name + " saved")



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

    day_interval = "86400000"
    week_interval = "604800000"

    interval = week_interval

    startDate = datetime.datetime(2017, 7, 17)
    endDate = datetime.datetime(2017, 7, 23)
    umid = 77685
    ffile = "./week_data/0724/user_project_org_info.csv"

    sources = [
        {
            "name" : "user_访问量&访问时长",
            "chart_id" : "a9a5KAE9",
            "cols" : session_col,
            "interval" : interval,
            "start" : startDate,
            "end" : endDate
        },
        {
            "name" : "FQY_主要功能数据_U_user_table_action交互类_old",
            "chart_id" : "4PKq5AD9",
            "cols" : click_col,
            "interval" : interval,
            "start" : startDate,
            "end" : endDate
        },
        {
            "name" : "FQY_主要功能数据_U_user_table_PV浏览类",
            "chart_id" : "j9yDBAZo",
            "cols" : view_col,
            "interval" : interval,
            "start" : startDate,
            "end" : endDate
        }
    ]

    sig = authToken(secret=pkey, project=pid, ai=ai, tm=tm)
    code = get_code(prkey=prkey, pid=pid, ai=ai, tm=tm, a=sig)

    for source in sources:
        try:
            query_request(source["chart_id"], source["start"], source["end"], prkey, code, source["interval"])
            Timer(15, requests.Session.close)
        except:
            print("Somthing goes wrong")

    t = Timer(mins * 60, scheduled_query, [pkey, prkey, ai, pid, sources, ffile, umid])
    t.start()
