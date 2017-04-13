import pandas as pd
import numpy as np
from functools import reduce
from simulator import user_simulator
import  re
import time




session_behavior = ["avg_duration","visits"]
view_event = ["chart_all_pv","chart_list_pv","create_chart_pv","edit_chart_pv",
                  "dashboard_all_pv","create_dashboard_pv","edit_dashboard_pv","dashboard_list_pv",
                  "funnel_all_pv","retention_all_pv","user_details_pv","realtime_pv","heatmap_use_imp",
                  "create_funnel_pv"]
click_event = ["dashboard_filter_clck","dashboard_usercohort_clck",
                      "dashboard_time_clck","dashboard_create_save_clck",
                      "dashboard_edit_update_clck",
                      "chart_detail_usercohort_clck","chart_detail_filter_clck",
                      "chart_detail_time_clck","chart_create_save_clck",
                      "chart_edit_savetoanother_clck","chart_edit_update_clck",
                      "chart_list_filter_clck","chart_list_time_clck",
                      "funnel_Dimension_clck","funnel_time_clck",
                      "funnel_trend_clck","funnel_create_save_clck",
                      "retention_time_clck","retention_Dimension_clck",
                      "retention_detail_behavior_clck"]

behavior_names = [session_behavior,view_event,click_event]

def secs_convertor(time=None):
    try:
        time_array = re.compile("(.*?)分(.*?)秒").findall(str(time))
    except:
        print(time)

    if len(time_array) > 0:
        result = int(time_array[0][0]) * 60 + int(time_array[0][1])
    else:
        result = 0

    return result


def behavior_data_generator(files=[],key=[]):

    if len(files) == 0:
        return []
    else:

        dfs = []
        for file_i in range(len(files)):
            df_names =  key + behavior_names[file_i]
            dfs.append(pd.read_csv(files[file_i], encoding="utf-16", sep="\t", dtype={"user":str},names=df_names,header=0,parse_dates=['Date'],infer_datetime_format=True))

    result = reduce(lambda left, right: pd.merge(left, right, how="left", on=["user", "Date"]), dfs).fillna(0)
    result["avg_duration"] = [ secs_convertor(d) for d in result["avg_duration"] ]
    return result

def user_generator(sim_user_filter=None,user_org_filter=None):

    users_sim = user_simulator("2017/1/1","2017/4/9", period=1, file_name=sim_user_filter)
    user_org_info_df = pd.read_csv(user_org_filter, dtype="str")

    return pd.merge(users_sim, user_org_info_df, how="inner", on="user_id")

def cohort_analysis(periods=None, sample=None, init_behavior=None,return_behavior=None, number=True,need_user_id=False):

    cohorts_user = []
    cohorts_num  = []
    cohorts_per  = []

    for i in range(1,periods-1):

        overlap_tseries = []
        overlap_num  = []
        overlap_per  = []

        for j in range( i + 1, periods):
            cohort_init   = set(sample[ ( sample["week_iso"] == i ) & ( sample["visits"] > 0 ) ]["user_id"])
            cohort_return = set(sample[ ( sample["week_iso"] == j ) & ( sample["visits"] > 0 ) ]["user_id"])

            overlap_users = list(cohort_init & cohort_return)

            overlap_tseries.append(overlap_users)
            overlap_num.append(len(overlap_users))
            overlap_per.append(round(len(overlap_users)/len(cohort_init)*100,2) )

        cohorts_user.append(overlap_tseries)
        cohorts_num.append(overlap_num)
        cohorts_per.append(overlap_per)

    if not number:
        results = ( cohorts_per,  cohorts_user )
    else:
        results = ( cohorts_num , cohorts_user )

    return results



if __name__ == "__main__":
    gio_files = ["./0412/0101-0409 user_访问量&访问时长.csv",
                 "./0412/0101-0409 FQY_主要功能数据_U_user_table_PV浏览类.csv",
                 "./0412/0101-0409 FQY_主要功能数据_U_user_table_action交互类.csv"]

    behavioral_data = behavior_data_generator(files=gio_files,key=["Date","user"])
    users = user_generator(sim_user_filter="user_join_org_info_raw.csv",user_org_filter="user_org_info.txt")


    columns = session_behavior + view_event + click_event
    result = pd.merge(users,behavioral_data, how="left", left_on=["user_id","sim_date"],right_on=["user","Date"])
    result[columns] = result[columns].fillna(0)

    print("DON'T BE PANICK. DATA ARE PREPARED")

    WN = 10
    t1 = time.time()
    report_per , report_users =  cohort_analysis(periods=WN,sample=result,number=False)
    t2 = time.time()

    print(t2 - t1)

    for line in report_per:
        s = ""
        for j in line:
            s += str(j) + "% "
        print(s)


    # behavioral_data.to_csv("./0412_result/user_behavior.csv")
    # users.to_csv("./0412_result/user_sim.csv")
    # result.to_csv("./0412_result/result.csv")
    #
    #
    # print("FUCK ! I AM DONE OF IT")
