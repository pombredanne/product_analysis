# -*- coding: utf-8 -*- 
import pandas as pd
import numpy as np 
from functools import reduce
from simulator import user_simulator
from datetime import datetime


def to_secs( duration_string=None ):
    if duration_string == "0":
        return 0
    else:
        return int(duration_string.split("分")[0])*60 + int(duration_string.split("分")[1].split("秒")[0])



user_data_sim_df = pd.read_csv("./user_data_all_sim.txt",dtype={"user_id" : str, "year" : np.int64, "week_num":np.int64})

file_names = {
    "user_session"     : "1201-0330 user_访问量&访问时长.csv",
    "user_org_info"    : "user_org_info.txt",
    "user_behavior_view"    : "1228-0327 FQY_主要功能数据_U_user_table_PV浏览类.txt",
    "user_behavior_click"   : "1228-0327 FQY_主要功能数据_U_user_table_action交互类.txt"
}

parse_dates = ['Date']


user_org_info_df = pd.read_csv(file_names["user_org_info"],dtype="str")
# user_sessions_df  = pd.read_csv(file_names["user_session"],encoding="utf-16",sep="\t",dtype={"user" : str, "访问时长 (分钟)" : str, "访问量":np.int64}, parse_dates=parse_dates)

session_name = ["Date","user","duration_min","visits"]
user_sessions_df = pd.read_csv("./1201-0330 user_访问量&访问时长.csv",dtype={"user":str},parse_dates=parse_dates,names=session_name,header=0)
# user_sessions_df["year"] = user_sessions_df["Date"].dt.year
# user_sessions_df["week_num"] = user_sessions_df["Date"].dt.week


view_name = ["Date","user","chart_all_pv","chart_list_pv","create_chart_pv","edit_chart_pv",
                  "dashboard_all_pv","create_dashboard_pv","edit_dashboard_pv","dashboard_list_pv",
                  "funnel_all_pv","retention_all_pv","user_details_pv","realtime_pv","heatmap_use_imp",
                  "create_funnel_pv"]

user_behavior_view_df = pd.read_csv(file_names["user_behavior_view"],encoding="utf-8",sep="\t",dtype={'user':str},parse_dates=parse_dates,names=view_name,header=0)

click_name = ["Date","user","dashboard_filter_clck","dashboard_usercohort_clck",
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

user_behavior_click_df = pd.read_csv(file_names["user_behavior_click"],sep="\t",encoding="utf-8",dtype={'user':str}, parse_dates=parse_dates,names=click_name,header=0)


dfs = [user_sessions_df, user_behavior_view_df, user_behavior_click_df]
user_behaviors = reduce(lambda left, right: pd.merge(left,right,how="left", on=["user","Date"]),dfs).fillna(0)

users_sim = user_simulator("2017/1/1",datetime.now(),period=1,user_file="user_join_org_info_raw.csv")


result = pd.merge(users_sim,user_behaviors,how="left", left_on=["user_id","sim_date"],right_on=["user","Date"] )



# # print(result.columns.values)
user_behaviors.to_csv("user_behaviors.csv")
result.to_csv("result.csv")
#
# # print(result)
#
print("done")


