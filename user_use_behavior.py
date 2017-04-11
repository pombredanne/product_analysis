import pandas as pd
import numpy as np
from functools import reduce
from simulator import user_simulator
from datetime import datetime
import time



session_behavior = ["duration_min","visits"]
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

def behavior_data_generator(files=[],key=[]):

    if len(files) == 0:
        return []
    else:

        dfs = []
        for file_i in range(len(files)):
            df_names =  key + behavior_names[file_i]
            dfs.append(pd.read_csv(files[file_i], encoding="utf-16", sep="\t", dtype="str",names=df_names,header=0,parse_dates=['Date']))

        return reduce(lambda left, right: pd.merge(left, right, how="left", on=["user", "Date"]), dfs).fillna(0)


def user_generator(sim_user_filter=None,user_org_filter=None):

    users_sim = user_simulator("2017/1/1", datetime.now(), period=1, file_name=sim_user_filter)
    user_org_info_df = pd.read_csv(user_org_filter, dtype="str")

    return pd.merge(users_sim, user_org_info_df, how="inner", on="user_id")



if __name__ == "__main__":

    gio_files = ["0110-0409 user_访问量&访问时长.csv","1201-0409 FQY_主要功能数据_U_user_table_PV浏览类.csv","0110-0409 FQY_主要功能数据_U_user_table_action交互类.csv"]

    behavioral_data = behavior_data_generator(files=gio_files,key=["Date","user"])
    users = user_generator(sim_user_filter="user_join_org_info_raw.csv",user_org_filter="user_org_info.txt")



    result = pd.merge(users,behavioral_data,left_on=["user_id","sim_date"],right_on=["user","Date"])
    columns = session_behavior + view_event + click_event
    result[columns] = result[columns].fillna(0)




