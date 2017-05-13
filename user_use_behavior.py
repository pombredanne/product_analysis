import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from functools import reduce
from simulator import user_simulator
import  re

session_behavior = ["avg_duration","visits"]
view_event = ["chart_all_pv","chart_list_pv","create_chart_pv","edit_chart_pv",
                  "dashboard_all_pv","create_dashboard_pv","edit_dashboard_pv","dashboard_list_pv",
                  "funnel_all_pv","retention_all_pv","user_details_pv","realtime_pv","heatmap_use_imp",
                  "create_funnel_pv","scene_list_pv","scene_all_pv"]

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
                      "retention_detail_behavior_clck","scene_time_clck","scene_filter_clck","scene_usercohort_clck"]

interactive_expl_sum_key = ["user_details_pv","retention_detail_behavior_clck",
"funnel_time_clck","funnel_trend_clck","funnel_Dimension_clck",
"chart_list_time_clck","chart_list_filter_clck","chart_detail_filter_clck",
"chart_detail_time_clck","chart_detail_usercohort_clck",
"heatmap_use_imp",
"retention_time_clck","retention_Dimension_clck",
"dashboard_usercohort_clck","dashboard_filter_clck","dashboard_time_clck","scene_time_clck","scene_filter_clck","scene_usercohort_clck"]

computed_fields = ["consumption_pv_sum","interactive_action_sum",
                   "single_diagram_view","funnel_report_view",
                   "analytics_dashboard","visual_analytic","net_income","capital_expenditure"]


behavior_names = [session_behavior, view_event, click_event]

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
            print("Start to parse file: " + files[file_i])
            dfs.append(pd.read_csv(files[file_i], encoding="utf-16", sep="\t", dtype={"user":str}, names=df_names,header=0,parse_dates=['Date'],infer_datetime_format=True, low_memory=False))

    result = reduce(lambda left, right: pd.merge(left, right, how="left", on=["user", "Date"]), dfs)
    result = result.fillna(0)

    result["avg_duration"] = result["avg_duration"].map(lambda time : secs_convertor(time))

    result["consumption_pv_sum"] = result["create_chart_pv"] + result["create_dashboard_pv"] + result["edit_chart_pv"] + result["create_funnel_pv"] + result["edit_dashboard_pv"]
    result["interactive_action_sum"] = result["user_details_pv"]+result["retention_detail_behavior_clck"]+result["funnel_time_clck"]+result["funnel_trend_clck"]+result["funnel_Dimension_clck"]+result["chart_list_time_clck"]+result["chart_list_filter_clck"]+result["chart_detail_filter_clck"]+result["chart_detail_time_clck"]+result["chart_detail_usercohort_clck"]+result["heatmap_use_imp"]+result["retention_time_clck"]+result["retention_Dimension_clck"]+result["dashboard_usercohort_clck"]+result["dashboard_filter_clck"]+result["dashboard_time_clck"]
    result["single_diagram_view"] = result["chart_all_pv"] - result["chart_list_pv"] - result["create_chart_pv"]-result["edit_chart_pv"]
    result["funnel_report_view"] = result["funnel_all_pv"] - result["create_funnel_pv"]

    result["analytics_dashboard"] = result["dashboard_all_pv"]-result["realtime_pv"]-result["create_dashboard_pv"]-result["edit_dashboard_pv"]-result["dashboard_list_pv"]

    result["visual_analytic"] = result["single_diagram_view"] + result["funnel_report_view"] + result["analytics_dashboard"] + result["retention_all_pv"] + result["scene_all_pv"] - result["scene_list_pv"]
    result["net_income"] = result["visual_analytic"]
    result["capital_expenditure"] = -1 * result["consumption_pv_sum"]


    return result

def user_generator(sim_user_filter=None,user_org_filter=None,user_max_id=None ):
    return user_simulator("2016/12/1","2017/5/8", period=1, file_name=sim_user_filter, user_max_id=user_max_id )


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
            overlap_per.append(round(len(overlap_users)/len(cohort_init)*100, 2))

        cohorts_user.append(overlap_tseries)
        cohorts_num.append(overlap_num)
        cohorts_per.append(overlap_per)

    if not number:
        cohorts_obj = ( cohorts_per,  cohorts_user )
    else:
        cohorts_obj = ( cohorts_num , cohorts_user )

    print("Cohort Analysis Completed")

    return cohorts_obj

def get_tableau_raw_data(user_src=pd.DataFrame,behavior_src=pd.DataFrame):

    columns = session_behavior + view_event + click_event + computed_fields
    result = pd.merge(user_src, behavior_src, how="left", left_on=["user_id", "sim_date"], right_on=["user", "Date"])

    print("Behavior Data Completed")
    result[columns] = result[columns].fillna(0)

    return result

def get_tableau_raw_data_from_source(files=[], user_max_id=None):


    behavioral_data = behavior_data_generator(files=files, key=["Date", "user"])
    print("Behavior Data Generation Completed")

    users = user_generator(sim_user_filter="./0508/user_project_org_info.csv", user_max_id=user_max_id)
    print("User Data Generation Completed")

    columns = session_behavior + view_event + click_event + computed_fields
    result = pd.merge(users, behavioral_data, how="left", left_on=["user_id", "sim_date"], right_on=["user", "Date"])
    print("Merging User Data and Behavior Data Completed")

    result[columns] = result[columns].fillna(0)

    return result


def get_core_user(sample=pd.DataFrame):
    return sample[(sample["visual_analytic"] > 0) & (sample["interactive_action_sum"] > 0)  & (sample["consumption_pv_sum"] > 0 )]

def get_active_user(sample=pd.DataFrame):
    return sample[(sample["visual_analytic"] > 0) & (sample["interactive_action_sum"] > 0)  | (sample["consumption_pv_sum"] > 0 )]

def get_casual_user(sample=pd.DataFrame):
    return sample[(sample["visual_analytic"] > 0) & (sample["interactive_action_sum"] == 0) & (sample["consumption_pv_sum"] == 0 )]

def get_login_user(sample=pd.DataFrame):
    return sample[(sample["visits"] > 0)]


def get_metrics_columns_name():
    columns =  session_behavior + view_event + click_event + computed_fields
    return columns


if __name__ == "__main__":
    gio_files = ["./0508/20161201-20170508_user_访问量&访问时长.csv",
                 "./0508/20161201-20170508_FQY_主要功能数据_U_user_table_PV浏览类.csv",
                 "./0508/20161201-20170508_FQY_主要功能数据_U_user_table_action交互类.csv"]

    user_max_id = 66093


    result = get_tableau_raw_data_from_source(files=gio_files, user_max_id=user_max_id)

    result["weekday"] =  result["sim_date"].map(lambda time: time.isoweekday())


    dashboard = result[result["analytics_dashboard"] > 0].groupby(["weekday"])["user_id_project"].agg("count")
    chart = result[result["single_diagram_view"] > 0].groupby(["weekday"])["user_id_project"].agg("count")
    funnel = result[result["funnel_report_view"] > 0].groupby(["weekday"])["user_id_project"].agg("count")
    retention = result[result["retention_all_pv"] > 0].groupby(["weekday"])["user_id_project"].agg("count")
    inter = result[result["interactive_action_sum"] > 0].groupby(["weekday"])["user_id_project"].agg("count")
    visual = result[result["visual_analytic"] > 0].groupby(["weekday"])["user_id_project"].agg("count")

    fig, axes = plt.subplots(nrows=2, ncols=3)
    dashboard.plot(ax=axes[0, 0]); axes[0, 0].set_title("Dashboard", fontsize=15)
    chart.plot(ax=axes[0, 1]); axes[0, 1].set_title("Chart", fontsize=15)
    visual.plot(ax=axes[0,2]); axes[0, 2].set_title("Visual Analytics", fontsize=15)
    retention.plot(ax=axes[1,0]); axes[1, 0].set_title("Retention", fontsize=15)
    inter.plot(ax=axes[1,1]); axes[1, 1].set_title("Interactive Analytics", fontsize=15)
    visual.plot(ax=axes[1,2]); axes[1, 2].set_title("Visual Analytics", fontsize=15)

    plt.show()

    # print("Complete generating raw data")
    #
    # result.to_csv("./0508/raw_data_0508.csv",encoding="utf-8")

    #
    # behavioral_data = behavior_data_generator(files=gio_files,key=["Date","user"])
    #
    # behavioral_data.to_csv("behavior_data.csv",encoding="utf-8")


    # users = user_generator(sim_user_filter="user_join_org_info_raw.csv",user_org_filter="user_org_info.txt")
    #
    # result = get_tableau_raw_data(user_src=users,behavior_src=behavioral_data)

    # core_user   = get_core_user(result)
    # active_user = get_active_user(result)
    # casual_user = get_casual_user(result)
    #
    #
    # print("DON'T BE PANICK. DATA ARE PREPARED")
    #
    # WN = 11
    # i = 0
    #
    # for user in [ core_user , active_user, casual_user ]:
    #
    #     if i == 0:
    #         print("Core User Retention Report")
    #     elif i == 1:
    #         print("Active User Retention Report")
    #     else:
    #         print("Casual User Retention Report")
    #
    #     report_per , report_users =  cohort_analysis(periods=WN,sample=user,number=False)
    #
    #     for line in report_per:
    #         s = ""
    #         for j in line:
    #             s += str(j) + "% "
    #         print(s)
    #
    #     i += 1
    #
    #     print("\n")
    #


    # behavioral_data.to_csv("./0412_result/user_behavior.csv")
    # users.to_csv("./0412_result/user_sim.csv")
    # result.to_csv("./0412_result/result.csv")
    #
    #
    # print("FUCK ! I AM DONE OF IT")
