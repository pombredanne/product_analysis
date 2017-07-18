import pandas as pd
import matplotlib.pyplot as plt
import  re
from functools import reduce
from simulator import user_simulator
from pyroaring import BitMap
from pandas import HDFStore , read_hdf


session_behavior = ["avg_duration","visits"]
view_event = ["chart_all_pv", "chart_list_pv", "create_chart_pv", "edit_chart_pv",
                  "dashboard_all_pv", "create_dashboard_pv", "edit_dashboard_pv","dashboard_list_pv",
                  "funnel_all_pv", "retention_all_pv", "user_details_pv", "realtime_pv","heatmap_use_imp",
                  "create_funnel_pv", "scene_list_pv", "scene_all_pv"]

click_event = ["dashboard_filter_clck", "dashboard_usercohort_clck",
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

interactive_expl_sum_key = ["user_details_pv", "retention_detail_behavior_clck", "funnel_time_clck","funnel_trend_clck","funnel_Dimension_clck",
"chart_list_time_clck","chart_list_filter_clck","chart_detail_filter_clck", "chart_detail_time_clck","chart_detail_usercohort_clck",
"heatmap_use_imp", "retention_time_clck","retention_Dimension_clck", "dashboard_usercohort_clck","dashboard_filter_clck","dashboard_time_clck","scene_time_clck","scene_filter_clck","scene_usercohort_clck"]

computed_fields = ["consumption_pv_sum", "interactive_action_sum",
                   "single_diagram_view", "funnel_report_view",
                   "analytics_dashboard", "visual_analytic", "net_income", "capital_expenditure"]

behavior_names = [ session_behavior, view_event, click_event ]

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
            dfs.append(pd.read_csv(files[file_i], encoding="utf-16", sep="\t", dtype={"user": str}, names=df_names, header=0,
                        parse_dates=['Date'], infer_datetime_format=True, low_memory=False))

    result = reduce(lambda left, right: pd.merge(left, right, how="left", on=["user", "Date"]), dfs).fillna(0)

    print("Start to Calculate Financial Model")

    result["consumption_pv_sum"] = result["create_chart_pv"] + result["create_dashboard_pv"] + result["edit_chart_pv"] + result["create_funnel_pv"] + result["edit_dashboard_pv"]
    result["interactive_action_sum"] = result["user_details_pv"]+result["retention_detail_behavior_clck"]+result["funnel_time_clck"]\
                                       +result["funnel_trend_clck"]+result["funnel_Dimension_clck"]+result["chart_list_time_clck"]+result["chart_list_filter_clck"]\
                                       +result["chart_detail_filter_clck"]+result["chart_detail_time_clck"]+result["chart_detail_usercohort_clck"]+result["heatmap_use_imp"]\
                                       +result["retention_time_clck"]+result["retention_Dimension_clck"]+result["dashboard_usercohort_clck"]+result["dashboard_filter_clck"]+result["dashboard_time_clck"]
    result["single_diagram_view"] = result["chart_all_pv"] - result["chart_list_pv"] - result["create_chart_pv"]-result["edit_chart_pv"]
    result["funnel_report_view"] = result["funnel_all_pv"] - result["create_funnel_pv"]

    result["analytics_dashboard"] = result["dashboard_all_pv"]-result["realtime_pv"]-result["create_dashboard_pv"]-result["edit_dashboard_pv"]-result["dashboard_list_pv"]

    result["visual_analytic"] = result["single_diagram_view"] + result["funnel_report_view"] + result["analytics_dashboard"] + result["retention_all_pv"] + result["scene_all_pv"] - result["scene_list_pv"]
    result["net_income"] = result["visual_analytic"]
    result["capital_expenditure"] = -1 * result["consumption_pv_sum"]

    return result


def user_generator(sim_user_filter=None,user_org_filter=None,user_max_id=None, startDate="", endDate="" ):
    return user_simulator(start_date=startDate,end_date=endDate, period=1, file_name=sim_user_filter, user_max_id=user_max_id )


def cohort_analysis(endP=None, sample=None, init_behavior=None,return_behavior=None, number=True,need_user_id=False):

    cohorts_user = []
    cohorts_num  = []
    cohorts = []
    overview = []
    periods = endP + 1

    for i in range(1, periods-1):

        overlap_tseries = []
        overlap_num  = []
        cohort_tmp = []

        for j in range( i + 1, periods):
            cohort_init   = BitMap(sample[(sample["week_iso"] == i) & (sample["visits"] > 0)]["user_id"].astype(int))
            cohort_return = BitMap(sample[(sample["week_iso"] == j) & (sample["visits"] > 0)]["user_id"].astype(int))

            overlap_users = list(cohort_init & cohort_return)

            overlap_user_num = len(overlap_users)
            cohort_init_num = len(cohort_init)

            overlap_tseries.append(overlap_users)
            overlap_num.append(overlap_user_num)
            cohort_tmp.append(cohort_init_num)

        cohorts_user.append(overlap_tseries)
        cohorts_num.append(overlap_num)
        cohorts.append(cohort_tmp)

    cohorts = pd.DataFrame(data=cohorts, columns=range(1, periods-1), index=range(1, periods-1))
    cohorts_num_df = pd.DataFrame(data=cohorts_num, columns=range(1, periods-1), index=range(1, periods-1))

    for i in range(1, periods-1):
        overview.append(round(cohorts_num_df[i].sum()*100/cohorts[i].sum(), 2))

    cohort_table =  pd.concat([cohorts[1].rename("sample size"), round(cohorts_num_df*100/cohorts, 2)], axis=1)

    return cohort_table


def get_tableau_raw_data(user_src=pd.DataFrame,behavior_src=pd.DataFrame):

    columns = session_behavior + view_event + click_event + computed_fields
    result = pd.merge(user_src, behavior_src, how="left", left_on=["user_id", "sim_date"], right_on=["user", "Date"])

    print("Behavior Data Completed")
    result[columns] = result[columns].fillna(0)

    return result


def get_tableau_raw_data_from_source(files=[], user_max_id=None, ffile="", simStartDate="", simEndDate=""):

    behavioral_data = behavior_data_generator(files=files, key=["Date", "user"])
    print("Behavior Data Generation Completed")

    users = user_generator(sim_user_filter=ffile, user_max_id=user_max_id, startDate=simStartDate, endDate=simEndDate)
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


def save_data(data=pd.DataFrame, hdfs=True, dir=""):

    import datetime

    file_name = "raw_data_" + datetime.datetime.now().strftime("%y%m%d")

    data.to_csv(dir + "/" + file_name + ".csv", encoding="utf-8")
    print("Data saved as csv already")

    if hdfs:
        hdf = HDFStore(file_name + ".h5")
        hdf.put(file_name, result, format="table", data_columns=True, encoding="utf-8")
        hdf.close()
        print("Data saved as HDF already")
    else:
        pass


def user_migration(sample, endP):

    periods = endP + 1
    casual_tmp = []
    active_tmp = []
    login_tmp = []

    for i in range(1, periods):

        active_user_init = BitMap(get_active_user(sample[(sample["week_iso"] == i -1 ) & (sample["visits"] > 0)])["user_id"].astype(int))
        casual_user_init = BitMap(get_casual_user(sample[(sample["week_iso"] == i -1) & (sample["visits"] > 0)])["user_id"].astype(int))
        active_user_return = BitMap(get_active_user(sample[(sample["week_iso"] == i) & (sample["visits"] > 0)])["user_id"].astype(int))
        casual_user_return = BitMap(get_casual_user(sample[(sample["week_iso"] == i) & (sample["visits"] > 0)])["user_id"].astype(int))
        login_user_init = BitMap(sample[(sample["week_iso"] == i -1) & (sample["visits"] > 0) & (~sample["first_date_of_getting_pv"].isnull()) ]["user_id"].astype(int))
        login_user_return = BitMap(sample[(sample["week_iso"] == i) & (sample["visits"] > 0) & (~sample["first_date_of_getting_pv"].isnull()) ]["user_id"].astype(int))

        active_init_num = len(active_user_init)
        active_return_num = len(active_user_return)
        active_down_to_casual_num = len((casual_user_return & active_user_init))
        active_remain_num = len((active_user_init & active_user_return))

        casual_init_num = len(casual_user_init)
        casual_return_num = len(casual_user_return)
        casual_up_to_active_num = len((active_user_return & casual_user_init))
        casual_remain_num = len((casual_user_init & casual_user_return))

        login_only_init = login_user_init - casual_user_init - active_user_init
        login_only_return = login_user_return - casual_user_return - active_user_return

        casual_down_to_login = (login_only_return & casual_user_init)
        active_down_to_login = (login_only_return & active_user_init)

        login_remain_num = len((login_only_return & login_only_init))
        login_to_active_num = len((active_user_return & login_only_init))
        login_to_casual_num = len((casual_user_return & login_only_init))


        try:
            casual_tmp.append([len(login_only_return), casual_init_num, casual_return_num, casual_remain_num, casual_up_to_active_num, len(casual_down_to_login),
                               round(casual_up_to_active_num*100/casual_init_num, 2) ,round(len(casual_down_to_login)*100 / casual_init_num, 2), round(casual_remain_num*100 / casual_init_num, 2),
                               round((casual_init_num - casual_remain_num - casual_up_to_active_num)*100 / casual_init_num, 2 )])
            active_tmp.append([len(login_only_return), active_init_num, active_return_num, active_remain_num, active_down_to_casual_num, len(active_down_to_login),
                               round(active_down_to_casual_num*100/active_init_num, 2),round(len(active_down_to_login)*100 / active_init_num, 2), round(active_remain_num*100 / active_init_num, 2),
                               round((active_init_num - active_remain_num - active_down_to_casual_num)*100 / active_init_num, 2)])
            login_tmp.append([len(login_only_init), len(login_only_return), login_remain_num, login_to_casual_num, login_to_active_num,
                              round(login_to_active_num*100/len(login_only_init), 2), round(login_to_casual_num*100/ len(login_only_init), 2), round(login_remain_num*100/ len(login_only_init), 2)])

        except:
            print("Something goes wrong")

    cu = pd.DataFrame( data=casual_tmp, columns=["login_only", "last", "current", "remain", "up_to_active", "down_to_login", "up_to_active_per", "down_to_login_per",  "remain_per", "drop_per"], index=range(2, periods))
    au = pd.DataFrame( data=active_tmp, columns=["login_only", "last", "current", "remain", "down_to_casual", "down_to_login",  "down_to_casual_per", "down_to_login_per", "remain_per", "drop_per"], index=range(2, periods))
    lu = pd.DataFrame( data=login_tmp, columns=["last", "current", "remain", "up_to_casual", "up_to_active",  "up_to_active_per", "up_to_casual_per", "remain_per"], index=range(2, periods))

    print(cu)
    print(au)
    print(lu)
    # cu[["up", "remain", "drop"]].plot(title="Casual User Migration")
    # au[["down", "remain", "drop"]].plot(title="Active User Migration")
    #
    # plt.show()


if __name__ == "__main__":
    gio_files = ["./week_data/0717/20170710-20170716_user_访问量&访问时长.csv",
                 "./week_data/0717/20170710-20170716_FQY_主要功能数据_U_user_table_PV浏览类.csv",
                 "./week_data/0717/20170710-20170716_FQY_主要功能数据_U_user_table_action交互类_old.csv"]

    user_project_org_file = "./0717/user_project_org_info.csv"

    user_max_id = 76155

    # result = get_tableau_raw_data_from_source(files=gio_files, user_max_id=user_max_id, ffile=user_project_org_file, simStartDate="2017/7/10", simEndDate="2017/7/17")

    # save_data(data=result, dir="./week_data/0717")
    # result = read_hdf("./0702/raw_data_170705.h5", "raw_data_170705.h5")

    result = pd.read_csv("./week_data/all_0717_week.csv")

    user_migration(sample=result, endP=27)



    # active_user = get_active_user(result)
    # casual_user = get_casual_user(result)
    # login_user = get_login_user(result)

    # print(len(login_user))

    # cohort_analysis(endP=26, sample=login_user, number=False)
    # cohort_analysis(endP=6, sample=login_user, number=False)

    # print("DON'T BE PANICK. DATA ARE PREPARED")
    #
    # active_user = active_user[active_user.days_get_pv > 270 & active_user.days_get_pv < 360 ]
    # casual_user = casual_user[casual_user.days_get_pv > 270 & casual_user.days_get_pv < 360 ]
    # core_user = core_user[core_user.days_get_pv > 270 & core_user.days_get_pv < 360 ]
    #
    # WN = 26
    #
    # i = 0
    # for user in [ core_user, active_user, casual_user ]:
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
    #     i += 1
    #     print("\n")
    # print("FUCK ! I AM DONE OF IT")