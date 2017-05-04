import pandas as pd
import numpy as np
import math


def sdk_company_analysis(df=pd.DataFrame, org_in_db=pd.DataFrame):

    nqa_proj = df[(df.first_date_of_getting_pv.isnull()) & (df["SDK_complete_click"] == 0)]
    qa_proj = df[(~df.first_date_of_getting_pv.isnull()) & (df["SDK_complete_click"] != 0)]

    qa_num = len(set(qa_proj["project_id"]) - (set(qa_proj["project_id"]) & set(nqa_proj["org_id_organization"])))
    nqa_num = len(set(nqa_proj["project_id"]))
    print("Qualified Account : " + str(qa_num))
    print("None Qualified Account : " + str(nqa_num))
    print("Install Rate : " + str(round(qa_num / nqa_num, 3) * 100) + "%")

    nqau = set(qa_proj["user"])
    qau = set(nqa_proj["user"])

    print("Users in qualified account : " + str(len(qau)))
    print("Users in non qualified account : " + str(len(nqau)))

    avg_qau_org = len(qau) / qa_num
    avg_nqau_org = len(nqau) / nqa_num

    print("Avg users in QA : " + str(round(avg_qau_org, 2)))
    print("Avg users in NQA: " + str(round(avg_nqau_org, 2)))


if __name__ == "__main__":

    # file_name = "SDK选择平台-过去14天.csv"
    # sdk_install_users = pd.read_csv(file_name, encoding="utf-16", sep="\t")

    behavior_file_name = "SDK 安装－数据源.csv"
    columns = ["date", "user", "page_source", "SDK_platform_view", "SDK_platform_click", "SDK_check_click", "SDK_complete_click"]
    users_behaviors = pd.read_csv(behavior_file_name, encoding="utf-16", sep="\t", names=columns, header=0, dtype={"user":str})
    users_behaviors = users_behaviors[~users_behaviors["user"].isnull()]

    user_org_project_info = pd.read_csv("./user_org_project_info.csv", header=0, dtype={"user_id_project":str})
    users_in_march = pd.merge(users_behaviors, user_org_project_info, how="left", left_on=["user"], right_on=["user_id_project"])
    sdk_company_analysis(users_in_march)



    ## Funnel Analysis

    # initial_users = users_behaviors["user"]
    # all = []
    # overlap_users = initial_users
    # for e in events:
    #     eu = users_behaviors[users_behaviors[e] > 0]["user"]
    #     overlap_users = (set(overlap_users) & set(eu))
    #     all.append(overlap_users)
    #
    # first_event = len(all[0])
    # second_event = len(all[1])
    # third_event = len(all[2])
    #
    # print("conversion rate : " + str((third_event / first_event) * 100))
    # print(first_event)
    # print("conversion rate : " + str((second_event / first_event) * 100))
    # print(second_event)
    # print("conversion rate : " + str((third_event / second_event) * 100))
    # print(third_event)












