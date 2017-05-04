import pandas as pd
import numpy as np


def sdk_page_source_analysis(df=pd.DataFrame):

    newdf = df.groupby(["org_name"])["SDK_platform_view", "SDK_complete_click"].agg([np.sum])
    newdf = newdf.assign(install_rate=newdf["SDK_complete_click"] / newdf["SDK_platform_view"])

    qualified_account = newdf[newdf.install_rate > 0].reset_index()
    qualified_account.columns = ["org_name", "SDK_platform_view", "SDK_complete_click", "install_rate"]
    qualified_account_info = pd.merge(df, qualified_account, how="inner", on=["org_name"])
    qualified_account_info.loc[:, "page_source_cat"] = [page.split("/")[-1] for page in qualified_account_info["page_source"]]

    qualified_account_info_pc = qualified_account_info.groupby("page_source_cat")["page_source_cat"].agg("count")
    # print(qualified_account_info_pc.sort_values(ascending=False))

    qualified_account_by_industry = qualified_account_info.groupby(["industry"])["org_name"].agg("count")
    # print(qualified_account_by_industry.sort_values(ascending=False))

    non_qualified_accounts = newdf[newdf.install_rate == 0].reset_index()
    non_qualified_accounts.columns = ["org_name", "SDK_platform_view", "SDK_complete_click", "install_rate"]
    non_qualified_accounts_info = pd.merge(df, non_qualified_accounts, how="inner", on=["org_name"])

    non_qualified_accounts_by_industry = non_qualified_accounts_info.groupby(["industry"])["org_name"].agg("count")
    # print(non_qualified_accounts_by_industry.sort_values(ascending=False))


def sdk_company_analysis(df=pd.DataFrame, org_in_db=pd.DataFrame):

    df = df.groupby(["org_name"])[["user", "page_source", "SDK_platform_view", "SDK_complete_click"]].agg([len, np.sum]).reset_index()
    df["install_rate"] = df["SDK_complete_click"]["sum"] / df["SDK_platform_view"]["sum"]

    qualified_account = df[(df.install_rate <= 1) & (df.install_rate > 0)]
    qualified_accounts = len(qualified_account)
    avg_users_in_qa = qualified_account["user"]["len"].mean()

    non_qualified_account = df[df.install_rate == 0]
    non_qualified_accounts = len(non_qualified_account)
    avg_users_in_nqa = non_qualified_account["user"]["len"].mean()

    print("Number of qualified accounts :" + str(qualified_accounts))
    print("Number of non qualified accounts :" + str(non_qualified_accounts))
    print("Ratio : between qualified and non qualified :" + str(round(qualified_accounts / non_qualified_accounts, 3)*100) + "%")
    print("Install Rate :" + str(round(qualified_accounts / len(df), 3)*100) + "%")

    print("Avg number of users in qualified accounts : " + str(round(avg_users_in_qa, 2)))
    print("Avg number of users in non qualified accounts : " + str(round(avg_users_in_nqa, 2)))

    qualified_account_1 = df[(df.install_rate == 1)]
    qualified_account_1s = len(qualified_account_1)
    avg_user_in_1qa = qualified_account_1["user"]["len"].mean()
    std_users_in_1qa = qualified_account_1["user"]["len"].std()

    qualified_account_m = df[(df.install_rate < 1) & (df.install_rate > 0)]
    qualified_account_ms = len(qualified_account_m)
    avg_users_in_mqa = qualified_account_m["user"]["len"].mean()
    # std_users_in_mqa = qualified_account_m["user"]["len"].std()

    print("Number of account User-Install-Rate == 1 : " + str(qualified_account_1s) + " Percentage : " + str(round(qualified_account_1s / qualified_accounts, 3)*100) + "%")
    print("Number of account User-Install-Rate < 1 : " + str(qualified_account_ms) + " Percentage : " + str(round(qualified_account_ms / qualified_accounts, 3)*100) + "%")
    print("Avg user on account with UIR == 1 : " + str(avg_user_in_1qa))
    print("Avg user on account with UIR < 1 : " + str(avg_users_in_mqa))

    qualified_account_org_name = qualified_account["org_name"].drop_duplicates()
    non_qualified_account_org_name = non_qualified_account["org_name"].drop_duplicates()

    q_duplicate_rate = round(len(qualified_account_org_name[qualified_account_org_name.isin(org_in_db)]) / len(qualified_account_org_name), 3)*100
    nq_duplicate_rate = round(len(non_qualified_account_org_name[non_qualified_account_org_name.isin(org_in_db)]) / len(non_qualified_account_org_name), 3)*100

    print("Qualified Account Duplicate Rate : " + str(q_duplicate_rate) + "%")
    print("Non-qualified Account Duplicate Rate : " + str(nq_duplicate_rate) + "%")


if __name__ == "__main__":

    # file_name = "SDK选择平台-过去14天.csv"
    # sdk_install_users = pd.read_csv(file_name, encoding="utf-16", sep="\t")

    behavior_file_name = "用户加载SDK.csv"
    columns = ["date", "user", "page_source", "page", "org_name", "industry", "SDK_platform_view", "SDK_check_click", "SDK_complete_click"]
    users_behaviors = pd.read_csv(behavior_file_name, encoding="utf-16", sep="\t", names=columns, header=0)

    user_org_info_file = "user_org_info.txt"

    user_org_info = pd.read_csv(user_org_info_file, header=0)
    org_in_db =  user_org_info["org_name"].drop_duplicates()

    # sdk_page_source_analysis(users_behaviors)
    sdk_company_analysis(users_behaviors, org_in_db=org_in_db)

    # users_ = users_behaviors[(users_behaviors.SDK_platform_view > 0) & (users_behaviors.SDK_check_click > 0)]

    # print(users_behaviors)

    # events = ["SDK_platform_view", "SDK_check_click", "SDK_complete_click"]


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












