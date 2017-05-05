import pandas as pd


def sdk_company_analysis(df=pd.DataFrame):

    nqa_proj = df[(df.first_date_of_getting_pv.isnull()) & (df["SDK_complete_click"] == 0)]
    qa_proj = df[(~df.first_date_of_getting_pv.isnull()) & (df["SDK_complete_click"] != 0)]

    qa_num = len(set(qa_proj["project_id"]) - (set(qa_proj["project_id"]) & set(nqa_proj["org_id_organization"])))
    nqa_num = len(set(nqa_proj["project_id"]))

    print("Qualified Account : " + str(qa_num))
    print("None Qualified Account : " + str(nqa_num))
    print("Install Rate : " + str(round(qa_num / nqa_num, 3) * 100) + "%")
    print("\n")

    nqau = set(qa_proj["user"])
    qau = set(nqa_proj["user"])

    print("Users in qualified account : " + str(len(qau)))
    print("Users in non qualified account : " + str(len(nqau)))
    print("\n")

    avg_qau_org = round(len(qau) / qa_num, 3)
    avg_nqau_org = round(len(nqau) / nqa_num, 3)

    print("Avg users in QA : " + str(round(avg_qau_org, 3)))
    print("Avg users in NQA: " + str(round(avg_nqau_org, 3)))
    print("\n")


def funnel_report(sample=pd.DataFrame, steps=[]):

    initial_users = set(sample["user"])
    result = []
    overlap_users = initial_users
    for e in steps:
        eu = sample[sample[e] > 0]["user"]
        overlap_users = (set(overlap_users) & set(eu))
        result.append(len(overlap_users))

    for step_i in range(len(steps)):
        print("Step " + str(step_i + 1) + " " + steps[step_i] + " : " + str(result[step_i]))
        if step_i < len(steps) - 1:
            cr = round(result[step_i + 1] / result[step_i], 3)* 100
            print("Conversion Rate : " + str(cr) + "%")
    print("\n")

if __name__ == "__main__":

    behavior_file_name = "SDK 安装－数据源.csv"
    columns = ["date", "user", "page_source", "SDK_platform_view", "SDK_platform_click", "SDK_check_click", "SDK_complete_click"]
    users_behaviors = pd.read_csv(behavior_file_name, encoding="utf-16", sep="\t", names=columns, header=0, dtype={ "user" :str})
    users_behaviors = users_behaviors[~users_behaviors["user"].isnull()]
    users_behaviors["page_source"] = users_behaviors["page_source"].map(lambda source: source.split("/")[-1])

    user_org_project_info = pd.read_csv("./user_org_project_info.csv", header=0, dtype={"user_id_project":str})
    users_in_march = pd.merge(users_behaviors, user_org_project_info, how="left", left_on=["user"], right_on=["user_id_project"])
    sdk_company_analysis(users_in_march)
    # print(users_in_march.groupby("page_source")["user"].agg("count").sort_values(ascending=False))

    events = ["SDK_platform_view", "SDK_platform_click", "SDK_check_click", "SDK_complete_click"]

    print("Project Non Activation Funnel")
    sample_con = (users_in_march.first_date_of_getting_pv.isnull()) & (users_in_march["SDK_complete_click"] == 0)
    funnel_report(sample=users_in_march[sample_con], steps=events)

    print("Project Activation Funnel")
    sample_con = (~users_in_march.first_date_of_getting_pv.isnull()) & (users_in_march["SDK_complete_click"] > 0)
    funnel_report(sample=users_in_march[sample_con], steps=events)
