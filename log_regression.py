import pandas as pd
import statsmodels.api as sm
import numpy as np




if __name__ == "__main__":

    u_info_date_columns = ["first_date_of_getting_pv", "user_created_at", "project_created_at", "latest_date_of_getting_pv"]
    u_info = pd.read_csv("./0502/user_org_project_info.csv", parse_dates=u_info_date_columns, header=0, dtype={"user_id_project": str})

    user_behavior_columns = ["Date", "user", "page_view", "visit", "bounce_rate", "duration", "page_session"]
    user_behavior = pd.read_csv("./0502/SDK 接入用戶行為數據源_20160601_20161231.csv", encoding="utf-16", sep="\t", names=user_behavior_columns, parse_dates=["Date"], header=0, dtype={"user": str})

    user_behavior = user_behavior[(~user_behavior.user.isnull()) & (user_behavior.user != "null") & (user_behavior.bounce_rate != "null")]
    user_behavior[["bounce_rate", "duration", "page_session"]] = user_behavior[["bounce_rate", "duration", "page_session"]].astype(np.float)

    ugp =  u_info.groupby(["user_id_project"])["project_id"].agg("count")
    ugp = ugp[ugp >= 2]

    print(ugp)

    u_info = pd.merge(u_info, user_behavior, how="inner", left_on=["user_id_project"], right_on=["user"])
    act_proj_ba  = u_info[(~u_info.first_date_of_getting_pv.isnull()) & (u_info.user_created_at < u_info.first_date_of_getting_pv) & (u_info.Date < u_info.first_date_of_getting_pv) & (u_info.user_id_project.isin(ugp.index))]
    non_act_proj = u_info[u_info.first_date_of_getting_pv.isnull()]

    role_tcount_nap = non_act_proj.groupby(["project_id"]).role.nunique()
    role_tcount_ap = act_proj_ba.groupby(["project_id"]).role.nunique()
    user_ap = act_proj_ba.groupby(["project_id"]).user_id_project.nunique()
    user_nap = non_act_proj.groupby(["project_id"]).user_id_project.nunique()

    # print(" Avg. Users before Activating Project : " + str(round(user_ap.mean(),3)))
    # print(" Std Users before Activating Project : " + str(round(user_ap.std(), 3)))
    # print(" Median Users before Activating Project : " + str(round(np.median(user_ap),3)))
    #
    # print(" Avg.  Users in Non-Activated Project : " + str(round(user_nap.mean(),3)))
    # print(" Std Users in None Activating Project : " + str(round(user_nap.std(), 3)))
    # print(" Median Users in Non-Activated Project : " + str(round(np.median(user_nap), 3)))
    # print("\n")
    #
    #
    proejct_users = u_info.groupby(["project_id"]).user_id_project.nunique()
    proejct_roles = u_info.groupby(["project_id"]).role.nunique()
    proejct_install = u_info.groupby(["project_id"]).first()["first_date_of_getting_pv"].apply(lambda x: 0 if pd.isnull(x) else 1)


    proejct_users.ix[proejct_users.index.isin(user_ap.index)] = user_ap
    proejct_roles.ix[proejct_users.index.isin(role_tcount_ap.index)] = role_tcount_ap
    proejct_users_behavior = u_info[["project_id", "page_view", "visit", "bounce_rate", "duration", "page_session"]].groupby(["project_id"]).agg(np.sum)

    projects = pd.concat([proejct_users, proejct_roles, proejct_users_behavior], axis=1)
    projects.columns = ["user_count", "role_type_count", "page_view_sum", "visit_sum", "bounce_rate_sum", "duration_sum", "page_session_sum"]

    logit = sm.Logit(proejct_install, projects)
    result = logit.fit()

    print(result.summary())
    params = result.params

    conf = result.conf_int()
    conf['OR'] = params
    conf.columns = ['2.5%', '97.5%', 'OR']
    print("\n")
    print(np.exp(conf))
