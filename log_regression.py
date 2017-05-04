import pandas as pd
import statsmodels.api as sm
import numpy as np

date_columns = ["first_date_of_getting_pv", "user_created_at", "project_created_at", "latest_date_of_getting_pv"]
df = pd.read_csv("user_org_project_info.csv", parse_dates=date_columns, header=0, dtype={"user_id_project": str})
df2 = pd.read_csv("SDK 接入用戶行為數據源_20160801-20161231.csv", encoding="utf-16", sep="\t", names=["Date", "user", "page_view", "visit", "bounce_rate", "duration", "page_session"], parse_dates=["Date"], header=0, dtype={"user": str})
# df2 = df2[(~df2.bounce_rate.isnull()) & (~df2.duration.isnull()) & (~df2.page_session.isnull()) ]
df2 = df2[(~df2.user.isnull()) & (df2.user != "null") & (df2.bounce_rate != "null")]
df2["bounce_rate"] = df2["bounce_rate"].astype(np.float)
df2["duration"] = df2["duration"].astype(np.float)
df2["page_session"] = df2["page_session"].astype(np.float)

df = pd.merge(df, df2, how="inner", left_on=["user_id_project"], right_on=["user"])
act_proj_ba = df[(~df.first_date_of_getting_pv.isnull()) & (df.user_created_at < df.first_date_of_getting_pv) & (df.Date < df.first_date_of_getting_pv) ]
non_act_proj = df[df.first_date_of_getting_pv.isnull()]

role_in_non_act_proj = non_act_proj.groupby(["project_id"])["role"].nunique()
role_in_act_proj = act_proj_ba.groupby(["project_id"])["role"].nunique()
users_in_act_proj = act_proj_ba.groupby(["project_id"])["user_id_project"].nunique()

proejct_users = df.groupby(["project_id"]).user_id_project.nunique()
proejct_roles = df.groupby(["project_id"]).role.nunique()
# proejct_orgs  = df.groupby(["project_id"]).org_id_organization.nunique()
proejct_install = df.groupby(["project_id"]).first()["first_date_of_getting_pv"].apply(lambda x: 0 if pd.isnull(x) else 1)
proejct_users.ix[proejct_users.index.isin(users_in_act_proj.index)] = users_in_act_proj
proejct_roles.ix[proejct_users.index.isin(role_in_act_proj.index)] = role_in_act_proj
proejct_users_behavior = df[["project_id", "page_view", "visit", "bounce_rate", "duration", "page_session"]].groupby(["project_id"]).agg(np.sum)

data_set = pd.concat([proejct_users, proejct_roles, proejct_users_behavior], axis=1)
data_set.columns = ["user_count", "role_type_count", "page_view_sum", "visit_sum", "bounce_rate_sum", "duration_sum", "page_session_sum"]

logit = sm.Logit(proejct_install, data_set)

result = logit.fit()

print(result.summary())
params = result.params
conf = result.conf_int()
conf['OR'] = params
conf.columns = ['2.5%', '97.5%', 'OR']
print(np.exp(conf))


