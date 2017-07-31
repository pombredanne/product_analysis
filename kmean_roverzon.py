import pandas as pd
# from raw_process import get_col_dtype
#
# coldtype, ddtype  = get_col_dtype()
#
# raw = pd.read_csv("./week_data/all_0724_week.csv", header=0, parse_dates=ddtype, dtype=coldtype, index_col=0)
#
# raw = pd.DataFrame(data=raw[raw["pay_status"] == "已付费"])
#
# print(raw)
#
# from user_use_behavior import get_active_user
# from user_use_behavior import get_casual_user
# from user_use_behavior import cohort_analysis
# from user_use_behavior import get_login_user
# from user_use_behavior import get_short_return_of_asset
# dataset = []
#
# from datetime import datetime
#
# orgs = raw["org_id_organization"].drop_duplicates().sort_values()
#
# cWN = datetime.now().isocalendar()[1]
#
# for org in orgs:
#     data = raw[raw["org_id_organization"] == org ]
#     org_name = data["org_name"].values.tolist()[0]
#     org_indsutry = data["industry"].values.tolist()[0]
#
#     active_users_num = get_active_user(data)["user_id"].nunique()
#     casual_users_num = get_casual_user(data)["user_id"].nunique()
#     # short_return_of_asset_mean = get_login_user(data).groupby("week_iso").apply(get_short_return_of_asset).mean()
#     au_1Wreten = cohort_analysis(sample=get_active_user(data), endP=cWN - 1)[1].mean()
#     cu_1Wreten = cohort_analysis(sample=get_casual_user(data), endP=cWN - 1)[1].mean()
#
#     dataset.append([org, org_name, org_indsutry,  active_users_num, casual_users_num, au_1Wreten, cu_1Wreten ])
#
# dataset = pd.DataFrame(data=dataset, columns=["org", "org_name", "org_industry", "active user", "casual user", "au 1w", "cu 1w"])

# dataset.to_csv("./dataset.csv")

import pandas as pd
from sklearn.cluster import KMeans, DBSCAN

dataset = pd.read_csv("./dataset.csv", index_col=0).fillna(0)

# kmean_labels = []

# kmeans = KMeans(n_clusters=4, random_state=i).fit(dataset.ix[:, 4:])
# kmeans = pd.Series(data=kmeans.labels_, name="label" + str(i))
# kmean_labels.append(kmeans)


dbscan = DBSCAN().fit(dataset.ix[:, 4:])
dbscan = pd.Series(data=dbscan.labels_)

dbscan.to_csv("dbscan_label.csv")

# kmeans.to_csv("data_label4.csv")

# org_info = dataset.ix[:, :4]
# result = pd.concat([org_info, kmeans], axis=1)
#
# dis = result.groupby("label")["org"].agg("count")
# dis_per = dis * 100 // dis.sum()
#
# dis_result = pd.concat([dis, dis_per.apply(lambda x : str(x) + "%")], names=["count", "percentage"], axis=1)
#
# print(dis_result)
#
# label_summary = result.groupby(["label", "org_industry"])["org"].agg("count")
#
# print(label_summary)
