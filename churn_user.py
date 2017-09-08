import pandas as pd
import matplotlib.pyplot as plt
from user_use_behavior import get_active_user
from pyroaring import BitMap

raw = pd.read_csv("/Users/apple/Desktop/Sophia/week_data2/raw_data_170731.csv", header=0, index_col=0, parse_dates=["user_created_at"])

weeks = list(range(5, 31))

week_criteria = 4
#
# dormant_users_W = []
#
# for week in weeks:
#     user_visit =  raw[(raw["week_iso"] >= (week-week_criteria)) & ( raw["week_iso"] <= week ) ]
#     result = user_visit.groupby(["user_id"])["visits"].sum()
#     churn_num = len(result[(result == 0)])
#     dormant_users_W.append(churn_num)
#
# result = pd.Series(data=dormant_users_W, index=weeks)
#
# result.plot()
# plt.show()

from datetime import datetime
from dateutil import parser

reactivate_users = []

for week in weeks:
    dormant_raw = raw[(raw["week_iso"] >= (week-week_criteria)) &
                      (raw["week_iso"] < week) &
                      (raw["user_created_at"] <  datetime.strptime("2017-W" + str(week-week_criteria) + "-1", "%Y-W%W-%w" ).date())]


    user_week_visits_sum = dormant_raw.groupby(["user_id"])["visits"].agg("sum")


    dormant_users = BitMap(user_week_visits_sum[user_week_visits_sum == 0].index.values)

    activated_user = BitMap(get_active_user(raw[(raw["week_iso"] == week)])["user_id"].astype("int"))

    reactivated_users = activated_user & dormant_users

    reactivated_user_num = len(reactivated_users)

    reactivate_users.append(reactivated_user_num)


result = pd.DataFrame(data=reactivate_users, index=weeks)

print(result)


result.plot()
plt.show()



