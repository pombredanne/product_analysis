import xlsxwriter
import pandas as pd
from datetime import datetime
from user_use_behavior import get_active_user
from user_use_behavior import get_casual_user
from user_use_behavior import cohort_analysis
from user_use_behavior import get_login_user
from user_use_behavior import user_migration
from user_use_behavior import get_short_return_of_asset
from user_use_behavior import get_short_return_of_equity

raw = pd.read_csv("./DB/all_0717.csv")
raw = raw[(raw["week_iso"] < 30)]

cWN = datetime.now().isocalendar()[1]

login_user = get_login_user(raw)

"""
Product Value 

No1  Short return of asset ( net_income / capital_expenditure )

Short return of equity ( sum(net_income) / countd(active user )

"""

# short_return_of_equity = login_user.groupby("week_iso").apply(get_short_return_of_equity)
short_return_of_asset = login_user.groupby("week_iso").apply(get_short_return_of_asset)


"""
    User growth ( Active User, Casual User )
"""

active_users = get_active_user(login_user).groupby("week_iso")["user_id"].nunique()
casual_users = get_casual_user(login_user).groupby("week_iso")["user_id"].nunique()


"""
    1 Week Retention ( Active User, Casual User )
"""

au_1Wreten = cohort_analysis(sample=get_active_user(raw), endP=cWN -1)[1]
cu_1Wreten = cohort_analysis(sample=get_casual_user(raw), endP=cWN -1)[1]

"""
    1 Week Migration ( Active User, Casual User )
"""

cu, au, lu = user_migration(login_user, endP=cWN -1)

cu_up_rate = cu["up_to_active"]
au_down_rate = au["down_to_casual"]

"""
 new user 1 week retention 
"""
new_user_1w = cohort_analysis(sample= login_user[login_user.user_life < 90], endP=cWN -1)[1].fillna(0)

"""
 Total Time Spent

"""

tts = login_user.assign(total_time_spent=lambda x : round(x.avg_duration.astype(float) * x.visits.astype(int),2) ).groupby("week_iso")["total_time_spent"].sum()

workbook = xlsxwriter.Workbook("weekly_report_WN27" + ".xlsx")
worksheet = workbook.add_worksheet()

metrics = [ short_return_of_asset, active_users, casual_users,
            au_1Wreten, cu_1Wreten, cu_up_rate, au_down_rate, new_user_1w, tts ]

metrics_name = ["Short ROE", "Active User", "Casual User",
                "Active User 1 WR", "Casual User 1 WR",
                "Casual User Up Rate", "Active User Down Rate", "New User 1 WR", "Total Time Spent"]

rid = 50
chrA = 65

worksheet.write_row("B" + str(rid), reversed(range(1, cWN)))
worksheet.write(rid, 0, "week number")

for mid in range(len(metrics)):
    worksheet.write(rid, 0, str(metrics_name[mid]))
    worksheet.set_column(firstcol=0, lastcol=0, width=18)
    worksheet.write_row("B" + str(rid), metrics[mid].sort_index(ascending=False) )

    chart = workbook.add_chart({"type": "line"})
    chart.add_series({'values': '=Sheet1!$Z$' + str(rid) + ":$B$" + str(rid)})

    rowl = ( mid // 3 ) * 15
    coll = ( mid % 3 ) * 10

    worksheet.insert_chart( row=rowl + 1, col=coll + 1, chart=chart)

    rid += 1
    chrA += 1

workbook.close()