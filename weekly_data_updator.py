import xlsxwriter
import pandas as pd
from xlsxwriter.utility import xl_rowcol_to_cell
from datetime import datetime
from user_use_behavior import get_active_user
from user_use_behavior import get_casual_user
from user_use_behavior import cohort_analysis
from user_use_behavior import get_login_user
from user_use_behavior import user_migration
from user_use_behavior import get_short_return_of_asset

raw = pd.read_csv("./week_data/all_0717_week.csv")
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

au_1Wreten = pd.Series([0]).append(cohort_analysis(sample=get_active_user(raw), endP=cWN -1)[1])
cu_1Wreten = pd.Series([0]).append(cohort_analysis(sample=get_casual_user(raw), endP=cWN -1)[1])

"""
    1 Week Migration ( Active User, Casual User )
"""

cu, au, lu = user_migration(login_user, endP=cWN -1)

cu_up_rate = pd.Series([0]).append(cu["up_to_active"])
au_down_rate = pd.Series([0]).append(au["down_to_casual"])

"""
 new user 1 week retention 
"""
new_user_1w = pd.Series([0]).append(cohort_analysis(sample=login_user[login_user.user_life < 90], endP=cWN -1)[1].fillna(0))

"""
 Total Time Spent

"""

tts = login_user.assign(total_time_spent=lambda x : round(x.avg_duration.astype(float) * x.visits.astype(int),2) ).groupby("week_iso")["total_time_spent"].sum()

workbook = xlsxwriter.Workbook("weekly_report_WN" + str(cWN)  + ".xlsx")
worksheet = workbook.add_worksheet()

metrics = [ short_return_of_asset, active_users, casual_users,
            au_1Wreten, cu_1Wreten, cu_up_rate, au_down_rate, new_user_1w, tts ]

metrics_name = ["Short Return of Equity ", "Weekly Active User", "Weekly Casual User",
                "Active User 1 Week Retention Rate", "Casual User 1 Week Retention Rate",
                "Casual User 1 Week Up Rate", "Active User 1 Week Down Rate", "New User 1 Week Retention Rate", " Weekly Total Time Spent (min)"]

rid = 55

worksheet.write_row("B" + str(rid), range(1, cWN))
worksheet.write(rid-1, 0, "week number")

for mid in range(len(metrics)):
    worksheet.write(rid, 0, str(metrics_name[mid]))
    worksheet.set_column(firstcol=0, lastcol=0, width=18)
    worksheet.write_row("B" + str(rid+1), metrics[mid])

    chart = workbook.add_chart({"type": "line"})
    chart.set_title({"name":metrics_name[mid] })

    cell = xl_rowcol_to_cell(rid, cWN-1)

    chart.add_series({'values': '=Sheet1!$B$' + str(rid+1) + ":" + cell})
    chart.set_x_axis({'reverse': True})
    chart.set_legend({'none': True})

    rowl = ( mid // 3 ) * 17
    coll = ( mid % 3 ) * 9

    worksheet.insert_chart( row=rowl + 1, col=coll + 1, chart=chart)

    rid += 1

workbook.close()