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
from email_service import send_emil

raw = pd.read_csv("./week_data/all_0724_week.csv")
raw = raw[(raw["year_iso"] == 2017)]

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

cu_up_rate = cu["up_to_active_per"]
au_down_rate =au["down_to_casual_per"]

"""
 new user 1 week retention 
"""
new_user_1w = cohort_analysis(sample=login_user[login_user.user_life < 90], endP=cWN -1)[1].fillna(0)

"""
 Total Time Spent

"""

tts = login_user.assign(total_time_spent=lambda x : round(x.avg_duration.astype(float) * x.visits.astype(int),2) ).groupby("week_iso")["total_time_spent"].sum()

workbook = xlsxwriter.Workbook("weekly_report_WN" + str(cWN)  + ".xlsx")
worksheet = workbook.add_worksheet()

metrics_ = [
    {
        "name" : "Weekly Short Return of Equity",
        "y_ax_label" : "",
        "x_ax_label" : "Week",
        "data" : short_return_of_asset
    },
    {
        "name" : "Weekly Active User",
        "y_ax_label" : "user",
        "x_ax_label" : "Week",
        "data" : active_users
    },
    {
        "name" : "Weekly Casual User",
        "y_ax_label" : "user",
        "x_ax_label" : "Week",
        "data" : casual_users
    },
    {
        "name" : "Active User 1 Week Retention Rate",
        "y_ax_label" : "Percentage of Active User ( % )",
        "x_ax_label" : "Week",
        "data" : au_1Wreten
    },
    {
        "name" : "Casual User 1 Week Retention Rate",
        "y_ax_label" : "Percentage of Casual User ( % )",
        "x_ax_label" : "Week",
        "data" : cu_1Wreten
    },
    {
        "name" : "Casual User 1 Week Up Rate",
        "y_ax_label" : "Percentage of Casual User ( % )",
        "x_ax_label" : "Week",
        "data" : cu_up_rate
    },
    {
        "name" : "Active User 1 Week Down Rate",
        "y_ax_label" : "Percentage of Active User ( % )",
        "x_ax_label" : "Week",
        "data" : au_down_rate
    },
    {
        "name" : "New User 1 Week Retention Rate",
        "y_ax_label" : "Percentage of User ( % )",
        "x_ax_label" : "Week",
        "data" : new_user_1w
    },
    {
        "name" : "Weekly Total Time Spent",
        "y_ax_label" : "mins",
        "x_ax_label" : "Week",
        "data" : tts
    }
]

rid = 55

week_nums = pd.Series(data=list(range(1, cWN)), index=reversed(range(1, cWN))).apply(lambda wn: datetime.strptime("2017-W" + str(wn) + "-1", "%Y-W%W-%w" ).date())

date_formate = workbook.add_format({'num_format': 'dd/mm/yyyy'})

for wn in range(len(week_nums)):
    worksheet.write_datetime( xl_rowcol_to_cell(rid -1, wn+1), week_nums[wn +1 ], date_formate )

worksheet.write(rid-1, 0, "week number")

wnrid = rid

for mid in range(len(metrics_)):
    worksheet.write(rid, 0, str(metrics_[mid]["name"]))
    worksheet.set_column(firstcol=0, lastcol=0, width=18)
    worksheet.write_row("B" + str(rid+1), metrics_[mid]["data"].sort_index(ascending=False))

    chart = workbook.add_chart({"type": "line"})
    chart.set_title({"name":metrics_[mid]["name"] })

    cell = xl_rowcol_to_cell(rid, cWN-1)

    chart.add_series({'values': '=Sheet1!$B$' + str(rid+1) + ":" + cell,
                      "categories" : "=Sheet1!$B$55:$AD$55" })
    chart.set_size({
        'width' : 620,
        'height' : 320
    })

    chart.set_x_axis({
        'date_axis' : True,
        'name' : metrics_[mid]["x_ax_label"]
    })

    chart.set_y_axis({
        'name' : metrics_[mid]["y_ax_label"]
    })

    rowl = ( mid // 3 ) * 17
    coll = ( mid % 3 ) * 10

    chart.set_legend({'none': True})
    worksheet.insert_chart( row=rowl + 1, col=coll + 1, chart=chart)

    rid += 1

workbook.close()
