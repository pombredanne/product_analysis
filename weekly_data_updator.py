import xlsxwriter
import pandas as pd
import plotly.plotly as py
import plotly.graph_objs as go
import matplotlib.pyplot as plt
from user_use_behavior import get_active_user
from user_use_behavior import get_casual_user
from user_use_behavior import cohort_analysis
from user_use_behavior import get_login_user


raw = pd.read_csv("./DB/all_0710.csv")
raw = raw[raw["week_iso"] < 30]


# active_users_week = get_active_user(raw).groupby("week_iso")["user"].nunique()
# casual_users_week = get_casual_user(raw).groupby("week_iso")["user"].nunique()
login_users_week = get_login_user(raw).groupby("week_iso")["user"].nunique()

login_user = get_login_user(raw)
login_user = login_user.assign(total_time_spent=lambda x : x.avg_duration.astype(float) * x.visits.astype(int))
srdf = login_user.groupby("week_iso")[["net_income", "capital_expenditure"]].agg("sum").assign(short_return=lambda x: -1* x.net_income / x.capital_expenditure)
print(srdf)

# print(login_user.groupby("week_iso")["total_time_spent"].sum())




#
#
# au_reten = cohort_analysis(sample=get_active_user(raw), endP=26)
# cu_reten = cohort_analysis(sample=get_casual_user(raw), endP=26)
#
# users = [active_users_week, casual_users_week, login_users_week]
# reten = [au_reten, cu_reten]
#
# workbook = xlsxwriter.Workbook("weekly_report.xlsx")
#
# worksheet = workbook.add_worksheet("user")
# week_index = active_users_week.index.tolist()
# worksheet.write_column('A1', week_index)
#
# for uid in range(len(users)):
#     cols = ['B', 'C', 'D']
#     titles = ["Active User", "Casual User", "Login User"]
#     worksheet.write_column( cols[uid]+'2', users[uid])
#     worksheet.write(0, 0, "Week Number")
#     worksheet.write(0, uid + 1, titles[uid])
#     chart = workbook.add_chart({'type': 'line'})
#     chart.set_title({'name': titles[uid]})
#     chart.add_series({
#         'values' : '=user!$' + cols[uid] + '2:$' + cols[uid] + '$' + str(len(users[uid]))
#     })
#
#     chart.set_size({'width': 720, 'height': 560})
#     cols2 = ['F', 'R', 'AD']
#     worksheet.insert_chart( cols2[uid] + '2' , chart)
#
# worksheet = workbook.add_worksheet("1_Week_Retention")
# week_index = au_reten.index
# worksheet.write_column('A1', week_index)
#
# for uid in range(len(reten)):
#     cols = ['B', 'C']
#     titles = ["Active User", "Casual User" ]
#     worksheet.write_column( cols[uid]+'2', reten[uid][1])
#     worksheet.write(0,0, "Week Number")
#     worksheet.write(0,uid + 1 , titles[uid])
#     chart = workbook.add_chart({'type': 'line'})
#     chart.set_title({'name': titles[uid]})
#     chart.add_series({
#         'values' : '=1_Week_Retention!$' + cols[uid] + '2:$' + cols[uid] + '$' + str(len(users[uid]))
#     })
#
#     chart.set_size({'width': 720, 'height': 560})
#     cols2 = ['E', 'R', 'Y']
#     worksheet.insert_chart( cols2[uid] + '2' , chart)
#
# workbook.close()
