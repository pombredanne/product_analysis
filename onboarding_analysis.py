import pytz
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta
from datetime import timezone
import numpy as np
import simulator
from dateutil import parser
from itertools import product

def non_activation_project(right=pd.DataFrame, left=pd.DataFrame):
    result = pd.merge(right, left, how="left", left_on=["project_id", "user_id_project"],
                      right_on=["project_id", "creator_id"])

    users_in_na_proj = result[result.project_created_at.notnull() & result.first_date_of_getting_pv.isnull() ].reset_index()
    users_in_na_proj = users_in_na_proj[users_in_na_proj.project_created_at > parser.parse("2016/1/1") ]
    users_in_na_proj["year"] = users_in_na_proj["project_created_at"].map(lambda time: time.isocalendar()[0])
    users_in_na_proj["week"] = users_in_na_proj["project_created_at"].map(lambda time: time.isocalendar()[1])


    users_in_na_proj_u_y_w = pd.DataFrame({'count': users_in_na_proj.groupby(["user_id_user", "year", "week"]).size()}).reset_index()

    # print(users_in_na_proj_u_y_w)

    print(users_in_na_proj_u_y_w.describe())
    # print(len(users_in_na_proj_u_y_w[ 2 < users_in_na_proj_u_y_w  ]))
    # print(len(users_in_na_proj_u_y_w[  (1 < users_in_na_proj_u_y_w ) & ( users_in_na_proj_u_y_w < 3) ]))
    # print(len(users_in_na_proj_u_y_w[users_in_na_proj_u_y_w == 1 ]))

def project_activation_rate(right=pd.DataFrame, left=pd.DataFrame):

    result = pd.merge(right,left, how="left",left_on=["project_id","user_id_project"], right_on=["project_id", "creator_id"] )

    projects = result[(result.project_created_at.notnull()  )].drop_duplicates("project_id")
    projects["year"] = projects["project_created_at"].map(lambda time: time.isocalendar()[0])
    projects["week"] = projects["project_created_at"].map(lambda time: time.isocalendar()[1])
    projects["weekday"] = projects["project_created_at"].map(lambda time: time.isocalendar()[2])
    projects["label"] = [ "nonactivated" if  pd.isnull(time) else "activated" for time in projects["first_date_of_getting_pv"] ]

    project_week = projects.groupby(["year","week"])["project_id"].aggregate(len)
    project_week_a = projects[projects.label == "activated"].groupby(["year","week"])["project_id"].aggregate(len)
    project_week_na = projects[projects.label == "nonactivated"].groupby(["year", "week"])["project_id"].aggregate(len)

    project_week_a_r =  round( ( project_week_a / project_week ) * 100 , 2 )
    project_week_na_r = round( ( project_week_na / project_week ) * 100 , 2 )

    project_week_a_r.plot(label="activation rate")
    project_week_na_r.plot(label="non activation rate")

    plt.legend()

    plt.show()

if __name__ == "__main__":
    file_name   = "./0502/user_all_information.csv"
    event_created_file = "./0502/first_event_created.csv"


    date_fields = ["org_created_at","project_created_at","first_date_of_getting_pv","latest_date_of_getting_pv"]

    user_all_info_df = pd.read_csv(file_name,encoding="utf-8",parse_dates=date_fields)
    user_all_info_df = user_all_info_df[user_all_info_df.project_id.notnull()]
    user_all_info_df.sort_values(["project_id"], inplace=True)

    event_created_df = pd.read_csv(event_created_file,encoding="utf-8",parse_dates=["created_at", "min_created_at"])
    event_created_df = event_created_df[event_created_df.project_id.notnull()]
    event_created_df.sort_values(["project_id"],inplace=True)

    project_activation_rate(right=user_all_info_df,left=event_created_df)
    non_activation_project(right=user_all_info_df, left=event_created_df)

    activated_projects = pd.merge(user_all_info_df,event_created_df, how="inner", left_on=["project_id","user_id_project"], right_on=["project_id", "creator_id"])
    #
    # print(activated_projects)

    activated_projects["project_act_delta"] = activated_projects["first_date_of_getting_pv"] - activated_projects["project_created_at"]
    activated_projects["user_act_delta"] = activated_projects["min_created_at"] - activated_projects["first_date_of_getting_pv"]
    activated_projects["user_to_project_delta"] = activated_projects["min_created_at"] - activated_projects["project_created_at"]

    activated_projects = activated_projects[activated_projects.project_created_at > parser.parse("2016/1/1") ]
    activated_projects = activated_projects[( activated_projects["project_act_delta"] >= timedelta(days=-1)  )]
    activated_projects["project_created_weekday"] = activated_projects["project_created_at"].dt.weekday
    # activated_projects = activated_projects[activated_projects.project_created_weekday < 5]

    activated_projects["year"] = activated_projects["project_created_at"].map(lambda time: time.isocalendar()[0])
    activated_projects["week"] = activated_projects["project_created_at"].map(lambda time: time.isocalendar()[1])

    print(activated_projects[["project_act_delta", "user_act_delta", "user_to_project_delta"]].describe())

    activated_projects["project_act_delta_h"] = activated_projects["project_act_delta"].map(lambda time : int(time.total_seconds() / 3600) )
    activated_projects["user_act_delta_h"] = activated_projects["user_act_delta"].map(lambda time: int(time.total_seconds() / 3600))
    activated_projects["user_to_project_delta_h"] = activated_projects["user_to_project_delta"].map(lambda time: int(time.total_seconds() / 3600))


    two_days_secs = 172800
    #
    activated_projects = activated_projects[activated_projects.project_act_delta_h < two_days_secs ]

    activated_projects_y_w = activated_projects.groupby(["year","week"])
    activated_projects_y_w_sum = activated_projects_y_w["user_act_delta_h"].agg([np.mean,np.std,len])

    print(activated_projects_y_w_sum)
    print(activated_projects.sort_values("project_id"))

    activated_projects_y_w_sum["mean"].plot()
    activated_projects_y_w_sum.plot()
    plt.show()


    r = activated_projects["project_act_delta_h"].value_counts().sort_index()
    r2 = activated_projects["user_act_delta_h"].value_counts().sort_index()
    r3 = activated_projects["user_to_project_delta_h"].value_counts().sort_index()

    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    x_major_ticks = np.arange(0, 168, 24)

    ax.set_xticks(x_major_ticks)

    ax.set_xlim(xmin=-2,xmax=168)
    ax.set_ylim(ymin=0, ymax=300)
    plt.plot(r)
    plt.plot(r2, label="tracking_to_act")
    plt.plot(r3, label="tracking_to_project")
    plt.legend()
    plt.grid(True)
    # plt.show()
    #

    local_tz = pytz.timezone("Asia/Shanghai")

    activated_projects["min_created_at_hour"] = activated_projects["min_created_at"].map(lambda time : time.replace(tzinfo=timezone.utc).astimezone(local_tz).hour)
    activated_projects["project_created_at_hour"] = activated_projects["project_created_at"].map(lambda time: time.hour)


    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    x_major_ticks = np.arange(0,24,1)
    ax.set_xticks(x_major_ticks)

    y_major_ticks = np.arange(0, 600, 50)
    ax.set_yticks(y_major_ticks)

    rr = activated_projects["min_created_at_hour"].value_counts().sort_index()
    rr2 = activated_projects["project_created_at_hour"].value_counts().sort_index()

    plt.plot(rr,label="event_created_prob")
    plt.plot(rr2, label="project_created_prob")

    plt.title("First Autotracking Time")
    plt.xlabel("Hour")
    plt.ylabel("Count")
    plt.legend()
    plt.grid(True)
    plt.show()







