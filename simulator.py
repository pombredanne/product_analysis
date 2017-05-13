import numpy as np
import pandas as pd
from dateutil import parser
from itertools import product
from datetime import datetime


def days_generator(start_date=None,end_date=None,period=1):
    start = parser.parse(start_date)
    end   = parser.parse(end_date)
    return pd.date_range(start,end,freq=pd.tseries.offsets.DateOffset(days=period))

def week_number_convertor(tseries):
    return [ time.isocalendar()[1] for time in tseries ]

def user_id_generator(start_user_id=1,end_user_id=None):
    return [ str(user_id) for user_id in np.arange(start_user_id,end_user_id+ 1) ]

def user_simulator(start_date=None,end_date=None,period=None,file_name=None,user_max_id=None):

    print("Start to simulate users")

    if type(end_date) != str or type(start_date) != str :
        end_date   = str(end_date)
        start_date = str(start_date)
    else:
        pass

    users_df = pd.DataFrame([ user_id for user_id in product(user_id_generator(1,user_max_id), days_generator(start_date,end_date,period)) ],
                            columns=["user_id","sim_date"])

    user_info_df = pd.read_csv(file_name, encoding="utf-8",
                            parse_dates=["org_created_at", "project_created_at", "first_date_of_getting_pv", "latest_date_of_getting_pv"],
                               dtype={"user_id_project":str, "user_id_user":str, "user_id_organization" : str},
                               low_memory=False).drop_duplicates("user_id_project")

    print("Start adding user information")

    result = pd.merge(users_df, user_info_df, how="left", left_on=["user_id"], right_on=["user_id_project"])
    result["year_iso"] = result["sim_date"].map(lambda time : time.isocalendar()[0])
    result["week_iso"] = result["sim_date"].map(lambda time: time.isocalendar()[1])
    result["weekday_iso"] = result["sim_date"].map(lambda time : time.isocalendar()[2])
    result["hour"] = result["sim_date"].map(lambda time : time.hour)

    print("Start remove non-exisit date of users ")

    return result[result["sim_date"] >= result["org_created_at"]]


if __name__ == "__main__":

    start_date = "2017/1/1"
    end_date   = "2017/4/9"
    user_max_id = 64369


    users = user_simulator(start_date,end_date,period=1,file_name="./0502/user_org_info.csv",user_max_id=user_max_id)
    users.to_csv("user_sim_v2" + ".csv")

