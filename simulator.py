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

def user_simulator(start_date=None,end_date=None,period=None,user_file=None):

    if type(end_date) != str or type(start_date) != str :
        end_date   = str(end_date)
        start_date = str(start_date)
    else:
        pass

    users = [ user_id for user_id in product(user_id_generator(1,60000),days_generator(start_date,end_date,period)) ]
    users_df = pd.DataFrame(users,columns=["user_id","sim_date"])

    user_info_df = pd.read_csv("user_join_org_info_raw.csv", encoding="utf-8",
                            parse_dates=["min_day_user_join_org", "update_date"], dtype={"user_id":str}).drop_duplicates("user_id")

    result = pd.merge(users_df, user_info_df, how="left", on="user_id")

    return result.loc[result["sim_date"] >= result["min_day_user_join_org"]]


if __name__ == "__main__":

    users = user_simulator("2017/1/1",datetime.now(),period=1,user_file="user_join_org_info_raw.csv")

    # print(users.loc[users["user_id"] == "51160"])

    # users.to_csv("sim_users-201-1-1-2017-4-11.csv")
    #
    # print("done")
