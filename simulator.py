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
    return [ user_id for user_id in np.arange(start_user_id,end_user_id+ 1) ]

def user_simulator(start_date=None,end_date=None,period=None):

    if type(end_date) != str or type(start_date) != str :
        end_date   = str(end_date)
        start_date = str(start_date)
    else:
        pass

    users_iter = product(user_id_generator(1,60000),days_generator(start_date,end_date,period))

    return [ user for user in users_iter ]


if __name__ == "__main__":

    users = user_simulator("2016/1/1",datetime.now(),7)
    users_df = pd.DataFrame(users,columns=["user_id","date"])