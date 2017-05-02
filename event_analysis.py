import pytz
import pandas as pd
import numpy as np
from datetime import timezone
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit


if __name__=="__main__":
    local_tz = pytz.timezone("Asia/Shanghai")
    metrics_df =  pd.read_csv("metrics_events_table.csv",encoding="utf-8",parse_dates=["created_at"])

    metrics_df["created_at_h"] = metrics_df["created_at"].map(lambda time : time.replace(tzinfo=timezone.utc).astimezone(local_tz).hour)
    metrics_df["created_at_year"] = metrics_df["created_at"].map(lambda time :time.isocalendar()[0])
    metrics_df["created_at_week_num"] = metrics_df["created_at"].map(lambda time :time.isocalendar()[1])

    # print(metrics_df["created_at_week_num"])

    # m_created_count =  metrics_df["created_at_h"].value_counts().sort_index()
    #
    # web_metrics = metrics_df[metrics_df.platform == "web" ]
    # mobile_metrics = metrics_df[metrics_df.platform != "web"]
    #
    # web_metrics_count = web_metrics["created_at_h"].value_counts().sort_index()
    # mobile_metrics_count = mobile_metrics["created_at_h"].value_counts().sort_index()
    #
    # # plt.plot(web_metrics_count,label="web")
    # # plt.plot(mobile_metrics_count,label="mobile")
    # #
    # # plt.legend()
    # #
    # # plt.show()
    #
    # metrics_platform =  metrics_df.groupby(["platform"])
    # metrics_platform_summary =  metrics_platform["platform"].aggregate(len)

    metrics_year_week = metrics_df.groupby(["created_at_year","created_at_week_num"])
    metrics_year_week_sum = metrics_year_week["name"].aggregate(len)
    metrics_year_week_sum.plot(label="total")

    # metrics_year_week_sum.to_csv("m.csv",encoding="utf-8")

    x = np.arange(len(metrics_year_week_sum))

    def fit_func(x,a,b):
        return a*np.power(x,b)

    popt, pcov = curve_fit(fit_func,xdata=x,ydata=metrics_year_week_sum, method="lm")

    # plt.plot(x,metrics_year_week, 'b-', label="orginal")

    metrics_year_week_sum.plot(label="original")

    plt.plot(x,fit_func(x,*popt),'g--',label="fit")

    plt.legend()

    plt.show()

    #
    # metrics_status = metrics_df.groupby(["status"])
    # metrics_status_sum = metrics_status["status"].aggregate(len)

    # print(metrics_status_sum)

    # metrics_activated = metrics_df[metrics_df.status == "activated"]
    # metrics_archived = metrics_df[metrics_df.status == "archived"]
    #
    # metrics_activated_sum = metrics_activated.groupby(["created_at_year","created_at_week_num"])["status"].aggregate(len)
    # metrics_archived_sum = metrics_archived.groupby(["created_at_year","created_at_week_num"])["status"].aggregate(len)
    #
    # metrics_activated_sum.plot(label="activated")
    # metrics_archived_sum.plot(label="archived")
    #
    # plt.legend()
    # plt.show()







