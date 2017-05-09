import pandas as pd
from datetime import timedelta
import matplotlib.pyplot as plt


def range_parser(trange=str):
    time = trange.split(":")[1].split(",")
    if trange.split(":")[0] == "abs":
        tdelta = (int(time[1]) - int(time[0]))/(86400*1000)
    else:
        tdelta = int(time[0]) - int(time[1])
    return  tdelta

def comp_parser(exps=str):
    return len(exps.split("=>")[1].split(","))


def reten_sum(sample=pd.DataFrame):

    user_type_dic = {
        "uv": "Unique Visitor",
        "nuv": "New Unique Visitor",
        "usv": "Unique Signup Visitor",
        "nusv": "New Unique Signup Visitor"
    }

    grouped_date = sample.groupby(["year", "week"])["id"].agg("count").rename("Total")
    agrouped_date = sample[sample.status == "activated"].groupby(["year", "week"])["id"].agg("count").rename("Activated")
    nagrouped_date = sample[~(sample.status == "activated")].groupby(["year", "week"])["id"].agg("count").rename("Archived")

    result = pd.concat([agrouped_date, nagrouped_date, grouped_date], axis=1)

    # result.plot()
    # plt.show()

    fig, axes = plt.subplots(nrows=2, ncols=2)

    grouped_usert = sample.groupby(["user_type"])["id"].agg("count").rename("User Type").sort_values(ascending=False)
    grouped_usert.index = grouped_usert.index.map(lambda type: user_type_dic[type])
    grouped_usert.plot(ax=axes[0,0], kind='pie', figsize=(6, 6), subplots=True,  autopct='%.2f', fontsize=10, mark_right=False)


    grouped_range = sample.groupby(["range"])["id"].agg("count").rename("Time Range").sort_values(ascending=False)
    grouped_range.plot(ax=axes[0, 1], kind='pie', figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15)

    sample["time_range_i"] = sample["time_range"].map(lambda trange: range_parser(trange))
    grouped_tri = sample.groupby(["time_range_i"])["id"].agg("count")


    grouped_scene =  sample.groupby(["scene"])["id"].agg("count").rename("count").sort_values(ascending=False)
    grouped_scene.plot(ax=axes[1, 0], kind='pie', figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15)

    grouped_dim = sample.groupby(["compared_type"])["id"].agg("count").sort_values(ascending=False)
    grouped_dim.plot(ax=axes[1, 1], kind='pie', figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15)

    stored_comp = sample[~sample.compared_map.isnull()]["compared_map"].map(lambda exps: comp_parser(exps))

    plt.show()


if __name__ == "__main__":
    retens = pd.read_csv("./db_export/retentions.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    retens = retens[(retens.status == "activated") & (retens.project_id != 3)]
    retens["year"] = retens["created_at"].map(lambda time: time.isocalendar()[0])
    retens["week"] = retens["created_at"].map(lambda time: time.isocalendar()[1])

    reten_sum(sample=retens)