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
        "uv": "访问用户",
        "nuv": "新访问用户",
        "usv": "登入用户",
        "nusv": "新登入用户"
    }

    grouped_date = sample.groupby(["year", "week"])["id"].agg("count").rename("count")
    print(grouped_date.describe())

    grouped_usert = sample.groupby(["user_type"])["id"].agg("count").rename("count").sort_values(ascending=False)
    grouped_usert.index = grouped_usert.index.map(lambda type: user_type_dic[type])
    print(grouped_usert)

    grouped_range = sample.groupby(["range"])["id"].agg("count").rename("count").sort_values(ascending=False)
    print(grouped_range)

    sample["time_range_i"] = sample["time_range"].map(lambda trange: range_parser(trange))
    grouped_tri = sample.groupby(["time_range_i"])["id"].agg("count")
    print(grouped_tri.describe())

    grouped_scene =  sample.groupby(["scene"])["id"].agg("count").rename("count").sort_values(ascending=False)
    print(grouped_scene)

    grouped_dim = sample.groupby(["compared_type"])["id"].agg("count").sort_values(ascending=False)
    stored_comp = sample[~sample.compared_map.isnull()]["compared_map"].map(lambda exps: comp_parser(exps))
    print(stored_comp.describe())
    # print(stored_comp)


if __name__ == "__main__":
    retens = pd.read_csv("./db_export/retentions.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    retens = retens[(retens.status == "activated") & (retens.project_id != 3)]
    retens["year"] = retens["created_at"].map(lambda time: time.isocalendar()[0])
    retens["week"] = retens["created_at"].map(lambda time: time.isocalendar()[1])

    reten_sum(sample=retens)