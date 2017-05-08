import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt


def count_psql_array(exps=pd.Series):
    return exps.map(lambda x: len(re.compile("{(.*?)}").findall(x)[0].split(",")))


def psql_parser(exps=pd.Series):
    return exps.map(lambda exp: list(re.compile("{(.*?)}").findall(exp)[0].split(",")))


def chart_summary(sample=pd.DataFrame, das=pd.DataFrame):
    cgrouped_date = sample.groupby(["year", "week"])["id"].agg("count").rename("chart")
    dgrouped_date = das.groupby(["year", "week"])["id"].agg("count").rename("dashbaord")
    re = pd.concat([cgrouped_date, dgrouped_date], axis=1)

    re.plot()
    plt.show()

    charts_type = sample["chart_type"].drop_duplicates()
    types = []
    for type in charts_type:
        types.append(sample[sample.chart_type == type].groupby(["year", "week"])["id"].agg("count").rename(type))

    result = pd.concat(types, axis=1)
    result.plot()
    plt.show()



def chart_type_summary(sample=pd.DataFrame):
    grouped_type = sample.groupby(["chart_type"])["id"].agg("count").rename("Charts")
    grouped_atype = sample[sample.status == "activated"].groupby(["chart_type"])["id"].agg("count").rename("Active Charts")
    per = (round(grouped_atype*100 / grouped_type, 3)).rename("per")
    summary = pd.concat([grouped_atype, grouped_type, per], axis=1)
    print(summary.sort_values(by="per", ascending=False))


def dashboard_project_sum(sample=pd.DataFrame):
    # sample = sample[sample["status"] == "activated"]
    mean_m = sample["last_version_num"]
    print("Modified Time")
    print(mean_m.describe())

    grouped_id = sample.groupby(["project_id"])["id"].agg("count")
    print("Dashboard Summary")
    print(grouped_id.describe())

    grouped_yw = sample.groupby(["year", "week"])["id"].agg("count")
    grouped_yw.plot()
    plt.show()


def sub_chart_sum(sample=pd.DataFrame, charts=pd.DataFrame, users=pd.DataFrame):
    grouped_type = sample.groupby(["subscribe_type"])["user_id"].agg("count")
    print(grouped_type)

    subd_charts = pd.merge(sample[sample.subscribe_type == "Chart"], charts[charts.status == "activated"], left_on=["subscribe_id"], right_on=["id"])
    grouped_chatype = subd_charts.groupby(["chart_type"])["user_id"].agg("count")
    grouped_chatype_sum = grouped_chatype.rename("count").sort_values(ascending=False)

    print(grouped_chatype_sum)

    subd_charts = subd_charts.assign(metrics_num=count_psql_array(subd_charts["metrics"]))
    subd_charts_metrics = subd_charts.groupby(["chart_type"])[["metrics_num", "chart_type"]].agg({"metrics_num": np.sum, "chart_type": len})
    subd_charts_metrics["metrics/charts"] = round(subd_charts_metrics["metrics_num"] / subd_charts_metrics["chart_type"], 3)

    subd_charts_user = pd.merge(subd_charts, users, how="left", left_on=["user_id"], right_on=["user_id_project"])
    grouped_type_nadmin = subd_charts_user[~subd_charts_user.role.isin(["Admin", "管理员"])].groupby(["chart_type"])["id_x"].agg("count").rename("coworker").sort_values(ascending=False).reset_index()
    grouped_type_admin = subd_charts_user[subd_charts_user.role.isin(["Admin", "管理员"])].groupby(["chart_type"])["id_x"].agg("count").rename("admin").sort_values(ascending=False).reset_index()

    result = pd.concat([grouped_type_nadmin, grouped_type_admin], axis=1)
    result["network"] = round(result["coworker"] / result["admin"], 3)
    print(result)


def sub_dashboard_sum(sample=pd.DataFrame, dashboard=pd.DataFrame, charts=pd.DataFrame):

    sub_dab = pd.merge(sample[sample.subscribe_type == "Dashboard"], dashboard[dashboard.status == "activated"],
                       left_on=["subscribe_id"], right_on=["id"])

    dab_users = sub_dab.groupby("subscribe_id")["user_id"].agg("count")
    # print(dab_users.describe())

    sdab_project = sub_dab.groupby("project_id_x")["project_id_x"].agg("count").rename("shared")
    dashboard = dashboard[dashboard.status == "activated"]
    dab_project = dashboard.groupby("project_id")["project_id"].agg("count").rename("total")

    r = pd.concat([sdab_project.describe(), dab_project.describe()], axis=1)
    # print(r)

    grouped_type = sample.groupby(["subscribe_type"])["id"].agg("count").rename("num")
    grouped_type_all = np.sum(grouped_type)
    grouped_type_per = (round(grouped_type*100 / grouped_type_all)).rename("per")
    grouped_type_sum = pd.concat([grouped_type, grouped_type_per], axis=1)

    print(grouped_type_sum)



def dashboard_usage(sample=pd.DataFrame, charts=pd.DataFrame):

    grouped_s = sample.groupby(["status"])["id"].agg("count")
    percentage = round(grouped_s / np.sum(grouped_s), 3)
    status = pd.concat([grouped_s, percentage], axis=1, keys=["count", "percentage(%)"])
    print("Dashboard Status Summary : ")
    print(status)

    a_sample = sample[sample.status == "activated"].groupby(["year", "week"])["id"].agg("count")
    d_sample = sample[sample.status == "deleted"].groupby(["year", "week"])["id"].agg("count")
    status_time = pd.concat([a_sample, d_sample], axis=1, keys=["activated", "deleted"]).fillna(0)

    status_time.plot()
    plt.show()


def dashboard_chart(sample=pd.DataFrame):
    sample = sample.assign(chart_num=lambda df: count_psql_array(df.chart_ids))
    print(sample)
    print(sample[["chart_num"]].describe())
    grouped_yw = sample.groupby(["year", "week"])["chart_num"].agg([np.sum])
    grouped_yw_mean = sample.groupby(["year", "week", "project_id"])["chart_num"].mean()
    print(np.mean(grouped_yw_mean))
    print(np.median(grouped_yw_mean))
    grouped_yw.plot()
    plt.show()


def dashboard_chart2(sample=pd.DataFrame, charts=pd.DataFrame):

    dsc = pd.DataFrame({"dashboard_id": sample["id"], "chart_ids": psql_parser(sample["chart_ids"])})
    result = []
    for d_id in dsc["dashboard_id"].index:
        for chart_id in dsc["chart_ids"][d_id]:
            if chart_id != '':
                result.append((dsc["dashboard_id"][d_id], int(chart_id)))

    da_chart = pd.DataFrame(result, columns=["dashboard_id", "chart_id"])
    da_chart = pd.merge(da_chart, sample, how="left", left_on=["dashboard_id"], right_on=["id"])
    da_chart = pd.merge(da_chart, charts, how="left", left_on=["dashboard_id"], right_on=["id"])
    da_chart = da_chart[(da_chart.status_x == "activated") & (da_chart.status_y == "activated")]

    project_chart_type = da_chart.groupby(["project_id_x"]).apply(lambda group: group.groupby(["chart_type"])["chart_id"].agg("count"))
    project_chart_sum = project_chart_type.reset_index().rename(columns={"project_id_x": "project_id", "chart_id": "count"})

    da_chart = project_chart_sum.groupby(["chart_type"])["count"].agg([np.sum, np.mean, np.median, np.std]).sort_values(by="sum", ascending=False)
    grouped_type = charts[charts.status == "activated"].groupby(["chart_type"])["id"].agg("count").sort_values(ascending=False)

    chart_usages = pd.concat([da_chart, grouped_type], axis=1).rename(columns={"id": "total"}).sort_values(by="sum", ascending=False)
    chart_usages["lever"] = round(chart_usages["sum"]/chart_usages["total"], 3)
    print(chart_usages)


if __name__ == "__main__":
    date_columns = ["created_at", "updated_at"]

    charts = pd.read_csv("./chart_dashboard/charts.csv", low_memory=False, parse_dates=["created_at"])
    dashboard = pd.read_csv("./chart_dashboard/dashboards.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    subs = pd.read_csv("./chart_dashboard/subscriptions.csv", low_memory=False)
    users = pd.read_csv("./0502/user_org_project_info.csv", low_memory=False)

    dashboard = dashboard[~(dashboard.project_id == 3)]
    dashboard["year"] = dashboard["created_at"].map(lambda time: time.isocalendar()[0])
    dashboard["week"] = dashboard["created_at"].map(lambda time: time.isocalendar()[1])

    charts = charts[~(charts.project_id == 3)]
    charts["year"] = charts["created_at"].map(lambda time: time.isocalendar()[0])
    charts["week"] = charts["created_at"].map(lambda time: time.isocalendar()[1])

    ndd = dashboard[~(dashboard.status == "hidden") & ~(dashboard.type == "realtime") & ~(dashboard.chart_ids.isnull())]
    # chart_type_summary(charts)
    chart_summary(charts, ndd)
    # dashboard_project_sum(ndd)
    # dashboard_usage(ndd)
    # dashboard_chart2(ndd, charts=charts)
    # sub_chart_sum(subs, charts=charts, users=users)
    # sub_dashboard_sum(subs, dashboard=ndd, charts=charts)
