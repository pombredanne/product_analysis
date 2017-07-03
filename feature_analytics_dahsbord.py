import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
import jieba
from raw_process import raw_prepare
from saviatr import saviatar_speed


def count_psql_array(exps=pd.Series):
    return exps.map(lambda x: len(re.compile("{(.*?)}").findall(x)[0].split(",")))


def psql_parser(exps=pd.Series):
    return exps.map(lambda exp: list(re.compile("{(.*?)}").findall(exp)[0].split(",")))


def count_dim_num(exp=""):
    rs = re.compile("{(.*?)}").findall(exp)[0]
    if rs == "":
        return 0
    else:
        return len(rs.split(","))


def chart_summary(sample=pd.DataFrame, das=pd.DataFrame):

    sample["dim_num"] = sample["dimensions"].map(lambda exp: count_dim_num(exp))

    grouped_typedm = sample.groupby(["chart_type"])["dim_num"].agg([np.mean, np.median, np.std])
    print(grouped_typedm)

    cgrouped_date = sample.groupby(["year", "week"])["id"].agg("count").rename("chart")
    dgrouped_date = das.groupby(["year", "week"])["id"].agg("count").rename("dashbaord")
    result = pd.concat([cgrouped_date, dgrouped_date], axis=1)
    result.plot(title="Feature Growth", fontsize=15)

    charts_type = sample["chart_type"].drop_duplicates()
    types = []
    for type in charts_type:
        types.append(sample[sample.chart_type == type].groupby(["year", "week"])["id"].agg("count").rename(type))

    result2 = pd.concat(types, axis=1)
    result2.plot(title="Chart with Type Growth", fontsize=15)

    acharts = sample[sample.status == "activated"]
    types2 = []
    for type in charts_type:
        types2.append(acharts[acharts.chart_type == type].groupby(["year", "week"])["id"].agg("count").rename(type))

    result3 = pd.concat(types2, axis=1)
    result3.plot(title='Activated Charts Usage with Type', fontsize=15)

    nacharts = sample[~(sample.status == "activated")].groupby(["year", "week"])["id"].agg("count").rename("Charts")
    nadshboard = das[~(das.status == "activated")].groupby(["year", "week"])["id"].agg("count").rename("Dashboard")
    pd.Series([round(np.sum(nadshboard) / (np.sum(nadshboard) + np.sum(nacharts)), 2),
               round(np.sum(nacharts) / (np.sum(nadshboard) + np.sum(nacharts)), 2) ]).plot(kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=20, labels=None, title="Delete Probability")
    plt.set_yable("")
    plt.legend()
    plt.show()


def chart_type_summary(sample=pd.DataFrame):
    grouped_type = sample.groupby(["chart_type"])["id"].agg("count").rename("Charts")
    grouped_atype = sample[sample.status == "activated"].groupby(["chart_type"])["id"].agg("count").rename("Active Charts")
    grouped_projid = sample.groupby(["project_id","chart_type"])["id"].agg("count").rename("count").reset_index().groupby(["chart_type"])["count"].agg([np.mean, np.median, np.std])

    print(grouped_projid.sort_values(by="mean", ascending=False))

    per = (round(grouped_atype*100 / grouped_type, 3)).rename("per")

    summary = pd.concat([grouped_atype, grouped_type, per], axis=1).sort_values(by="Active Charts", ascending=False)

    fig, axes = plt.subplots(nrows=1, ncols=2)
    summary[["Active Charts", "Charts"]].plot.bar(ax=axes[0]).set_title('Usage', fontsize=15)
    percentage = (summary["Active Charts"] / np.sum( summary["Active Charts"]))
    percentage.plot(kind='pie', figsize=(6, 6), subplots=True, labels=None) ; axes[1].set_title("Chart Type Share"); axes[1].set_ylabel('')
    print(percentage)
    print(summary.sort_values(by="per", ascending=False))
    plt.show()


def dashboard_project_sum(sample=pd.DataFrame):

    print(sample)

    sample = sample[sample["status"] == "activated"]

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

    # grouped_type = sample.groupby(["subscribe_type"])["user_id"].agg("count").rename("count")
    # type_per = round(grouped_type*100 / np.sum(grouped_type), 1).rename("percentage")
    # grouped_type = pd.concat([grouped_type, type_per], axis=1)

    # fig, axes = plt.subplots(nrows=1, ncols=2)
    # grouped_type["count"].sort_values(ascending=False).plot.bar(ax=axes[0]); axes[0].set_title("Subscription Type", fontsize=15)
    # grouped_type["percentage"].plot(kind="pie", figsize=(6, 6), subplots=True, fontsize=10) ; axes[1].set_title("Subscription Share", fontsize=15); axes[1].set_ylabel('')
    # print(grouped_type)

    # plt.show()

    subd_charts = pd.merge(sample[sample.subscribe_type == "Chart"], charts[charts.status == "activated"], left_on=["subscribe_id"], right_on=["id"])
    subd_charts = subd_charts[subd_charts.creator_id != subd_charts.user_id]
    # chart_metrics(c=subd_charts)
    # subd_charts_num = subd_charts["id_x"].drop_duplicates()
    # total_charts = charts[charts.status == "activated"]["id"].drop_duplicates()
    # sub_rate = round(len(subd_charts_num)*100 / len(total_charts), 3)
    # print(len(total_charts))
    # print(len(subd_charts_num))
    #
    # percentage = pd.Series([ sub_rate , 100 -sub_rate], index=["Subscribed", "Not Subscribed"])
    # print([ sub_rate , 100 -sub_rate])
    # percentage.plot(kind="pie", figsize=(6, 6), subplots=True, fontsize=10, title="Chart Sub Rate")

    # plt.show()

    # board_name_sum(sample=charts, name="Total Charts")
    # board_name_sum(sample=subd_charts, name="Sub Active Charts")
    #
    # grouped_sub = subd_charts.groupby(["chart_type"])["id_y"].agg("count").rename("Subscribed")
    # grouped_active = charts[charts.status == "activated"].groupby(["chart_type"])["id"].agg("count").rename("Active")
    # grouped_total = charts.groupby(["chart_type"])["id"].agg("count").rename("Total")
    #
    # grouped_chart = pd.concat([grouped_sub, grouped_active, grouped_total], axis=1).sort_values(by="Total", ascending=False)
    # grouped_chart.plot.bar(title="Number of Charts")
    # plt.show()
    #
    subd_charts = subd_charts.assign(metrics_num=count_psql_array(subd_charts["metrics"]))
    charts = charts.assign(metrics_num=count_psql_array(charts["metrics"]))

    subd_ct_metrics = subd_charts.groupby(["chart_type"])["metrics_num"].agg([np.mean, np.median, np.std])
    nsub_ct_metrics = charts[~charts["id"].isin(subd_charts["id_y"].drop_duplicates())].groupby(["chart_type"])["metrics_num"].agg([np.mean, np.median, np.std])
    chart_metrics = charts.groupby(["chart_type"])["metrics_num"].agg([np.mean, np.median, np.std]).sort_values(by="mean", ascending=False)
    subd_cp_describe = subd_charts.groupby(["project_id_x",
                                            "chart_type", "id_y"])["metrics_num"].agg(np.sum).reset_index().groupby(["chart_type"])["metrics_num"].agg([np.sum, np.mean, np.median, np.std]).sort_values(by="mean", ascending=False)
    print(subd_cp_describe)

    metrics_num = pd.concat([subd_ct_metrics, nsub_ct_metrics, chart_metrics], axis=1)
    print(metrics_num)
    #
    # # print(subd_cp_describe)
    #
    # subd_charts_user = pd.merge(subd_charts, users, how="left", left_on=["user_id"], right_on=["user_id_project"])
    # grouped_type_producer = subd_charts_user[~subd_charts_user.role.isin(["Admin", "管理员", "Creator"])].groupby(["chart_type"])["id_x"].agg("count").rename("coworker").sort_values(ascending=False).reset_index()
    # grouped_type_consumer = subd_charts_user[subd_charts_user.role.isin(["Admin", "管理员", "Creator"])].groupby(["chart_type"])["id_x"].agg("count").rename("admin").sort_values(ascending=False).reset_index()
    #
    # result = pd.concat([grouped_type_producer, grouped_type_consumer], axis=1)
    # result["network"] = round(result["coworker"] / result["admin"], 3)
    # # print(result)
    #
    # num_shrad_charts = len(subd_charts["id_y"].drop_duplicates())
    # num_total_charts = len(charts["id"].drop_duplicates())
    # per_shared_charts = round(num_shrad_charts*100 / num_total_charts, 3)
    # per_nshared_charts =  round( (num_total_charts - num_shrad_charts)*100 / num_total_charts, 3 )
    #
    # shared_per = pd.Series([per_shared_charts, per_nshared_charts], index=["Shared", "Not Shared"])
    # shared_per.plot(kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=20)
    #
    # plt.show()


def sub_dashboard_sum(sample=pd.DataFrame, dashboard=pd.DataFrame, charts=pd.DataFrame):

    sub_dab = pd.merge(sample[sample.subscribe_type == "Dashboard"], dashboard[dashboard.status == "activated"],
                       left_on=["subscribe_id"], right_on=["id"])

    sub_dab = sub_dab[sub_dab.creator_id != sub_dab.user_id]

    # board_name_sum(sub_dab, name="Sub Dashbaord")
    num_shared_dasd = len(sub_dab["id_y"].drop_duplicates())
    num_total_dasd = len(dashboard["id"].drop_duplicates())
    per_shared_dasd = round(num_shared_dasd * 100 / num_total_dasd, 3)
    per_nshared_dasd = round((num_total_dasd - num_shared_dasd) * 100 / num_total_dasd, 3)

    shared_per = pd.Series([per_shared_dasd, per_nshared_dasd], index=["Shared", "Not Shared"])
    # shared_per.plot(kind="pie", figsize=(6, 6), subplots=True)

    # plt.show()
    #
    dab_users = sub_dab.groupby("subscribe_id")["user_id"].agg("count")
    print(dab_users.describe())

    sdab_project = sub_dab.groupby("project_id_x")["project_id_x"].agg("count").rename("shared")
    dashboard = dashboard[dashboard.status == "activated"]
    dab_project = dashboard.groupby("project_id")["project_id"].agg("count").rename("total")

    r = pd.concat([sdab_project.describe(), dab_project.describe()], axis=1)
    print(r)

    grouped_type = sample.groupby(["subscribe_type"])["id"].agg("count").rename("num")
    grouped_type_all = np.sum(grouped_type)
    grouped_type_per = (round(grouped_type*100 / grouped_type_all)).rename("per")
    grouped_type_sum = pd.concat([grouped_type, grouped_type_per], axis=1)

    print(grouped_type_sum)
    grouped_type_sum["num"].plot(kind='pie', figsize=(6, 6), subplots=True,  autopct='%.2f', fontsize=20)
    plt.show()


def dashboard_usage(sample=pd.DataFrame, charts=pd.DataFrame):

    grouped_s = sample.groupby(["status"])["id"].agg("count")
    percentage = round(grouped_s*100 / np.sum(grouped_s), 3)
    status = pd.concat([grouped_s, percentage], axis=1, keys=["count", "percentage(%)"])

    status["percentage(%)"].plot(kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=20)
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
    da_chart = da_chart[da_chart.user_id != da_chart.creator_id]

    # board_name_sum(sample=da_chart)

    grouped_did = da_chart.groupby(["dashboard_id"])["chart_id"].agg("count").describe()
    print(grouped_did)


    grouped_proj_ctype = da_chart.groupby(["project_id_x", "chart_type"])["chart_id"].agg("count").reset_index().groupby(["chart_type"])["chart_id"].agg([np.sum, np.mean, np.median, np.std]).sort_values(by="sum", ascending=False)

    print(grouped_proj_ctype)

    project_chart_type = da_chart.groupby(["project_id_x"]).apply(lambda group: group.groupby(["chart_type"])["chart_id"].agg("count"))
    project_chart_sum = project_chart_type.reset_index().rename(columns={"project_id_x": "project_id", "chart_id": "count"})

    da_chart = project_chart_sum.groupby(["chart_type"])["count"].agg([np.sum, np.mean, np.median, np.std]).sort_values(by="sum", ascending=False)
    grouped_type = charts[charts.status == "activated"].groupby(["chart_type"])["id"].agg("count").sort_values(ascending=False)

    chart_usages = pd.concat([da_chart, grouped_type], axis=1).rename(columns={"id": "total"}).sort_values(by="sum", ascending=False)
    chart_usages["lever"] = round(chart_usages["sum"]/chart_usages["total"], 3)

    print(chart_usages)
    chart_usages[["sum", "total"]].rename(columns={"sum" : "Dashboard", "total": "DB"}).plot.bar()
    plt.show()


def board_name_sum(sample=pd.DataFrame, name=""):

    jieba.enable_parallel(4)
    jieba.suggest_freq(("看板"), True)
    print("Start to Tokenize")
    name_tokenize = sample["name"].map(lambda name: jieba.cut_for_search(str(name).split("_")[0]))
    print("Start to Flat Token")
    total_token = [item for sug_list in name_tokenize for item in sug_list]
    print("Start to Count Words")

    word_dict = {}
    for word in total_token:
        if word in word_dict:
            word_dict[word] += 1
        else:
            word_dict[word] = 1

    wc = pd.DataFrame(list(word_dict.items()), columns=["word", "count"]).sort_values(by="count", ascending=False).set_index(["word"])

    print("Start to Output " + name + " Top 20 words")
    wc[:20].to_csv(name + "_word_count.csv", encoding="utf-8")


def get_dashboard_info():
    dashboard = pd.read_csv("./chart_dashboard/dashboards.csv", low_memory=False,
                            parse_dates=["created_at", "updated_at"])
    dashboard = raw_prepare(dashboard)
    dashboard = dashboard[~(dashboard.status == "hidden") & ~(dashboard.type == "realtime") & ~(dashboard.chart_ids.isnull())]
    dashboard["chart_num"] = dashboard["chart_ids"].map(lambda ids: len(ids.split(",")))
    dashboard["created_at"] = dashboard["created_at"].map(lambda time: pd.to_datetime(time.strftime("%Y-%m-%d")))

    cols = ["id", "project_id", "chart_num", "status",
            "creator_id", "created_at", "year", "week", "weekday", "hour"]

    rename_dic = {
        "id" : "dashboard_id",
        "chart_num" : "dashboard_chart_num",
        "created_at": "dashboard_created_at",
        "status" : "dashboard_chart_status"
    }

    return dashboard[cols].rename(columns=rename_dic)


def get_charts_info():
    charts = pd.read_csv("./chart_dashboard/charts.csv", low_memory=False, parse_dates=["created_at"])
    charts = raw_prepare(charts)
    charts["created_at"] = charts["created_at"].map(lambda time: pd.to_datetime(time.strftime("%Y-%m-%d")))

    charts = charts.assign(metrics_num=count_psql_array(charts["metrics"]))
    charts = charts.assign(dims_num=count_psql_array(charts["dimensions"]))

    cols = ["id","project_id",  "chart_type", "metrics_num", "creator_id",
            "dims_num", "created_at",
            "year", "week", "weekday", "hour"]

    rename_dic = {
        "id" : "chart_id",
        "metrics_num" : "chart_metrics_num",
        "dim_num" : "chart_dim_num",
        "created_at": "chart_created_at"
    }

    return charts[cols].rename(columns=rename_dic)


def chart_breaking_level(c=pd.DataFrame):
    metric_event_rule = pd.read_csv("./db_export/growing_metrics_event_rule.csv", low_memory=False)
    nouse = pd.read_csv("./db_export/nouse_metrics_rule_id.csv", low_memory=False)
    nouse_metric_ids = pd.merge(nouse, metric_event_rule, how="left", left_on=["rules_id"], right_on=["rule_id"])[
        "metric_id"].drop_duplicates().sort_values().astype(float)

    c["metrics"] =  c["metrics"].map(lambda exp: list(re.compile("{(.*?)}").findall(exp)[0].split(",")))
    sc = c[["id", "chart_type", "metrics"]].reset_index(drop=False)

    cms = []
    for i in sc.index.get_values():
        for si in sc.iloc[i]["metrics"]:
            try:
                cms.append((sc.iloc[i]["id"], int(si)))
            except:
                pass

    cms = pd.DataFrame.from_records(cms, columns=["cid", "metric_id"])
    result = pd.merge(sc, cms, how="left", left_on=["id"], right_on=["cid"])
    result = result[~result["metric_id"].isnull()]
    result["astatus"] = result["metric_id"].map(lambda id: "dead" if id in nouse_metric_ids else "alive")

    fids = result["cid"].drop_duplicates()
    per = []
    for fid in fids:
        rr = result[result["cid"] == fid]
        cma = len(rr[rr.astatus == "alive"])
        cmap = round(int(cma)*100/int(len(rr)), 3)
        per.append((fid, cmap, 100 - cmap ))

    result = pd.DataFrame.from_records(per, columns=["cid", "am_per", "dm_per"])
    labels = ["{0} - {1}".format(i, i + 10) for i in range(0, 110, 10)]
    dm_cut = pd.cut(result["dm_per"], range(0, 115, 10), right=False, labels=labels).rename("breaking_level")

    describe = pd.concat([result, dm_cut], axis=1).groupby("breaking_level")["cid"].agg("count").rename("Dead Metric Count")
    describe_per = round( describe*100 / np.sum(describe), 3).rename("chart_share")
    describe = pd.concat([describe, describe_per],axis=1).sort_values(by="chart_share", ascending=False)

    describe["chart_share"].plot(kind='pie', figsize=(6, 6), labels=None, title="Breaking Level Share of Activated Chart Report")
    print(describe)

    plt.show()


if __name__ == "__main__":

    projects = pd.read_csv("./0508/user_project_org_info.csv", low_memory=False)
    aproject_ids =  projects[~projects.first_date_of_getting_pv.isnull()][["project_id", "level"]].drop_duplicates("project_id")

    charts = pd.read_csv("./chart_dashboard/charts.csv", low_memory=False, parse_dates=["created_at"])
    dashboard = pd.read_csv("./chart_dashboard/dashboards.csv", low_memory=False, parse_dates=["created_at", "updated_at"])

    subs = pd.read_csv("./chart_dashboard/subscriptions.csv", low_memory=False)
    users = pd.read_csv("./0502/user_org_project_info.csv", low_memory=False)

    dashboard = raw_prepare(dashboard)
    dashboard = dashboard[dashboard.project_id.isin(aproject_ids)]

    default_dashboards = ["市场概况分析", "用户概况分析", "产品概况分析"]
    default_charts = ["用户获取_Web访问来源", "用户获取_Web端自主投放渠道跟踪",
                      "用户获取_APP下载渠道", "用户属性_地域分布",
                      "用户属性_设备属性", "产品使用概况_受访域名", "产品使用概况_Web端跳出率", "产品使用概况_访问时长"]


    charts = raw_prepare(charts)
    charts = charts[charts.project_id.isin(aproject_ids["project_id"]) ]

    # chart_metrics(c=charts)

    ndd = dashboard[~(dashboard.status == "hidden") & ~(dashboard.type == "realtime") & ~(dashboard.chart_ids.isnull()) & (~dashboard.name.isin(default_dashboards))].reset_index(drop=True)
    # dash_saviatar = saviatar_speed(ndd)
    # print(dash_saviatar.head(20))
    #
    # chart_savitar = saviatar_speed(charts)
    # print(chart_savitar.head(20))

    # chart_type_summary(charts)
    # chart_summary(charts, ndd)
    # dashboard_project_sum(ndd)
    # dashboard_usage(ndd)
    # dashboard_chart2(ndd, charts=charts)
    sub_chart_sum(subs, charts=charts, users=users)
    # sub_dashboard_sum(subs, dashboard=ndd, charts=charts)
    # board_name_sum(sample=ndd, name="dashboard")