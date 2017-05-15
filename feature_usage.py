import pandas as pd
import matplotlib.pyplot as plt
from raw_process import raw_prepare
import numpy as np


def proddb_growth(c=pd.DataFrame, d=pd.DataFrame, f=pd.DataFrame, m=pd.DataFrame, s=pd.DataFrame, r=pd.DataFrame, pids=pd.DataFrame):

    acgrouped = c[c.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("chart")
    adgrouped = d[d.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("dashboard")
    afgrouped = f[f.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("funnel")
    argrouped = r[r.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("retention")
    amgrouped = m[m.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("metric")
    asgrouped = s[s.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("segmentation")

    cgrouped = c.groupby(["year", "week"])["id"].agg("count").rename("chart")
    dgrouped = d.groupby(["year", "week"])["id"].agg("count").rename("dashboard")
    fgrouped = f.groupby(["year", "week"])["id"].agg("count").rename("funnel")
    rgrouped = r.groupby(["year", "week"])["id"].agg("count").rename("retention")
    mgrouped = m.groupby(["year", "week"])["id"].agg("count").rename("metric")
    sgrouped = s.groupby(["year", "week"])["id"].agg("count").rename("segmentation")

    fig, axes = plt.subplots(nrows=1, ncols=2)
    result = pd.concat([cgrouped, dgrouped, fgrouped, rgrouped, mgrouped, sgrouped], axis=1)
    aresult = pd.concat([acgrouped, adgrouped, afgrouped, argrouped, amgrouped, asgrouped], axis=1)
    result.plot(ax=axes[0]); axes[0].set_title("Feature Growth in DB", fontsize=15)
    aresult.plot(ax=axes[1]); axes[1].set_title("Feature Growth in Activated Project")
    plt.show()


def feature_growth(c=pd.DataFrame, d=pd.DataFrame, f=pd.DataFrame, r=pd.DataFrame, pids=pd.DataFrame):

    acgrouped = c[c.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("Activated Project")
    adgrouped = d[d.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("Activated Project")
    afgrouped = f[f.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("Activated Project")
    argrouped = r[r.project_id.isin(pids)].groupby(["year", "week"])["id"].agg("count").rename("Activated Project")

    cgrouped = c.groupby(["year", "week"])["id"].agg("count").rename("Project")
    dgrouped = d.groupby(["year", "week"])["id"].agg("count").rename("Project")
    fgrouped = f.groupby(["year", "week"])["id"].agg("count").rename("Project")
    rgrouped = r.groupby(["year", "week"])["id"].agg("count").rename("Project")

    charts = pd.concat([acgrouped, cgrouped], axis=1)
    dashboards = pd.concat([adgrouped, dgrouped], axis=1)
    funnels = pd.concat([afgrouped, fgrouped], axis=1)
    retentions = pd.concat([argrouped, rgrouped], axis=1)

    charts.plot(title="Chart")
    dashboards.plot(title="Dashboard")
    funnels.plot(title="Funnel Report")
    retentions.plot(title="Retention Report")
    plt.show()


def created_weekday(c=pd.DataFrame, d=pd.DataFrame, f=pd.DataFrame, m=pd.DataFrame, s=pd.DataFrame, r=pd.DataFrame, pids=pd.DataFrame):

    acgrouped = c[c.project_id.isin(pids)].groupby(["weekday"])["id"].agg("count").rename("chart")
    adgrouped = d[d.project_id.isin(pids)].groupby(["weekday"])["id"].agg("count").rename("dashboard")
    afgrouped = f[f.project_id.isin(pids)].groupby(["weekday"])["id"].agg("count").rename("funnel")
    argrouped = r[r.project_id.isin(pids)].groupby(["weekday"])["id"].agg("count").rename("retention")
    amgrouped = m[m.project_id.isin(pids)].groupby(["weekday"])["id"].agg("count").rename("metric")
    asgrouped = s[s.project_id.isin(pids)].groupby(["weekday"])["id"].agg("count").rename("segmentation")

    acprob = acgrouped / np.sum(acgrouped)
    adprob = adgrouped / np.sum(adgrouped)
    afprob = afgrouped / np.sum(afgrouped)
    arprob = argrouped / np.sum(argrouped)
    amprob = amgrouped / np.sum(amgrouped)
    asprob = asgrouped / np.sum(asgrouped)

    fig, axes = plt.subplots(nrows=2, ncols=3)
    fig.suptitle("Feature Usage Probability (Weekday)", fontsize=15)
    acprob.plot(ax=axes[0, 0]); axes[0, 0].set_title("Chart",  fontsize=10)
    adprob.plot(ax=axes[0, 1]); axes[0, 1].set_title("Dashbaord", fontsize=10)
    afprob.plot(ax=axes[0, 2]); axes[0, 2].set_title("Funnel", fontsize=10)
    arprob.plot(ax=axes[1, 0]); axes[1, 0].set_title("Retention", fontsize=10)
    amprob.plot(ax=axes[1, 1]); axes[1, 1].set_title("Metrics", fontsize=10)
    asprob.plot(ax=axes[1, 2]); axes[1, 2].set_title("Segmentation", fontsize=10)

    plt.show()


def created_hour(c=pd.DataFrame, d=pd.DataFrame, f=pd.DataFrame, m=pd.DataFrame, s=pd.DataFrame, r=pd.DataFrame, pids=pd.DataFrame):

    acgrouped = c[c.project_id.isin(pids)].groupby(["hour"])["id"].agg("count").rename("chart")
    adgrouped = d[d.project_id.isin(pids)].groupby(["hour"])["id"].agg("count").rename("dashboard")
    afgrouped = f[f.project_id.isin(pids)].groupby(["hour"])["id"].agg("count").rename("funnel")
    argrouped = r[r.project_id.isin(pids)].groupby(["hour"])["id"].agg("count").rename("retention")
    amgrouped = m[m.project_id.isin(pids)].groupby(["hour"])["id"].agg("count").rename("metric")
    asgrouped = s[s.project_id.isin(pids)].groupby(["hour"])["id"].agg("count").rename("segmentation")

    acprob = acgrouped / np.sum(acgrouped)
    adprob = adgrouped / np.sum(adgrouped)
    afprob = afgrouped / np.sum(afgrouped)
    arprob = argrouped / np.sum(argrouped)
    amprob = amgrouped / np.sum(amgrouped)
    asprob = asgrouped / np.sum(asgrouped)

    fig, axes = plt.subplots(nrows=2, ncols=3)
    fig.suptitle("Feature Usage Probability (Hour)", fontsize=15)
    acprob.plot(ax=axes[0, 0]); axes[0, 0].set_title("Chart", fontsize=10)
    adprob.plot(ax=axes[0, 1]); axes[0, 1].set_title("Dashbaord", fontsize=10)
    afprob.plot(ax=axes[0, 2]); axes[0, 2].set_title("Funnel", fontsize=10)
    arprob.plot(ax=axes[1, 0]); axes[1, 0].set_title("Retention", fontsize=10)
    amprob.plot(ax=axes[1, 1]); axes[1, 1].set_title("Metrics", fontsize=10)
    asprob.plot(ax=axes[1, 2]); axes[1, 2].set_title("Segmentation", fontsize=10)

    plt.show()


if __name__ == "__main__":

    projects = pd.read_csv("./0508/user_project_org_info.csv", low_memory=False)
    aproject_ids = projects[~(projects.first_date_of_getting_pv.isnull())]["project_id"].drop_duplicates()

    charts = pd.read_csv("./chart_dashboard/charts.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    charts = raw_prepare(charts)

    dashboards = pd.read_csv("./chart_dashboard/dashboards.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    dashboards = raw_prepare(dashboards)
    dashboards = dashboards[~(dashboards.status == "hidden") & ~(dashboards.type == "realtime") & ~(dashboards.chart_ids.isnull())]

    funnels = pd.read_csv("./chart_dashboard/funnels.csv", low_memory=False, parse_dates=["created_at"])
    funnels = raw_prepare(funnels)

    retentions = pd.read_csv("./db_export/retentions.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    retentions = raw_prepare(retentions)

    metrics = pd.read_csv("./db_export/metrics.csv", low_memory=False, parse_dates=["created_at", "updated_at"], error_bad_lines=False)
    metrics = raw_prepare(metrics)

    segmentations = pd.read_csv("./db_export/segmentations.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    segmentations = raw_prepare(segmentations)

    # proddb_growth(c=charts, d=dashboards, f=funnels, r=retentions, m=metrics, s=segmentations, pids=aproject_ids)
    # created_weekday(c=charts, d=dashboards, f=funnels, r=retentions, m=metrics, s=segmentations, pids=aproject_ids)
    # created_hour(c=charts, d=dashboards, f=funnels, r=retentions, m=metrics, s=segmentations, pids=aproject_ids)
    # feature_growth(c=charts, d=dashboards, f=funnels, r=retentions, pids=aproject_ids)

