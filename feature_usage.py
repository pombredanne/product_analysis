import pandas as pd
import matplotlib.pyplot as plt


def usage(c=pd.DataFrame, d=pd.DataFrame, f=pd.DataFrame, m=pd.DataFrame, s=pd.DataFrame, r=pd.DataFrame):

    cgrouped = c.groupby(["year", "week"])["id"].agg("count").rename("chart")
    dgrouped = d.groupby(["year", "week"])["id"].agg("count").rename("dashboard")
    fgrouped = f.groupby(["year", "week"])["id"].agg("count").rename("funnel")
    rgrouped = r.groupby(["year", "week"])["id"].agg("count").rename("retention")
    mgrouped = m.groupby(["year", "week"])["id"].agg("count").rename("metric")
    sgrouped = s.groupby(["year", "week"])["id"].agg("count").rename("segmentation")

    result = pd.concat([cgrouped, dgrouped, fgrouped, rgrouped, mgrouped, sgrouped], axis=1)
    result.plot()

    # fig, axes = plt.subplots(nrows=2, ncols=2)
    # result['chart'].plot(ax=axes[0, 0], color="b"); axes[0, 0].set_title('Chart', fontsize=15)
    # result['dashboard'].plot(ax=axes[0, 1], color="r"); axes[0, 1].set_title('Dashboard', fontsize=15)
    # result['funnel'].plot(ax=axes[1, 0], color="g"); axes[1, 0].set_title('Funnel', fontsize=15)
    # result['retention'].plot(ax=axes[1, 1], color="k"); axes[1, 1].set_title('Retention', fontsize=15)

    plt.show()



def raw_prepare(sample=pd.DataFrame):
    sample = sample[sample.project_id != 3]
    sample = sample.assign(year=lambda df: df["created_at"].map(lambda time: time.isocalendar()[0]))
    sample = sample.assign(week=lambda df: df["created_at"].map(lambda time: time.isocalendar()[1]))
    return sample


if __name__ == "__main__":

    charts = pd.read_csv("./chart_dashboard/charts.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    charts = raw_prepare(charts)

    dashboards = pd.read_csv("./chart_dashboard/dashboards.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    dashboards = raw_prepare(dashboards)

    funnels = pd.read_csv("./chart_dashboard/funnels.csv", low_memory=False, parse_dates=["created_at"])
    funnels = raw_prepare(funnels)

    retentions = pd.read_csv("./db_export/retentions.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    retentions = raw_prepare(retentions)

    metrics = pd.read_csv("./db_export/metrics.csv", low_memory=False, parse_dates=["created_at", "updated_at"], error_bad_lines=False)
    metrics = raw_prepare(metrics)

    segmentations = pd.read_csv("./db_export/segmentations.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    segmentations = raw_prepare(segmentations)

    usage(c=charts, d=dashboards, f=funnels, r=retentions, m=metrics, s=segmentations)



