import pandas as pd
from functools import reduce
from datetime import datetime
from datetime import timedelta
from feature_analytics_dahsbord import get_charts_info
from feature_analytics_dahsbord import get_dashboard_info
from feature_auto_tracking import get_metrics_intfo
from feature_funnel_report import get_funnel_info
from feature_retention_report import get_retention_intfo
from feature_segmentation import get_seg_info


if __name__ == '__main__':
    projects = pd.read_csv("./0508/user_project_org_info.csv", low_memory=False, parse_dates=["first_date_of_getting_pv"]).drop_duplicates("project_id").sort_values(by="project_id")
    projects = projects[(~projects.first_date_of_getting_pv.isnull()) & (projects.first_date_of_getting_pv > datetime(2016, 1,1))]

    features = ["chart", "dashboard", "funnel", "reten", "seg", "metric"]


    fst_chart = get_charts_info().sort_values(by="chart_created_at").groupby("project_id", as_index=False).first()
    fst_dashboard = get_dashboard_info().sort_values(by="dashboard_created_at").groupby("project_id", as_index=False).first()
    fst_funnel = get_funnel_info().sort_values(by="funnel_created_at").groupby("project_id", as_index=False).first()
    fst_reten = get_retention_intfo().sort_values(by="reten_created_at").groupby("project_id", as_index=False).first()
    fst_seg = get_seg_info().sort_values(by="seg_created_at").groupby("project_id", as_index=False).first()
    fst_metric = get_metrics_intfo().sort_values(by="metric_created_at").groupby("project_id", as_index=False).first()

    dfs = [projects, fst_chart, fst_dashboard, fst_funnel, fst_reten, fst_metric, fst_seg]
    cols = ["project_id", "first_date_of_getting_pv", "chart_created_at",
                  "dashboard_created_at", "funnel_created_at", "reten_created_at", "seg_created_at", "metric_created_at"]

    result = reduce(lambda left, right: pd.merge(left, right, how="left", left_on=["project_id"], right_on=["project_id"]), dfs)[cols].sort_values(by="project_id")
    result = result.fillna(0)

    describes = []

    for feature in ["chart", "dashboard", "funnel", "reten", "seg", "metric"]:
        delta = result[ feature + "_created_at" ] - result["first_date_of_getting_pv"]
        delta = delta[delta >= timedelta(days=-1)].rename(feature)
        describes.append(delta.describe())

    describe =  pd.concat(describes, axis=1)
    describe.to_csv("describe.csv")

    print(describe)
