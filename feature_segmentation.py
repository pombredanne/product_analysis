import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json


def seg_sum(sample=pd.DataFrame):
    grouped_yw = sample.groupby(["year", "week"])["id"].agg("count").rename("count")
    grouped_yw.plot()
    plt.show()


def user_num_sum(sample=pd.DataFrame):

    labels = ["{0} - {1}".format(i, i + 500000) for i in range(0, 100000000, 500000)]
    seg_cut = pd.cut(sample["user_nums"], range(0, 100000005, 500000), right=False, labels=labels).rename("cut")

    grouped_cut = pd.concat([sample, seg_cut], axis=1)
    result = grouped_cut.groupby("cut")["id"].agg("count").rename("sum")
    per = round(result*100 / np.sum(result), 3).rename("per")
    result = pd.concat([result, per ], axis=1).head()

    sample2 = sample[sample.user_nums < 500000]
    labels2 = ["{0} - {1}".format(i, i + 10000) for i in range(0, 500000, 10000)]
    seg_cut2 = pd.cut(sample2["user_nums"], range(0, 500005, 10000), right=False, labels=labels2).rename("cut")
    grouped_cut2 = pd.concat([sample2, seg_cut2], axis=1)
    result2 =  grouped_cut2.groupby("cut")["id"].agg("count").head()
    per = round(result2 * 100 / np.sum(result2), 3).rename("per")
    result2 = pd.concat([result2, per], axis=1).head()

    print(result)
    print(result2)


def cohort_con(sample=pd.DataFrame):
    # print(sample[["metric_filters_op","filter","dim_filters_op","snapshot"]])

    con_and = sample.groupby(["metric_filters_op"])["id"].agg("count").rename("sum(and)")
    con_and_pro = round(con_and *100 / np.sum(con_and), 3).rename("pro(and)")
    con_and = pd.concat([con_and, con_and_pro], axis=1)

    con_or = sample.groupby(["dim_filters_op"])["id"].agg("count").rename("sum(or)")
    con_or_pro = round(con_or*100 / np.sum(con_or), 3).rename("pro(or)")
    con_or = pd.concat([con_or, con_or_pro], axis=1)

    result = pd.concat([con_and, con_or], axis=1)
    print(result)

def chort_snapshot(sample=pd.DataFrame):
    # print(json.loads(sample["snapshot"][18191]))

    dim_filters = []
    metrics_filters = []
    total_filters = []

    for i in sample.index:
        dims = len(json.loads(sample["snapshot"][i])["dimensionFilters"])
        metrics = len(json.loads(sample["snapshot"][i])["metricFilters"])

        dim_filters.append(dims)
        metrics_filters.append(metrics)
        total_filters.append((dims+metrics))

    filter_nums = pd.DataFrame({"metric_filters" : metrics_filters, "dim_filters": dim_filters, "filters": total_filters})
    sample = pd.concat([sample, filter_nums], axis=1)

    grouped_filter_num = sample.groupby(["filters"])["id"].agg("count").rename("total")
    grouped_mfilter_num = sample.groupby(["metric_filters"])["id"].agg("count").rename("metrics")
    grouped_dfilter_num = sample.groupby(["dim_filters"])["id"].agg("count").rename("dim")

    result = pd.concat([grouped_mfilter_num,  grouped_dfilter_num, grouped_filter_num], axis=1).fillna(0)
    print(result)


if __name__ == "__main__":
    segmentation = pd.read_csv("./db_export/segmentations.csv", low_memory=False, parse_dates=["created_at"])
    segmentation["year"] = segmentation["created_at"].map(lambda time: time.isocalendar()[0])
    segmentation["week"] = segmentation["created_at"].map(lambda time: time.isocalendar()[1])
    segmentation = segmentation[~(segmentation.project_id == 3)]


    # user_num_sum(sample=segmentation)
    seg_sum(sample=segmentation)
    # cohort_con(sample=segmentation)
    # chort_snapshot(sample=segmentation)