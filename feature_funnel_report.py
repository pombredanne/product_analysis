import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def funnel_conversion_window(sample=pd.DataFrame):
    grouped = sample.groupby(["conversion_window"])["id"].agg("count")
    print(grouped)


def funnel_timepicker(sample=pd.DataFrame):
    sample["picker_type"] = sample["range"].map(lambda range: range.split(":")[0])
    picker_type_sum = sample.groupby(["picker_type"])["id"].agg("count").sort_values(ascending=False)
    grouped = sample.groupby(["year", "week", "picker_type"])["id"].agg("count")

    #    # for typei in picker_type_sum.index:    #     grouped.xs(typei, level="picker_type").plot(label=typei)    #    # plt.legend()    # plt.show()

def funnel_steps(sample=pd.DataFrame):

    sample["steps_count"] = sample["steps"].map(lambda steps: len(steps.split(",")))
    grouped_steps = sample.groupby(["steps_count"])["id"].agg("count")
    grouped_time = sample.groupby(["year", "week"])["steps_count"].agg([np.mean, np.median, np.std])
    grouped = sample.groupby(["year", "week", "steps_count"])["id"].agg("count")

    for typei in grouped_steps.index:
        grouped.xs(typei, level="steps_count").plot(label=typei)

    plt.legend()
    # grouped_steps.plot.bar()    # grouped_time.plot()    plt.show()


def funnel_growth(sample=pd.DataFrame):

    grouped_status = sample.groupby(["year", "week"])["id"].agg("count")
    a_funnel = sample[sample.status == "activated"].groupby(["year", "week"])["id"].agg("count")
    del_funnel = sample[sample.status == "deleted"].groupby(["year", "week"])["id"].agg("count")

    grouped_status.plot(label="Total Funnels")
    a_funnel.plot(label="Activated Funnels")
    del_funnel.plot(label="Deleted Funnels")

    plt.legend()
    plt.show()


if __name__ == "__main__":

    funnel_reports = pd.read_csv("project_funnel_report.csv", parse_dates=["created_at"])

    funnel_reports["year"] = funnel_reports["created_at"].map(lambda time: time.isocalendar()[0])
    funnel_reports["week"] = funnel_reports["created_at"].map(lambda time: time.isocalendar()[1])
    funnel_mean_week = funnel_reports.groupby(["year", "week"])[["id"]].agg("count").mean()


    # funnel_growth(sample=funnel_reports)    # funnel_steps(sample=funnel_reports)    funnel_timepicker(sample=funnel_reports)
    # funnel_conversion_window(sample=funnel_reports)