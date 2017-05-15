import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from raw_process import raw_prepare
from raw_process import days_convertor
import jieba


def funnel_conversion_window(sample=pd.DataFrame):
    grouped = sample.groupby(["conversion_window"])["id"].agg("count")
    grouped.plot.bar()
    plt.show()


def funnel_timepicker(sample=pd.DataFrame):
    sample["picker_type"] = sample["range"].map(lambda range: range.split(":")[0])
    picker_type_sum = sample.groupby(["picker_type"])["id"].agg("count").sort_values(ascending=False)
    grouped = sample.groupby(["year", "week", "picker_type"])["id"].agg("count")

    fig, axes = plt.subplots(nrows=1, ncols=2)
    for typei in picker_type_sum.index:
        grouped.xs(typei, level="picker_type").plot(label=typei)

    picker_type_sum.plot(kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15, ax=axes[0])
    plt.legend()
    plt.show()

def funnel_steps(sample=pd.DataFrame):

    sample["steps_count"] = sample["steps"].map(lambda steps: len(steps.split(",")))
    grouped_steps = sample.groupby(["steps_count"])["id"].agg("count")
    grouped_time = sample.groupby(["year", "week"])["steps_count"].agg([np.mean, np.median, np.std])
    grouped = sample.groupby(["year", "week", "steps_count"])["id"].agg("count")

    fig, axes = plt.subplots(nrows=1, ncols=2)
    for typei in grouped_steps.index:
        grouped.xs(typei, level="steps_count").plot(label=typei)

    grouped_steps.plot(kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15, ax=axes[0])

    plt.legend()
    plt.show()


def funnel_growth(sample=pd.DataFrame):

    grouped_status = sample.groupby(["year", "week"])["id"].agg("count")
    a_funnel = sample[sample.status == "activated"].groupby(["year", "week"])["id"].agg("count")
    del_funnel = sample[sample.status == "deleted"].groupby(["year", "week"])["id"].agg("count")

    grouped_status.plot(label="Total Funnels")
    a_funnel.plot(label="Activated Funnels")
    del_funnel.plot(label="Deleted Funnels")

    plt.legend()
    plt.show()

def funnel_name_sum(sample=pd.DataFrame):

    jieba.enable_parallel(4)
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
    print("Start to Output Top 20 words")
    wc[:20].to_csv("funnel_word_count.csv", encoding="utf-8")


def get_funnel_info():
    funnel_reports = pd.read_csv("./chart_dashboard/funnels.csv", parse_dates=["created_at"])
    funnel_reports = raw_prepare(funnel_reports)
    funnel_reports["created_at"] = funnel_reports["created_at"].map(lambda time: pd.to_datetime(time.strftime("%Y-%m-%d")))

    funnel_reports["steps"] = funnel_reports["steps"].map(lambda steps: len(steps.split(",")))
    funnel_reports["range"] = funnel_reports["range"].map(lambda range: days_convertor(range))

    cols = ["id", "project_id","creator_id",
            "steps", "status", "range",
            "status", "platform", "conversion_window",
            "created_at", "year", "week", "weekday", "hour"]

    rename_dic = {
        "id" : "funnel_id",
        "steps" : "funnel_steps",
        "status" : "funnel_status",
        "range" : "funnel_range",
        "platform" : "funnel_platform",
        "conversion_window" : "funnel_cw",
        "created_at": "funnel_created_at"
    }

    return funnel_reports[cols].rename(columns=rename_dic)


if __name__ == "__main__":

    funnel_reports = pd.read_csv("./chart_dashboard/funnels.csv", parse_dates=["created_at"])
    funnel_reports = raw_prepare(funnel_reports)

    # print(get_funnel_info())

    # funnel_growth(sample=funnel_reports)
    # funnel_steps(sample=funnel_reports)
    # funnel_timepicker(sample=funnel_reports)
    # funnel_conversion_window(sample=funnel_reports)
    # funnel_name_sum(sample=funnel_reports)

