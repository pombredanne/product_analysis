import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import jieba
from raw_process import raw_prepare


def metrics_sum(sample=pd.DataFrame):
    grouped_t_yw = sample.groupby(["year", "week"])["id"].agg("count").rename("total")
    grouped_ah_yw = sample[~(sample.status == "activated")].groupby(["year", "week"])["id"].agg("count").rename("archived")
    grouped_at_yw = sample[sample.status == "activated"].groupby(["year", "week"])["id"].agg("count").rename("activated")

    print(grouped_ah_yw.describe())
    print(grouped_at_yw.describe())

    fig, axes = plt.subplots(nrows=2, ncols=1)
    metrics_growth = pd.concat([grouped_at_yw, grouped_ah_yw, grouped_t_yw], axis=1)
    metrics_growth.plot(ax=axes[0]); axes[0].set_title('Metrics Growth');

    compx = sample[sample.exp_type == "complex"].groupby(["year", "week"])["id"].agg("count").rename("complex")
    compd = sample[sample.exp_type == "compound"].groupby(["year", "week"])["id"].agg("count").rename("compound")
    normal = sample[sample.exp_type == "normal"].groupby(["year", "week"])["id"].agg("count").rename("normal")

    metrics_growth2 = pd.concat([compx, compd], axis=1)
    metrics_growth2.plot(ax=axes[1]); axes[1].set_title('Calculated Metrics Growth');
    plt.show()


def metrics_type(exp:str):
    if len(exp.split("/")) > 1:
        type = "complex"
    elif len(exp.split("+")) > 1:
        type = "compound"
    else:
        type = "normal"
    return type


def metrics_flexp_sum(sample=pd.DataFrame):
   grouped_exp = sample.groupby(["exp_type"])["id"].agg("count").rename("count(*)")
   grouped_pro = round(grouped_exp*100 /np.sum(grouped_exp), 3).rename("pro")
   result = pd.concat([grouped_exp, grouped_pro], axis=1).sort_values(by="count(*)", ascending=False)
   # print(result)

   compx_grouped = sample[sample.exp_type == "complex"].groupby(["status"])["id"].agg("count").rename("complex")
   compx_pro = round(compx_grouped*100 / np.sum(compx_grouped), 2).rename("complex_pro")
   compd_grouped = sample[sample.exp_type == "compound"].groupby(["status"])["id"].agg("count").rename("compound")
   compd_pro = round(compd_grouped * 100 / np.sum(compd_grouped), 2).rename("compound_pro")
   norml_grouped = sample[sample.exp_type == "normal"].groupby(["status"])["id"].agg("count").rename("normal")
   norml_pro = round(norml_grouped * 100 / np.sum(norml_grouped), 2).rename("normal_pro")

   result = pd.concat([compx_grouped, compx_pro, compd_grouped, compd_pro, norml_grouped, norml_pro], axis=1).fillna(0)
   print(result)


def archive_periods(sample=pd.DataFrame):
    archived_metrics = sample[sample.status == "archived"]
    artimedelta = (archived_metrics["updated_at"] - archived_metrics["created_at"]).apply(lambda tdelta : tdelta.total_seconds() / 86400 )
    archived_metrics = pd.concat([archived_metrics, artimedelta], axis=1)

    print(artimedelta)


def parse_metrics_exp(exp=str):
    els = json.loads(exp)['els'][0]['els']
    if len(els) > 1:
        exps = []
        for e in els:
            exps.append(e["action"])
        return "-".join(exps)
    else:
        return els[0]["action"]


def metrics_exp_sum(sample=pd.DataFrame):
   fig, axes = plt.subplots(nrows=2, ncols=1)
   sample["exps_abt"] = sample["expression"].map(lambda exp: parse_metrics_exp(exp))
   com_metrics = sample[sample.exp_type != "normal"].groupby(["exps_abt"])["id"].agg("count").sort_values(ascending=False)
   com_metrics.head().plot.bar(title="Top 5 Calculated Event Combination", ax=axes[0])

   metrics_type = sample.groupby(["exps_abt"])["id"].agg("count").sort_values(ascending=False)
   metrics_type.head().plot.bar(title="Top 5 Event Type", ax=axes[1])
   plt.show()


   c_grouped_p = sample[sample.exps_abt.isin(["clck"])].groupby(["project_id", "exps_abt"])["id"].agg("count").rename("clck")
   i_grouped_p = sample[sample.exps_abt.isin(["imp"])].groupby(["project_id", "exps_abt"])["id"].agg("count").rename("imp")
   p_grouped_p = sample[sample.exps_abt.isin(["page"])].groupby(["project_id", "exps_abt"])["id"].agg("count").rename("page")

   result = pd.concat([c_grouped_p.describe(), i_grouped_p.describe(), p_grouped_p.describe()], axis=1)
   print(result)


def metrics_name_sum(sample=pd.DataFrame):

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
    wc[:20].to_csv("word_count.csv", encoding="utf-8")


if __name__ == "__main__":

    metrics = pd.read_csv("./db_export/metrics.csv", low_memory=False, error_bad_lines=False, parse_dates=["created_at", "updated_at"])

    metrics = raw_prepare(metrics)
    metrics["exp_type"] = metrics["flatten_expression"].map(lambda exp: metrics_type(exp=exp))

    metrics_sum(sample=metrics)
    # metrics_flexp_sum(sample=metrics)
    # archive_periods(sample=metrics)
    # metrics_exp_sum(sample=metrics)
    # metrics_name_sum(sample=metrics)