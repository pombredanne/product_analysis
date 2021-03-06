import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import jieba
from raw_process import raw_prepare
from saviatr import saviatar_speed



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

    grouped_yw = sample.groupby(["year", "week"])["id"].agg("count").rename("Total")
    agrouped_yw = sample[sample.astatus == "alive"].groupby(["year", "week"])["id"].agg("count").rename("Alive")
    dgrouped_yw = sample[sample.astatus == "dead"].groupby(["year", "week"])["id"].agg("count").rename("Dead")


    fig, axes = plt.subplots(nrows=1, ncols=2)
    trending = pd.concat([grouped_yw, agrouped_yw, dgrouped_yw], axis=1).fillna(0)
    trending.plot(ax=axes[0]); axes[0].set_title("Metric Over Time")

    total = len(sample["id"].drop_duplicates())
    active = len(sample[sample.astatus == "alive"]["id"].drop_duplicates())
    dead = len(sample[sample.astatus == "dead"]["id"].drop_duplicates())

    share = pd.Series([ round((active/total)*100, 3) ,  round((dead/total)*100, 3)], index=["Alive", "Dead"])

    share.plot(kind="pie",ax=axes[1], figsize=(6, 6), subplots=True, fontsize=15, ); axes[1].set_title("Share of Status"); axes[1].set_ylabel("")
    print(share)

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


def get_metrics_intfo():
    metrics = pd.read_csv("./db_export/metrics.csv", low_memory=False, error_bad_lines=False,
                          parse_dates=["created_at", "updated_at"])

    metrics = raw_prepare(metrics)
    metrics["exp_type"] = metrics["flatten_expression"].map(lambda exp: metrics_type(exp=exp))
    metrics["created_at"] = metrics["created_at"].map(lambda time: pd.to_datetime(time.strftime("%Y-%m-%d")))

    cols = ["id","project_id", "creator_id",
            "created_at", "status", "exp_type",
            "action", "year", "week", "weekday", "hour"]

    metrics = metrics[cols]
    rename_dic = {
        "id": "metric_id",
        "status": "metric_status",
        "created_at": "metric_created_at",
        "exp_type" : "metrics_exp_type",
        "action" : "metric_action"
    }

    return metrics.rename(columns=rename_dic)


if __name__ == "__main__":

    metrics = pd.read_csv("./db_export/metrics.csv", low_memory=False, error_bad_lines=False, parse_dates=["created_at", "updated_at"])
    metric_event_rule = pd.read_csv("./db_export/growing_metrics_event_rule.csv", low_memory=False)
    nouse = pd.read_csv("./db_export/nouse_metrics_rule_id.csv", low_memory=False)

    nouse_metric_ids = pd.merge(nouse, metric_event_rule, how="left", left_on=["rules_id"], right_on=["rule_id"])["metric_id"].drop_duplicates().sort_values()
    metrics = raw_prepare(metrics)
    metrics["exp_type"] = metrics["flatten_expression"].map(lambda exp: metrics_type(exp=exp))
    metrics["astatus"] = metrics["id"].map(lambda id: "dead" if id in nouse_metric_ids else "alive")

    # metrics_sum(sample=metrics)
    # metrics_flexp_sum(sample=metrics)
    # archive_periods(sample=metrics)
    # metrics_exp_sum(sample=metrics)
    # metrics_name_sum(sample=metrics)