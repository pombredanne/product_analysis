import pandas as pd
import numpy as np
import re
import json

def parse_metric_id(s=pd.DataFrame):

    s["ometrics"] = s["metrics"].map(lambda exp: re.compile("{(.*?)}").findall(exp)[0].split(","))

    cms = []
    for sid in s.index:
        for si in s["ometrics"][sid]:
            cms.append((s["id"][sid], si))

    cms = pd.DataFrame.from_records(cms, columns=["cid", "metric_id"])
    result = pd.merge(s, cms, how="left", left_on=["id"], right_on=["cid"])

    return result[["id","name", "metrics", "metric_id"]]


def parse_steps(steps=""):
    try:
        return re.compile("m_(.*?)_").findall(steps)
    except:
        return re.compile("e_(.*?)_").findall(steps)


def parse_flatten_m(s=pd.DataFrame, m=pd.DataFrame):

    s = pd.merge(s,m[["id", "project_id", "name", "status", "flatten_expression"]], how="left", left_on=["metric_id"], right_on=["id"])
    s = s[~s.project_id.isnull()]
    s["fla_exp_ometric"] = s["flatten_expression"].map(lambda exp: parse_steps(exp))

    cms = []
    for sid in s.index:
        for si in s["fla_exp_ometric"][sid]:
            cms.append((s["id_y"][sid], si))

    cms = pd.DataFrame.from_records(cms, columns=["omid", "ometric_id"])
    result = pd.merge(s, cms, how="left", left_on=["id_y"], right_on=["omid"])

    return result



if __name__ == '__main__':
    charts = pd.read_csv("./chart_dashboard/charts.csv", low_memory=False)
    charts = charts[charts.status == "activated"]
    charts = parse_metric_id(s=charts)

    metrics = pd.read_csv("./db_export/metrics.csv", low_memory=False, error_bad_lines=False, dtype={"id":str})
    charts = parse_flatten_m(s=charts, m=metrics)

    gmer = pd.read_csv("./db_export/growing_metrics_event_rule.csv", low_memory=False, dtype={"ometric_id":str})
    gmer["action"] = gmer["expression"].map(lambda exp: json.loads(exp)['els'][0]['els'][0]["action"])
    charts = pd.merge(charts, gmer, how="left", left_on=["omid"], right_on=["ometric_id"])
    charts = charts[~charts.rule_id.isnull()]

    group_action = charts.groupby(["action"])["id_x"].agg("count")
    group_actionp = round( group_action*100  / np.sum(group_action),3 )

    result = pd.concat([group_action, group_actionp], axis=1, names=["count", "share"])
    print(result)
    # charts.to_csv("chart_metric_action.csv")