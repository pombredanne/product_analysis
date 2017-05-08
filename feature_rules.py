import pandas as pd
import matplotlib.pyplot as plt


def rule_growth(sample=pd.DataFrame):
    # grouped_platform = sample.groupby(["platform", "activated"])["id"].agg("count").rename("count(*)").reset_index().pivot(index="activated", columns="platform")
    # print(grouped_platform)

    grouped_yw = sample.groupby(["year", "week"])["id"].agg("count").rename("total")
    agrouped_yw = sample[sample.activated == True].groupby(["year", "week"])["id"].agg("count").rename("activated")
    nagrouped_yw = sample[~(sample.activated == True)].groupby(["year", "week"])["id"].agg("count").rename("no activated")
    result = pd.concat([agrouped_yw, nagrouped_yw, grouped_yw], axis=1)

    result.plot()
    plt.show()


def rule_action_sum(sample=pd.DataFrame):
    # grouped = sample.groupby(["action", "activated"])["id"].agg("count")
    #
    # print(grouped)

    chng = sample[sample.action == "=:chng"].groupby(["ai"])["id"].agg("count")
    clck = sample[sample.action == "=:clck"].groupby(["ai"])["id"].agg("count")
    imp = sample[sample.action == "=:imp"].groupby(["ai"])["id"].agg("count")
    page = sample[sample.action == "=:page"].groupby(["ai"])["id"].agg("count")

    result = pd.concat([chng, clck, imp, page], axis=1).fillna(0)
    print(result.describe())


if __name__ == "__main__":
    rules = pd.read_csv("./db_export/rules.csv", low_memory=False, parse_dates=["created_at", "updated_at"])
    rules = rules[(~rules["created_at"].isnull()) & ~(rules["ai"] == "0a1b4118dd954ec3bcc69da5138bdb96") ]
    rules["year"] = rules["created_at"].map(lambda time: time.isocalendar()[0])
    rules["week"] = rules["created_at"].map(lambda time: time.isocalendar()[1])
    # rule_growth(sample=rules)
    rule_action_sum(sample=rules)