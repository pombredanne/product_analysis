import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def rule_growth(sample=pd.DataFrame):
    grouped_platform = sample.groupby(["year", "week"])["id"].agg("count").rename("Total")
    # print(grouped_platform)

    # grouped_yw = sample.groupby(["year", "week"])["id"].agg("count").rename("total")
    # agrouped_yw = sample[sample.activated == True].groupby(["year", "week"])["id"].agg("count").rename("activated")
    # nagrouped_yw = sample[(sample.activated == False)].groupby(["year", "week"])["id"].agg("count").rename("no activated")

    fig, axes = plt.subplots(nrows=1, ncols=2)
    wground = sample[sample.platform == "web"].groupby(["year", "week"])["id"].agg("count").rename("web")
    wper = np.sum(wground) / np.sum(grouped_platform)
    iground = sample[sample.platform == "iOS"].groupby(["year", "week"])["id"].agg("count").rename("iOS")
    iper = np.sum(iground) / np.sum(grouped_platform)
    aground = sample[sample.platform == "Android"].groupby(["year", "week"])["id"].agg("count").rename("Android")
    aper = np.sum(aground) / np.sum(grouped_platform)

    result = pd.concat([wground, iground, aground, grouped_platform], axis=1)
    percentage = pd.Series([wper, iper, aper], index=["web", "iOS", "Android"])

    result.plot(ax=axes[0])
    percentage.plot(ax=axes[1], kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15)
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


def type_converter(exp=str):
    if exp == "=:chng":
        return "change"
    elif exp == "=:clck":
        return "click"
    elif exp == "=:imp":
        return "impression"
    else:
        return "page"


def rules_type(sample=pd.DataFrame):

    position = sample[(sample["content"].isnull()) & (sample["href"].isnull()) & (~sample["index"].isnull()) ]
    element_pattern = sample[(sample["content"].isnull()) & (sample["href"].isnull()) & (sample["index"].isnull()) ]
    element = sample[(~sample["id"].isin(position["id"])) & (~sample["id"].isin(element_pattern["id"]))]

    fig, axes = plt.subplots(nrows=1, ncols=2)
    sample["action"] = sample["action"].map(lambda exp: type_converter(exp))
    action_type = sample.groupby(["action"])["ai"].agg("count")
    action_type.plot(ax=axes[0], kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15)

    type_count = pd.Series([len(position), len(element_pattern), len(element)], index=["Postion", "Pattern Matching", "Element"])
    type_count = round(type_count / np.sum(type_count), 3)
    type_count.plot(ax=axes[1], kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=15)

    plt.show()


if __name__ == "__main__":
    rules = pd.read_csv("./db_export/rules.csv", low_memory=False, parse_dates=["created_at", "updated_at"], dtype=str)
    rules = rules[(~rules["created_at"].isnull()) & ~(rules["ai"] == "0a1b4118dd954ec3bcc69da5138bdb96") ]
    rules["year"] = rules["created_at"].map(lambda time: time.isocalendar()[0])
    rules["week"] = rules["created_at"].map(lambda time: time.isocalendar()[1])
    rule_growth(sample=rules)
    # rule_action_sum(sample=rules)
    # rules_type(sample=rules)