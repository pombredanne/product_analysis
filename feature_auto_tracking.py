import pandas as pd


if __name__ == "__main__":

    rules = pd.read_csv("./metrics_rules.csv", parse_dates=["created_at"], dtype={"xpath": str}).dropna()

    ai_grouped = rules.groupby(["ai"]).agg("count")

    print(len(ai_grouped.index))

    # total_rules = len(rules.index)
    # pattern_tracking = rules[( rules["href"].isnull() & rules["index"].isnull() & rules["content"].isnull())]
    # ep_tracking = rules[(~rules["href"].isnull() | ~rules["index"].isnull() | ~rules["content"].isnull())]
    # pos_tracking = ep_tracking[ep_tracking["xpath"].map(lambda xpath: len(xpath.split(",")) > 1)]
    #
    # print(pos_tracking)




