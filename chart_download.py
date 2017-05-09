import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


if __name__ == "__main__":
    downalod =  open("chart_download.txt").read().split(",")
    downalod = pd.DataFrame({"chart_id" : downalod }, dtype="str")

    charts = pd.read_csv("./chart_dashboard/charts.csv", low_memory=False, dtype={"id":str})

    result = pd.merge(downalod, charts, how="left", left_on=["chart_id"], right_on=["id"])

    grouped_type = result.groupby(["chart_type"])["id"].agg("count")
    grouped_type = grouped_type / np.sum(grouped_type)

    grouped_type.plot(kind="pie", figsize=(6, 6), subplots=True, autopct='%.2f', fontsize=20)

    plt.show()
