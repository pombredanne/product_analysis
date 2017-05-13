import pandas as pd
import matplotlib.pyplot as plt
from raw_process import raw_prepare


def udf_growth(cs=pd.DataFrame, ps=pd.DataFrame):

    csgrouped_yw = cs.groupby(["year", "week"])["id"].agg("count").rename("CS udf")
    psgrouped_yw = ps.groupby(["year", "week"])["id"].agg("count").rename("PS udf")

    result = pd.concat([csgrouped_yw, psgrouped_yw], axis=1).fillna(0)
    result.plot(title="UDF Integration Growth")

    plt.show()


def udf_project_share(cs=pd.DataFrame, ps=pd.DataFrame, pids=pd.DataFrame):

    cs_pids = cs["project_id"].drop_duplicates()
    ps_pids = ps["project_id"].drop_duplicates()

    cs_share = round(len(cs_pids) * 100  / len(pids), 3)
    ps_share = round(len(ps_pids) * 100  / len(pids), 3)

    cs_result = pd.Series([cs_share, 100 - cs_share], index=["CS", "No CS"])
    ps_result = pd.Series([ps_share, 100 - ps_share], index=["PS", "No PS"])

    cp_pids = pids[(pids.isin(cs_pids)) & (pids.isin(ps_pids))]
    cp_share = round(len(cp_pids)*100 / len(pids), 3)

    cp_result = pd.Series([cp_share, 100 - cp_share], index=["CS & PS", "~(CS & PS)"])

    fig, axes = plt.subplots(nrows=1, ncols=3)
    cs_result.plot(kind='pie', ax=axes[0]); axes[0].set_title("Share of Project with CS")
    ps_result.plot(kind='pie', ax=axes[1]); axes[1].set_title("Share of Project with PS")
    cp_result.plot(kind='pie', ax=axes[2]); axes[2].set_title("Share of Project with CS & PS")

    print(cs_result)
    print(ps_result)
    print(cp_result)

    plt.show()


def udf_project_sum(cs=pd.DataFrame, ps=pd.DataFrame):

    cgroupedp = cs.groupby(["project_id"])["id"].agg("count").describe().rename("CS UDF Project Summary")
    pgroupedp = ps.groupby(["project_id"])["id"].agg("count").describe().rename("PS UDF Project Summary")

    result = pd.concat([cgroupedp, pgroupedp], axis=1)
    print(result)

    r = cs.groupby(["cs"])["id"].agg("count").sort_values(ascending=False)
    print(r)


if __name__ == '__main__':
    cs_udf = pd.read_csv("./chart_dashboard/cs.csv", parse_dates=["created_at", "updated_at"])
    ps_udf = pd.read_csv("./chart_dashboard/ps.csv", parse_dates=["created_at", "updated_at"])
    pids = pd.read_csv("./0508/user_project_org_info.csv", low_memory=False)
    pids = pids[~pids.first_date_of_getting_pv.isnull()]["project_id"].drop_duplicates().sort_values()

    cs_udf = raw_prepare(cs_udf)
    ps_udf = raw_prepare(ps_udf)

    # udf_growth(cs=cs_udf, ps=ps_udf)
    # udf_project_share(cs=cs_udf, ps=ps_udf, pids=pids)
    udf_project_sum(cs=cs_udf, ps=ps_udf)


