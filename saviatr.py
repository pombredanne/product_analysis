import pandas as pd
import numpy as np
from raw_process import raw_prepare


def saviatar_speed(s=pd.DataFrame):
    s.loc[:, "created_at"] = s.loc[:, "created_at"].map(lambda time: pd.to_datetime(time.strftime("%Y-%m-%d")))
    pidgf = s[["id", "project_id", "created_at"]].sort_values(["project_id", "created_at"])
    beg = (pidgf.project_id != pidgf.project_id.shift(1))
    pidgf["dur"] = (pidgf["created_at"] - pidgf["created_at"].where(beg).ffill()) / np.timedelta64(1, 'D')
    pidgf["diffs"] = (pidgf["created_at"] - pidgf["created_at"].shift(1)) / np.timedelta64(1, 'D')
    pidgf.ix[beg, "diffs"] = np.nan

    pids = pidgf["project_id"].drop_duplicates()
    durs = []
    diffs = []
    for pid in pids:
        durs.append(pidgf[pidgf.project_id == pid]["dur"].rename(pid).reset_index(drop=True))
        diffs.append(pidgf[pidgf.project_id == pid]["diffs"].rename(pid).reset_index(drop=True))

    result = pd.concat(durs, axis=1)
    result2 = pd.concat(diffs, axis=1)

    nnull_count = result.notnull().sum(axis=1).rename("count")
    mean_time = result.mean(axis=1).rename("mean")
    mean_rdtime = result2.mean(axis=1).rename("rd mean")
    time_std = result.std(axis=1).rename("std")
    rdtime_std = result2.std(axis=1).rename("rd std")

    result3 = pd.concat([nnull_count, mean_time , time_std, mean_rdtime, rdtime_std], axis=1)
    result3 = result3.set_index(np.arange(1, len(result3)+1, 1))
    result3.index.name = "nth"

    return result3


if __name__ == '__main__':
    funnel = pd.read_csv("./chart_dashboard/funnels.csv", parse_dates=["created_at"])
    funnel = raw_prepare(funnel)
    result =  saviatar_speed(funnel)
    print(result.head(20))

