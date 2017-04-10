# -*- coding: utf-8 -*- 
import pandas as pd
import numpy as np 
from functools import reduce
#
user_data_sim_df = pd.read_csv("./user_data_all_sim.txt",dtype={"user_id" : str, "year" : np.int64, "week_num":np.int64})

file_names = {
    "user_session"     : "1201-0330 user_访问量&访问时长.csv",
    "user_org_info"    : "user_org_info.txt",
    "user_behavior_view"    : "1228-0327 FQY_主要功能数据_U_user_table_PV浏览类.txt",
    "user_behavior_click"   : "1228-0327 FQY_主要功能数据_U_user_table_action交互类.txt"
}

parse_dates = ['时间']

user_org_info_df = pd.read_csv(file_names["user_org_info"],dtype="str")
# user_sessions_df  = pd.read_csv(file_names["user_session"],encoding="utf-16",sep="\t",dtype={"user" : str, "访问时长 (分钟)" : str, "访问量":np.int64}, parse_dates=parse_dates)
user_sessions_df = pd.read_csv("./1201-0330 user_访问量&访问时长.csv",dtype="str",parse_dates=parse_dates)
user_sessions_df["year"] = user_sessions_df["时间"].dt.year
user_sessions_df["week_num"] = user_sessions_df["时间"].dt.week


user_behavior_view_df = pd.read_csv(file_names["user_behavior_view"],encoding="utf-8",sep="\t",dtype={'user':str, '主要功能——单图细节（分析师专用勿改）_页面浏览量':np.int64, 'fqy-单图列表页面_页面浏览量':np.int64,
 'fqy-创建单图页面_页面浏览量' : np.int64, '单图-编辑页面(分析师专用勿改)_页面浏览量' : np.int64, '看板全部页面（分析师专用勿改）_页面浏览量' :np.int64,
 'fqy-创建看板页面_页面浏览量':np.int64, '看板-编辑页面(分析师专用勿动)_页面浏览量' : np.int64, 'fqy-看板列表页面(勿动)_页面浏览量' : np.int64,
 '主要功能——漏斗全部_页面浏览量':np.int64, '主要功能——留存全部页面_页面浏览量':np.int64, '主要功能——用户细查_页面浏览量' : np.int64,
 '主要功能——实时_页面浏览量':np.int64, '热图使用情况用这个指标_浏览量':np.int64, '新建——漏斗_页面浏览量':np.int64},parse_dates=parse_dates)

user_behavior_click_df = pd.read_csv(file_names["user_behavior_click"],sep="\t",encoding="utf-8",dtype={'user':str, '看板-套用筛选条件控件(分析师专用勿改)_点击量':np.int64, '看板-套用分群控件(分析师专用勿改)_点击量':np.int64,
 '看板-自定义时间页卡(分析师专用勿改)_点击量':np.int64, '看板-创建看板-保存按钮(分析师专用勿动)_点击量':np.int64,
 'ZSQ-编辑看板-更新按钮(分析师专用勿改)_点击量':np.int64, '单图-详情页-用户分群过滤按钮(分析师专用勿改)_点击量':np.int64,
 '单图-详情页-数据过滤按钮(分析师专用勿改)_点击量':np.int64, '单图-详情页-时间控件按钮(分析师专用勿改)_点击量':np.int64,
 'fqy-创建单图-保存_点击量' '单图——另存(分析师专用勿改)_点击量' :np.int64,'ZSQ-单图编辑页面-更新按钮(分析师专用勿改)_点击量':np.int64,
 '单图-列表页条件筛选控件(分析师专用勿改)_点击量':np.int64, '单图-列表页时间控件(分析师专用勿改)_点击量':np.int64,
 '可视化分析操作 - 漏斗分析 - 維度選擇_点击量':np.int64, 'zjl-漏斗页面-切换时间_点击量':np.int64,
 '可视化分析操作 - 漏斗分析 - 漏斗趨勢_点击量':np.int64, '漏斗——保存_点击量' '留存-留存率时间切换（分析师用勿改）_点击量':np.int64,
 '留存-选择留存切分维度/分群_点击量':np.int64, 'zyw-留存2.0-选择功能/起始行为/回访行为_点击量':np.int64 }, parse_dates=['时间'])


dfs = [user_sessions_df, user_behavior_view_df, user_behavior_click_df]
behaviors = reduce(lambda left, right: pd.merge(left,right,how="left", on=["user","时间"]),dfs)

tmp4 = pd.merge(user_data_sim_df,behaviors,how="left",left_on=["user_id","year","week_num"],right_on=["user","year","week_num"])
result = pd.merge(tmp4,user_org_info_df,how="inner",on="user_id")
print(len(result))

