import pytz
import numpy as np
local_tz = pytz.timezone("Asia/Shanghai")


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)

def raw_prepare(sample):

    sample = sample[sample.project_id != 3]
    sample = sample.assign(created_at=lambda df: df["created_at"].map(lambda time: utc_to_local(time)))
    sample = sample.assign(year=lambda df: df["created_at"].map(lambda time: time.isocalendar()[0]))
    sample = sample.assign(week=lambda df: df["created_at"].map(lambda time: time.isocalendar()[1]))
    sample = sample.assign(hour=lambda df: df["created_at"].map(lambda time: time.hour))
    sample = sample.assign(weekday=lambda df: df["created_at"].map(lambda time: time.isoweekday()))

    return sample

def days_convertor(timed=""):
    unit = timed.split(":")[0]
    dur = timed.split(":")[1]

    if unit == "abs":
        dur = int(dur.split(",")[1]) - int(dur.split(",")[0])
        return int( dur / ( 86400 * 1000) )
    elif unit == "day":
        if dur == "prev":
            return 7
        else:
            return int(dur.split(",")[0]) - int(dur.split(",")[1])
    elif unit == "month":
        if dur == "prev":
            return 30
        else:
            return ( int(dur.split(",")[0]) - int(dur.split(",")[1]) )*30
    else:
        if dur == "prev":
            return 7
        else:
            return  ( int(dur.split(",")[0]) - int(dur.split(",")[1]) )*7

def get_col_dtype():

    common = {
        "user" : np.str,
        "user_id" : np.str
    }

    viewdtype = {
        "chart_all_pv" : np.int32,
        "chart_list_pv" : np.int32,
        "create_chart_pv" : np.int32,
        "edit_chart_pv" : np.int32,
        "dashboard_all_pv" : np.int32,
        "create_dashboard_pv" : np.int32,
        "edit_dashboard_pv" : np.int32,
        "dashboard_list_pv" : np.int32,
        "funnel_all_pv" : np.int32,
        "retention_all_pv" : np.int32,
        "user_details_pv": np.int32,
        "realtime_pv" : np.int32,
        "heatmap_use_imp": np.int32,
        "create_funnel_pv" : np.int32,
        "scene_list_pv": np.int32 ,
        "scene_all_pv" : np.int32
    }

    clickdtype = {
        "dashboard_filter_clck" : np.int32,
        "dashboard_usercohort_clck" : np.int32,
        "dashboard_time_clck" : np.int32,
        "dashboard_create_save_clck" : np.int32,
        "dashboard_edit_update_clck": np.int32,
        "chart_detail_usercohort_clck" : np.int32,
        "chart_detail_filter_clck": np.int32,
        "chart_detail_time_clck": np.int32 ,
        "chart_create_save_clck": np.int32,
        "chart_edit_savetoanother_clck" : np.int32,
        "chart_edit_update_clck" : np.int32,
        "chart_list_filter_clck": np.int32,
        "chart_list_time_clck" : np.int32,
        "funnel_Dimension_clck": np.int32,
        "funnel_time_clck" : np.int32,
        "funnel_trend_clck" : np.int32,
        "funnel_create_save_clck" : np.int32,
        "retention_time_clck" : np.int32,
        "retention_Dimension_clck" : np.int32,
        "retention_detail_behavior_clck" : np.int32,
        "scene_time_clck": np.int32,
        "scene_filter_clck": np.int32,
        "scene_usercohort_clck": np.int32
    }

    session_dtype = {
        "avg_duration" : np.float,
        "visits" : np.int32
    }

    user_dtype = {
        "project_id" : np.int32,
        "user_id_project" : np.int32,
        "role" : np.str,
        "user_id_user" : np.int32,
        "user_id_organization" : np.int32,
        "name" : np.str,
        "email" : np.str,
        "mobile" :np.str,
        "user_life" : np.int32,
        "department" : np.str,
        "title" : np.str,
        "org_id_organization" : np.int32,
        "org_name" : np.str,
        "industry" : np.str,
        "project_id_2" : np.int32,
        "project_name" : np.str,
        "org_id_projects" : np.int32,
        "ai" : np.str,
        "days_get_pv" : np.int32,
        "pv" : np.int32,
        "uv" : np.int32,
        "pay_status" : np.str,
        "lead_source" : np.str,
        "level" : np.str,
        "csm" : np.str,
        "sale" : np.str,
        "accountid" : np.int32
    }

    dtdtype = ["Date", "user_created_at", "org_created_at", "project_created_at", "first_date_of_getting_pv", "latest_date_of_getting_pv", "sim_date"]

    col_dtype = {**common, **session_dtype, **viewdtype, **clickdtype}

    return (col_dtype, dtdtype)