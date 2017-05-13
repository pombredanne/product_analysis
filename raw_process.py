import pytz
local_tz = "Asia/Shanghai"


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