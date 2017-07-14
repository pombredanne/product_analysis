import hashlib
import hmac
import time
import requests
import json
import datetime
from threading import Timer


def authToken(secret, project, ai, tm):
    message = ("POST\n/auth/token\nproject=" + project + "&ai=" + ai + "&tm=" + tm).encode('utf-8')
    signature = hmac.new(bytes(secret.encode('utf-8')), bytes(message), digestmod=hashlib.sha256).hexdigest()
    return signature

def get_code(prkey, pid, ai, tm, a):
    headers = {"X-Client-Id": prkey}
    r_code = requests.post(
        "https://gta.growingio.com/auth/token" + "?ai=" + ai + "&project=" + pid + "&tm=" + tm + "&auth=" + a,
        headers=headers)
    return json.loads(r_code.text)["code"]

def query_request(chart_id, startDate, endDate, prkey, code, interval):
    headers = {"X-Client-Id": prkey, "Authorization": code}
    url =  "https://gta.growingio.com/projects/nxog09md/charts/j9yDBAZo.json" + "?startTime=" + startDate + "&endTime=" + endDate + "&interval=" + interval
    return requests.get(url=url, headers=headers)


def scheduled_query():

    pkey = "9dahxn76jr9ghlrfequiua8cflnlwlxfy8krtl3s"
    prkey = "7f8it37dxdt91x4n5cvuvccc1cgaqe22"
    ai = "0a1b4118dd954ec3bcc69da5138bdb96"
    pid = "nxog09md"
    tm = str(int(time.time() * 1000))

    chart_ids = ["1R36x1x9", "39lknBgo", "lPQelrQP"]
    sig = authToken(secret=pkey, project=pid, ai=ai, tm=tm)
    code = get_code(prkey=prkey, pid=pid, ai=ai, tm=tm, a=sig)
    interval = "86400000"

    endDate = str(int(datetime.datetime(2017, 7, 2).timestamp() * 1000 - 1))
    startDate = str(int(datetime.datetime(2017, 6, 2).timestamp() * 1000))

    for cid in chart_ids:
        try:
            result = query_request(cid, startDate, endDate, prkey, code, interval)
            print(result.text)
        except:
            print("Somthing goes wrong")
    return


if __name__ == "__main__":

    pkey = "9dahxn76jr9ghlrfequiua8cflnlwlxfy8krtl3s"
    prkey = "7f8it37dxdt91x4n5cvuvccc1cgaqe22"
    ai = "0a1b4118dd954ec3bcc69da5138bdb96"
    pid = "nxog09md"
    tm = str(int(time.time() * 1000))
    mins = 10

    chart_ids = ["1R36x1x9", "39lknBgo", "lPQelrQP" ]
    sig = authToken(secret=pkey, project=pid, ai=ai, tm=tm)
    code = get_code(prkey=prkey, pid=pid, ai=ai, tm=tm, a=sig)
    interval = "86400000"

    endDate = str(int(datetime.datetime(2017, 7, 2).timestamp() * 1000 - 1))
    startDate = str(int(datetime.datetime(2017, 6, 2).timestamp() * 1000))

    for cid in chart_ids:
        try:
            query_request(cid, startDate, endDate, prkey, code, interval)
            Timer(10, requests.Session.close)
        except:
            print("Somthing goes wrong")

    t = Timer(mins * 60, scheduled_query)
    t.start()
