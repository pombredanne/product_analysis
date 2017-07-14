import hashlib
import hmac
import time
import requests
import json
import datetime
from threading import Timer
from apscheduler.schedulers.blocking import BlockingScheduler

pkey = "9dahxn76jr9ghlrfequiua8cflnlwlxfy8krtl3s"
prkey = "7f8it37dxdt91x4n5cvuvccc1cgaqe22"
ai = "0a1b4118dd954ec3bcc69da5138bdb96"
pid = "nxog09md"
tm = str(int(time.time() *1000))
endDate = str(int(datetime.datetime(2017, 7, 2).timestamp() * 1000 -1 ))


def authToken(secret, project, ai, tm):
    message = ("POST\n/auth/token\nproject=" + project + "&ai=" + ai + "&tm=" + tm).encode('utf-8')
    signature = hmac.new(bytes(secret.encode('utf-8')), bytes(message), digestmod=hashlib.sha256).hexdigest()
    return signature

a = authToken(pkey, pid, ai, tm)

headers = {"X-Client-Id": prkey}
r_code = requests.post("https://gta.growingio.com/auth/token" + "?ai=" + ai + "&project=" + pid + "&tm=" + tm + "&auth=" + a, headers=headers)
code = json.loads(r_code.text)["code"]

startDate = str(int(datetime.datetime(2017, 6, 2).timestamp() * 1000))

try:
    headers = {"X-Client-Id": prkey, "Authorization": code}
    r = requests.get(
        "https://gta.growingio.com/projects/nxog09md/charts/j9yDBAZo.json" + "?startTime=" + startDate + "&endTime=" + endDate + "&interval=" + "86400000",
        headers=headers)

    Timer(10, requests.Session.close)

except:
    print("Something goes wrong")

min = 10

def data_query():
    tm = str(int(time.time() * 1000))
    a = authToken(pkey, pid, ai, tm)

    headers = {"X-Client-Id": prkey}
    r_code = requests.post(
        "https://gta.growingio.com/auth/token" + "?ai=" + ai + "&project=" + pid + "&tm=" + tm + "&auth=" + a,
        headers=headers)
    print(r_code)
    code = json.loads(r_code.text)["code"]

    print(code)


    headers = {"X-Client-Id": prkey, "Authorization": code}
    r = requests.get(
        "https://gta.growingio.com/projects/nxog09md/charts/j9yDBAZo.json" + "?startTime=" + startDate + "&endTime=" + endDate + "&interval=" + "86400000",
        headers=headers)
    print(r.text)


t = Timer( min * 60, data_query)
t.start()
