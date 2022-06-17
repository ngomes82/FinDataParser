import schedule
import time
from dateutil import parser
from datetime import datetime
import json

f = open("SourceData/portfolio.json")

portfolio = json.load(f)
startDate = parser.parse(portfolio["startDate"])


def job():
    global startDate
    global portfolio
    
    earningsPerSecond = portfolio["earnings"] / (60*60*24*365)
    divPerSecond = portfolio["dividends"] / (60*60*24*365)

    dt = datetime.now() - startDate
    totalEarnings = earningsPerSecond * dt.total_seconds()
    totalCash = divPerSecond * dt.total_seconds()

    totalEarnings = "{:.5f}".format(totalEarnings)
    totalCash = "{:.5f}".format(totalCash)

    print(f"Earnings: ${totalEarnings} -- Cash: ${totalCash} \r", end="")


schedule.every(1).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)