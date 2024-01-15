#!/usr/bin/env python3
import os
import sys
import time
from datetime import date, datetime, timedelta
from datetime import time as dt_time

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
from db import sqldb


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_dt = date(2023, 1, 1)
end_dt = date(2024, 1, 1)
for dt in daterange(start_dt, end_dt):
    print(f"Importing swaps from {dt.strftime('%Y-%m-%d')} {dt}")
    start_ts = datetime.combine(dt, dt_time()).timestamp()
    end_ts = datetime.combine(dt, dt_time()).timestamp() + 86400
    print(f"Importing swaps from {start_ts} - {end_ts}")
    sqldb.populate_pgsqldb(start=start_ts, end=end_ts)
    time.sleep(2)
