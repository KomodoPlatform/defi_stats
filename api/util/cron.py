#!/usr/bin/env python3
from datetime import datetime, timedelta


class Cron:
    def __init__(self, from_ts: int = 0, dynamic=False):
        if from_ts == 0:
            self.from_ts = int(self.now_utc())
        else:
            self.from_ts = from_ts
        self.minute = 60
        self.hour = self.minute * 60
        self.day = self.hour * 24
        self.week = self.day * 7
        self.dynamic = dynamic

    def now_utc(self):
        return datetime.utcnow().timestamp()

    def minutes_ago(self, num):
        if self.dynamic:
            self.from_ts = int(self.now_utc())
        return self.from_ts - num * self.minute

    def hours_ago(self, num):
        if self.dynamic:
            self.from_ts = int(self.now_utc())
        return self.from_ts - num * self.hour

    def days_ago(self, num):
        if self.dynamic:
            self.from_ts = int(self.now_utc())
        return self.from_ts - num * self.day

    def weeks_ago(self, num):
        if self.dynamic:
            self.from_ts = int(self.now_utc())
        return self.from_ts - num * self.week

    def daterange(self, start_date, end_date):  # pragma: no cover
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)


cron = Cron(dynamic=True)
