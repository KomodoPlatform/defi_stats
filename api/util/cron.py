#!/usr/bin/env python3
from datetime import datetime, timedelta


def now_utc():
    return datetime.utcnow().timestamp()


def daterange(start_date, end_date):  # pragma: no cover
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


class Time:
    def __init__(self, from_ts: int = 0):
        if from_ts == 0:
            self.from_ts = self.now
        else:
            self.from_ts = from_ts
        self.minute = 60
        self.hour = self.minute * 60
        self.day = self.hour * 24
        self.week = self.day * 7

    @property
    def now(self):  # pragma: no cover
        return int(now_utc())

    def minutes_ago(self, num):
        return self.from_ts - num * self.minute

    def hours_ago(self, num):
        return self.from_ts - num * self.hour

    def days_ago(self, num):
        return self.from_ts - num * self.day

    def weeks_ago(self, num):
        return self.from_ts - num * self.week
