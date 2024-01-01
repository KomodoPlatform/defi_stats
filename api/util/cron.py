#!/usr/bin/env python3
import time


class Time:
    def __init__(self, from_ts: int = int(time.time())):
        self.from_ts = from_ts
        self.minute = 60
        self.hour = self.minute * 60
        self.day = self.hour * 24
        self.week = self.day * 7

    def now(self):  # pragma: no cover
        return int(time.time())

    def minutes_ago(self, num):
        return self.from_ts - num * self.minute

    def hours_ago(self, num):
        return self.from_ts - num * self.hour

    def days_ago(self, num):
        return self.from_ts - num * self.day

    def weeks_ago(self, num):
        return self.from_ts - num * self.week
