#!/usr/bin/env python3
import time
from fixtures_cron import (
    setup_time,
)



def test_time(setup_time):
    _time = setup_time
    assert _time.from_ts == 1700000000
    assert isinstance(_time.now, int)
    assert _time.now == int(time.time())
    assert _time.minutes_ago(5) == 1700000000 - 300
    assert _time.minutes_ago(180) == _time.hours_ago(3)
    assert _time.days_ago(0.5) == _time.minutes_ago(720)
    assert _time.days_ago(0.5) == _time.hours_ago(12)
    assert _time.weeks_ago(2) == _time.days_ago(14)
