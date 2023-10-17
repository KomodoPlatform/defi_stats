#!/usr/bin/env python3
import time
import sqlite3
import models
import const
from fixtures import setup_swaps_test_data, setup_database, setup_time, logger
from helper import format_10f

now = int(time.time())
hour_ago = now - 3600
two_hours_ago = now - 7200
day_ago = now - 86400
week_ago = now - 604800
month_ago = now - 2592000
two_months_ago = now - 5184000


def test_get_pairs(setup_swaps_test_data):
    # Confirm pairs response is correct
    DB = setup_swaps_test_data
    pairs = DB.get_pairs()
    assert ("MCL", "KMD") not in pairs
    assert ("DGB", "LTC") not in pairs
    assert ("KMD", "BTC") in pairs
    assert ("BTC", "KMD") not in pairs
    assert len(pairs) == 6
    pairs = DB.get_pairs(45)
    assert ("MCL", "KMD") not in pairs
    assert ("DGB", "LTC") in pairs
    assert ("DOGE", "LTC") not in pairs
    pairs = DB.get_pairs(90)
    assert ("DOGE", "LTC") in pairs


def test_get_swaps_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    swaps = DB.get_swaps_for_pair(("MCL", "KMD"), day_ago)
    assert len(swaps) == 0

    swaps = DB.get_swaps_for_pair(("DOGE", "BTC"), day_ago)
    assert len(swaps) == 1
    assert swaps[0]["trade_type"] == "sell"

    swaps = DB.get_swaps_for_pair(("DGB", "LTC"), two_months_ago)
    assert len(swaps) == 2
    assert swaps[0]["trade_type"] == "buy"


def get_actual_db_data(maker_coin: str = "BTC", limit: int = 5):
    '''
    This is just here for convienince to get data from
    actual DB for use in testing fixtures
    '''
    DB = models.SqliteDB(const.MM2_DB_PATH)
    DB.sql_cursor.execute(
        'select * from stats_swaps where maker_coin = "BTC" limit 5')
    data = []
    for r in DB.sql_cursor.fetchall():
        data.append(r)
    DB.close()
    return data


def test_get_actual_db_data():
    r = get_actual_db_data()
    assert len(r) <= 5


def test_get_last_price_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data
    r = DB.get_last_price_for_pair("LTC", "DOGE")["price"]
    assert format_10f(r) == format_10f(0.1)
    r = DB.get_last_price_for_pair("DOGE", "LTC")["price"]
    assert format_10f(r) == format_10f(10)
    r = DB.get_last_price_for_pair("KMD", "DGB")["price"]
    assert format_10f(r) == format_10f(1 / 0.0018)
    r = DB.get_last_price_for_pair("KMD", "ETH")["timestamp"]
    assert r == 0
