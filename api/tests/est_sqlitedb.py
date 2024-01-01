#!/usr/bin/env python3
import time
import sqlite3
from fixtures import (
    logger,
    helper,
)

from fixtures_db import setup_actual_db, setup_swaps_db_data, setup_swaps_db_data

from util.transform import merge_orderbooks, format_10f

now = int(time.time())
hour_ago = now - 3600
two_hours_ago = now - 7200
day_ago = now - 86400
week_ago = now - 604800
month_ago = now - 2592000
two_months_ago = now - 5184000


def test_get_pairs(setup_swaps_db_data):
    DB = setup_swaps_db_data
    pairs = DB.get_pairs(include_all_kmd=False)
    assert ("KMD", "BTC") in pairs
    assert ("DOGE", "LTC") not in pairs
    assert len(pairs) == 7
    pairs = DB.get_pairs(45, include_all_kmd=False)
    logger.info(pairs)
    assert ("KMD", "BTC") in pairs
    assert ("DGB", "LTC") in pairs
    assert ("DOGE", "LTC") not in pairs
    assert len(pairs) == 7
    pairs = DB.get_pairs(90, include_all_kmd=False)
    assert ("KMD", "BTC") in pairs
    assert ("DGB", "LTC") in pairs
    assert ("DOGE", "LTC") in pairs
    assert ("LTC", "DOGE") not in pairs
    assert len(pairs) == 8


def test_get_swaps_for_pair(setup_swaps_db_data):
    DB = setup_swaps_db_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    # Test failed excluded
    swaps = DB.get_swaps_for_pair(("MCL", "KMD"), day_ago)
    assert len(swaps) == 0

    swaps1 = DB.get_swaps_for_pair(("LTC", "KMD"), day_ago)
    swaps2 = DB.get_swaps_for_pair(("KMD", "LTC"), day_ago)
    assert len(swaps1) == len(swaps2)
    for i in swaps1:
        logger.info(i)
    assert len(swaps1) == 3
    assert swaps1[2]["trade_type"] == "sell"
    for i in swaps2:
        logger.info(i)
    assert len(swaps2) == 3
    assert swaps2[0]["trade_type"] == "buy"

    swaps = DB.get_swaps_for_pair(("DGB", "LTC"), two_months_ago)
    for i in swaps:
        logger.info(i)
    assert len(swaps) == 3
    assert swaps[0]["trade_type"] == "buy"


def get_actual_db_data(setup_actual_db):
    """
    This is just here for convienince to get data from
    actual DB for use in testing fixtures
    """
    DB = setup_actual_db
    DB.sql_cursor.execute('select * from stats_swaps where maker_coin = "KMD" limit 5')
    data = []
    for r in DB.sql_cursor.fetchall():
        data.append(r)
    DB.close()
    assert len(data) == 5


def test_get_last_price_for_pair(setup_swaps_db_data):
    DB = setup_swaps_db_data
    r = DB.get_last_price_for_pair("LTC", "DOGE")["price"]
    assert format_10f(r) == format_10f(0.1)
    r = DB.get_last_price_for_pair("DOGE", "LTC")["price"]
    assert format_10f(r) == format_10f(10)
    r = DB.get_last_price_for_pair("KMD", "DGB")["price"]
    assert format_10f(r) == format_10f(1 / 0.0018)
    r = DB.get_last_price_for_pair("KMD", "ETH")["timestamp"]
    assert r == 0


def test_get_swap(setup_swaps_db_data):
    DB = setup_swaps_db_data
    r = DB.get_swap("77777777-2762-4633-8add-6ad2e9b1a4e7")
    assert r["maker_coin"] == "LTC-segwit"
    assert r["taker_coin"] == "KMD"
    assert r["maker_amount"] == 10
    assert r["taker_amount"] == 1
