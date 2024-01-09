#!/usr/bin/env python3
import time
import sqlite3
from fixtures_class import (
    helper,
)
from util.logger import logger

from fixtures_data import swap_item, swap_item2
from fixtures_db import (
    setup_actual_db,
    setup_swaps_db_data,
    setup_time,
)
from util.transform import merge_orderbooks, format_10f

from db.sqlitedb import (
    is_source_db,
    get_sqlite_db,
    get_sqlite_db_paths,
    list_sqlite_dbs,
    get_netid,
    compare_uuid_fields,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL, DB_MASTER_PATH

now = int(time.time())
hour_ago = now - 3600
two_hours_ago = now - 7200
day_ago = now - 86400
week_ago = now - 604800
month_ago = now - 2592000
two_months_ago = now - 5184000


def test_get_pairs(setup_swaps_db_data):
    # Returns priced and unpriced pairs
    DB = setup_swaps_db_data
    pairs = DB.query.get_pairs()
    logger.calc(pairs)
    assert ("KMD_LTC") in pairs
    assert ("LTC_KMD") not in pairs
    assert len(pairs) == 7
    assert ("DGB_KMD-BEP20") not in pairs
    assert ("KMD-BEP20_DGB") in pairs
    pairs = DB.query.get_pairs(90)
    assert len(pairs) == 8


def test_get_swaps_for_pair(setup_swaps_db_data):
    DB = setup_swaps_db_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    # Test failed excluded
    swaps = DB.query.get_swaps_for_pair("MCL", "KMD", start_time=day_ago)
    assert len(swaps) == 0

    swaps1 = DB.query.get_swaps_for_pair("LTC", "KMD", start_time=day_ago)
    swaps2 = DB.query.get_swaps_for_pair("KMD", "LTC", start_time=day_ago)
    assert len(swaps1) == len(swaps2)
    for i in swaps1:
        logger.info(i)
    assert len(swaps1) == 3
    assert swaps1[2]["trade_type"] == "buy"
    for i in swaps2:
        logger.info(i)
    assert len(swaps2) == 3
    assert swaps2[2]["trade_type"] == "sell"

    swaps = DB.query.get_swaps_for_pair("DGB", "LTC", start_time=two_months_ago)
    for i in swaps:
        logger.info(i)
    assert len(swaps) == 3
    assert swaps[0]["trade_type"] == "buy"


def test_get_swap(setup_swaps_db_data):
    DB = setup_swaps_db_data
    r = DB.query.get_swap("77777777-2762-4633-8add-6ad2e9b1a4e7")
    assert r["maker_coin"] == "LTC-segwit"
    assert r["taker_coin"] == "KMD"
    assert r["maker_amount"] == 10
    assert r["taker_amount"] == 1


def test_is_source_db():
    assert is_source_db("xyz_MM2.db")
    assert not is_source_db("xyz_MM2x.db")


def test_is_7777():
    assert is_source_db("seed_MM2.db")
    assert not is_source_db("xyz_seed.db")


def test_compare_uuid_fields():
    r = compare_uuid_fields(swap_item, swap_item2)
    assert r["taker_coin_usd_price"] == "75.1"
    assert r["maker_coin_usd_price"] == "0.5"
    assert r["is_success"] == "1"
    assert r["finished_at"] == "1700000777"


def test_get_sqlite_db():
    r = get_sqlite_db(netid="7777")
    assert r.db_file == "MM2_7777.db"
    r = get_sqlite_db(db=r)
    assert r.db_file == "MM2_7777.db"


def test_get_sqlite_db_paths():
    assert get_sqlite_db_paths(netid="7777") == MM2_DB_PATH_7777
    assert get_sqlite_db_paths(netid="8762") == MM2_DB_PATH_8762
    assert get_sqlite_db_paths(netid="ALL") == MM2_DB_PATH_ALL


def test_list_sqlite_dbs():
    r = list_sqlite_dbs(DB_MASTER_PATH)
    assert "MM2_all.db" in r


def test_get_netid():
    assert get_netid("file_7777.db") == "7777"
    assert get_netid("7777_file.db") == "7777"
    assert get_netid("file_7777_backup.db") == "7777"
    assert get_netid("file_MM2.db") == "8762"
    assert get_netid("seed_file.db") == "7777"
    assert get_netid("node_file.db") == "ALL"
