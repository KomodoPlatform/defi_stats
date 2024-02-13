#!/usr/bin/env python3
from util.cron import cron
import sqlite3
from decimal import Decimal
from db.sqldb import SqlSource, SqlQuery
from db.sqlitedb import get_sqlite_db, get_sqlite_db_paths
from db.sqlitedb_merge import (
    list_sqlite_dbs,
    compare_uuid_fields,
)
from tests.fixtures_data import swap_item, swap_item2, cipi_swap, cipi_swap2
from tests.fixtures_db import (
    setup_actual_db,
    setup_swaps_db_data,
)
import util.helper as helper
from util.logger import logger
from util.transform import format_10f

from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL, DB_MASTER_PATH
import util.validate as validate
import util.memcache as memcache

now = int(cron.now_utc())
hour_ago = now - 3600
two_hours_ago = now - 7200
day_ago = now - 86400
week_ago = now - 604800
month_ago = now - 2592000
two_months_ago = now - 5184000


# TODO: Use new DB
def test_get_pairs(setup_swaps_db_data):
    # Returns priced and unpriced pairs
    DB = setup_swaps_db_data
    pairs = DB.get_pairs()
    logger.info(pairs)
    assert ("KMD_LTC") in pairs
    assert ("LTC_KMD") not in pairs
    assert len(pairs) == 8
    assert ("DGB_KMD-BEP20") not in pairs
    assert ("KMD-BEP20_DGB-segwit") in pairs
    pairs = DB.get_pairs(90)
    assert len(pairs) == 11


def test_get_swaps_for_pair(setup_swaps_db_data):
    DB = setup_swaps_db_data

    swaps = DB.get_swaps_for_pair("MCL", "KMD", start_time=day_ago, success_only=False)
    assert len(swaps) == 1
    # excludes failed by default
    swaps = DB.get_swaps_for_pair("MCL", "KMD", start_time=day_ago, all_variants=True)
    assert len(swaps) == 0

    # No inversion, that happens later. Should be the same.
    swaps1 = DB.get_swaps_for_pair("LTC", "KMD", start_time=day_ago, all_variants=True)
    swaps2 = DB.get_swaps_for_pair("KMD", "LTC", start_time=day_ago, all_variants=True)
    assert len(swaps1) == len(swaps2) == 3
    assert swaps1[0]["trade_type"] == "sell"
    assert swaps2[0]["trade_type"] == "sell"
    assert swaps1[1]["trade_type"] == "buy"
    assert swaps2[1]["trade_type"] == "buy"
    assert swaps1[2]["trade_type"] == "buy"
    assert swaps2[2]["trade_type"] == "buy"

    swaps = DB.get_swaps_for_pair(
        "DGB", "LTC", start_time=two_months_ago, all_variants=True
    )
    assert len(swaps) == 3
    assert swaps[0]["trade_type"] == "sell"

    # Should be same as above, as segwit will merge
    swaps = DB.get_swaps_for_pair(
        "DGB", "LTC", start_time=two_months_ago, all_variants=False
    )
    assert len(swaps) == 3
    assert swaps[0]["trade_type"] == "sell"

    # Should be lt above, as segwit will not merge
    swaps = DB.get_swaps_for_pair(
        "DGB", "LTC", start_time=two_months_ago, all_variants=False, merge_segwit=False
    )
    assert len(swaps) == 1
    assert swaps[0]["trade_type"] == "sell"


def test_get_swap(setup_swaps_db_data):
    DB = setup_swaps_db_data
    r = DB.get_swap("77777777-2762-4633-8add-6ad2e9b1a4e7")
    assert r["taker_coin"] == "LTC-segwit"
    assert r["maker_coin"] == "KMD"
    assert r["maker_amount"] == 100
    assert r["taker_amount"] == 1
    r = DB.get_swap("x")
    assert "error" in r


def test_is_source_db():
    assert validate.is_source_db("xyz_MM2.db")
    assert not validate.is_source_db("xyz_MM2x.db")


def test_is_7777():
    assert validate.is_source_db("seed_MM2.db")
    assert not validate.is_source_db("xyz_seed.db")


def test_compare_uuid_fields():
    r = compare_uuid_fields(swap_item, swap_item2)
    logger.info(r)
    assert r["taker_coin_usd_price"] == "50.0"
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
    assert helper.get_netid("file_7777.db") == "7777"
    assert helper.get_netid("7777_file.db") == "7777"
    assert helper.get_netid("file_7777_backup.db") == "7777"
    assert helper.get_netid("file_MM2.db") == "8762"
    assert helper.get_netid("seed_file.db") == "7777"


def test_swap_counts(setup_swaps_db_data):
    DB = setup_swaps_db_data
    r = DB.swap_counts()
    assert r["swaps_all_time"] == 14
    assert r["swaps_30d"] == 12
    assert r["swaps_14d"] == 11
    assert r["swaps_7d"] == 10
    assert r["swaps_24hr"] == 8


def test_get_swaps_for_coin(setup_swaps_db_data):
    DB = setup_swaps_db_data
    r = DB.get_swaps_for_coin("KMD")
    assert len(r) == 5
    r = DB.get_swaps_for_coin("KMD-BEP20", all_variants=False)
    assert len(r) == 2
    r = DB.get_swaps_for_coin("KMD", all_variants=True)
    assert len(r) == 7
    r = DB.get_swaps_for_coin("KMD-BEP20", all_variants=True)
    assert len(r) == 7

    r = DB.get_swaps_for_coin("LTC")
    assert len(r) == 2
    r = DB.get_swaps_for_coin("LTC-segwit")
    assert len(r) == 2
    r = DB.get_swaps_for_coin("LTC-segwit", all_variants=True)
    assert len(r) == 4
    r = DB.get_swaps_for_coin("LTC", all_variants=True)
    assert len(r) == 4
    r = DB.get_swaps_for_coin("LTC", merge_segwit=True)
    assert len(r) == 4
    r = DB.get_swaps_for_coin("LTC-segwit", merge_segwit=True)
    assert len(r) == 4


def test_coin_trade_volumes_usd(setup_swaps_db_data):
    DB = setup_swaps_db_data
    gecko_source = memcache.get_gecko_source()
    volumes = DB.coin_trade_volumes(
        start_time=int(cron.now_utc()) - 86400,
        end_time=int(cron.now_utc()),
    )

    r = DB.coin_trade_volumes_usd(volumes, gecko_source)
    logger.info(r)
    logger.info(r.keys())
    logger.info(r["volumes"].keys())
    vols = r["volumes"]["LTC"]
    logger.info(vols)
    assert vols["LTC"]["maker_volume"] == 1
    assert vols["LTC"]["maker_volume_usd"] == 100
    assert vols["LTC"]["total_swaps"] == 2
    assert vols["LTC-segwit"]["total_swaps"] == 2
    assert vols["ALL"]["total_swaps"] == 4

    assert vols["LTC-segwit"]["taker_volume"] == 1
    assert vols["ALL"]["maker_volume"] == 2
    assert vols["ALL"]["taker_volume"] == 3

    vols = r["volumes"]["KMD"]
    assert vols["KMD"]["taker_volume"] == 1000101
    assert vols["KMD-BEP20"]["maker_volume"] == Decimal(str(1.9))
    assert vols["KMD-BEP20"]["taker_volume"] == 0
    assert vols["ALL"]["trade_volume_usd"] == Decimal(str(1000402.9))


def test_get_uuids(setup_swaps_db_data):
    DB = setup_swaps_db_data
    r = DB.swap_uuids(start_time=1, success_only=True)
    assert len(r) == 14
    r = DB.swap_uuids(start_time=1, success_only=False)
    assert len(r) == 15
    r = DB.swap_uuids(failed_only=True)
    assert len(r) == 1
    r = DB.swap_uuids(success_only=False)
    assert len(r) == 9
    r = DB.swap_uuids()
    assert len(r) == 8


def test_normalise_swap_data(setup_swaps_db_data):
    DB = SqlSource()

    r = DB.normalise_swap_data([cipi_swap])
    logger.info(r)
    assert r[0]["pair"] == "KMD_LTC"
    assert r[0]["trade_type"] == "sell"
    assert r[0]["price"] == Decimal("0.01")

    r = DB.normalise_swap_data([cipi_swap2])
    logger.info(r)
    assert r[0]["pair"] == "KMD_LTC"
    assert r[0]["trade_type"] == "buy"
    assert r[0]["price"] == Decimal("0.01")


# TODO: Returning errors, debug later
def test_gui_last_traded():
    DB = SqlQuery()
    r = DB.gui_last_traded()
    logger.calc(r)
    assert len(r) == 6
    r = DB.gui_last_traded(False)
    logger.calc(r)
    assert len(r) == 8
