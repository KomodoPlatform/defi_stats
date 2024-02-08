import time
from util.cron import cron
import pytest
from decimal import Decimal
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.cache_calc import CacheCalc
from lib.generic import Generic, get_pairs_status
from lib.pair import Pair
from tests.fixtures_db import setup_swaps_db_data
from tests.fixtures_data import swap_item
from util.logger import logger
import util.memcache as memcache


def test_orderbook():
    pair = Pair("KMD_DOGE")
    time.sleep(1)
    r1 = pair.orderbook("KMD_DOGE", all_variants=True, no_thread=True)
    r2 = pair.orderbook("DOGE_KMD", all_variants=True, no_thread=True)
    assert len(r1["asks"]) == 6
    assert len(r2["asks"]) == 6

    r3 = pair.orderbook("KMD_DGB", depth=2, no_thread=True)
    assert len(r3["asks"]) == 2
    assert len(r3["bids"]) == 2

    r5 = pair.orderbook("KMD/DGB", depth=2, no_thread=True)
    assert "error" in r5

    r6 = pair.orderbook("KMD_XXX", depth=2, no_thread=True)
    assert r6["bids"] == []

    with pytest.raises(Exception):
        r6 = pair.orderbook("KMDXX", depth=2, no_thread=True)
        assert r6["bids"] == []

    r_all = pair.orderbook("KMD_LTC", all_variants=True, no_thread=True)
    r_all2 = pair.orderbook("KMD_LTC-segwit", all_variants=True)
    assert r_all["bids"][0] == r_all2["bids"][0]
    assert r_all["asks"][0] == r_all2["asks"][0]
    assert r_all["pair"] == r_all2["pair"]
    assert r_all["quote"] == r_all2["quote"]
    assert r_all["base"] == r_all2["base"]
    assert r_all["liquidity_in_usd"] == r_all2["liquidity_in_usd"]

    r = pair.orderbook("DGB_DOGE", all_variants=True, no_thread=True)
    assert r["pair"] == "DGB_DOGE"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 6
    assert len(r["bids"]) == 6
    assert Decimal(r["total_asks_base_vol"]) == Decimal(4959)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3348)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(3348)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(5089)

    r = pair.orderbook("DGB_DOGE", all_variants=False, no_thread=True)
    assert r["pair"] == "DGB_DOGE"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4848)

    r = pair.orderbook("DOGE_DGB", all_variants=False, no_thread=True)
    assert r["pair"] == "DOGE_DGB"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(4848)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(111)


def test_tickers():
    cache_calc = CacheCalc()
    r = cache_calc.tickers(7, 1)

    assert r["pairs_count"] > 0
    r2 = cache_calc.tickers(1, 1)
    assert len(r2["data"]) > 0
    for i in r2["data"][0]:
        assert not isinstance(i, Decimal)
    assert r["data"][0]["ticker_id"] < r["data"][2]["ticker_id"]


def test_get_pairs_status():
    r = get_pairs_status([], None)
    logger.info(r)
    r2 = get_pairs_status([])
    assert "KMD" in [i["base"] for i in r]
    assert "DOGE" in [i["target"] for i in r]
    assert "KMD_LTC" in [i["ticker_id"] for i in r2]
