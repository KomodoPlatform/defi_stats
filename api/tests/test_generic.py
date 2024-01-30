import time
import util.cron as cron
import pytest
from decimal import Decimal
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.generic import Generic
from tests.fixtures_db import setup_swaps_db_data, setup_time
from tests.fixtures_data import swap_item
from util.logger import logger
import util.memcache as memcache


def test_orderbook():
    generic = Generic()
    r = generic.orderbook("KMD_DOGE", all=True)
    r2 = generic.orderbook("DOGE_KMD", all=True)
    r3 = generic.orderbook("KMD_DGB", depth=2)
    r5 = generic.orderbook("KMD/DGB", depth=2)
    r6 = generic.orderbook("KMD_XXX", depth=2)
    r6 = generic.orderbook("KMDXX", depth=2)
    r_all = generic.orderbook("KMD_LTC", all=True)
    r_all2 = generic.orderbook("KMD_LTC-segwit", all=True)
    r = generic.orderbook("DGB_DOGE", all=True)

    time.sleep(1)
    r = generic.orderbook("KMD_DOGE", all=True)
    r2 = generic.orderbook("DOGE_KMD", all=True)
    assert Decimal(r["combined_volume_usd"]) == Decimal(r2["combined_volume_usd"])

    r3 = generic.orderbook("KMD_DGB", depth=2)
    assert len(r3["asks"]) == 2
    assert len(r3["bids"]) == 2

    r5 = generic.orderbook("KMD/DGB", depth=2)
    logger.info(r5)
    assert "error" in r5

    r6 = generic.orderbook("KMD_XXX", depth=2)
    assert r6["bids"] == []

    with pytest.raises(Exception):
        r6 = generic.orderbook("KMDXX", depth=2)
        assert r6["bids"] == []

    r_all = generic.orderbook("KMD_LTC", all=True)
    logger.calc(r_all)
    r_all2 = generic.orderbook("KMD_LTC-segwit", all=True)
    logger.loop(r_all2)
    assert r_all["bids"][0] == r_all2["bids"][0]
    assert r_all["asks"][0] == r_all2["asks"][0]
    assert r_all["pair"] == r_all2["pair"]
    assert r_all["quote"] == r_all2["quote"]
    assert r_all["base"] == r_all2["base"]
    assert r_all["liquidity_in_usd"] == r_all2["liquidity_in_usd"]

    r = generic.orderbook("DGB_DOGE", all=True)
    assert r["pair"] == "DGB_DOGE"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 6
    assert len(r["bids"]) == 6
    assert Decimal(r["total_asks_base_vol"]) == Decimal(4959)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3348)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(3348)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(5089)

    r = generic.orderbook("DGB_DOGE", all=False)
    assert r["pair"] == "DGB_DOGE"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4848)

    r = generic.orderbook("DOGE_DGB", all=False)
    assert r["pair"] == "DOGE_DGB"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(4848)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(111)


def test_tickers():
    generic = Generic()
    r = generic.tickers()

    assert r["last_update"] > cron.now_utc() - 60
    assert r["pairs_count"] > 0
    assert r["swaps_count"] > 0
    assert float(r["combined_volume_usd"]) > 0
    assert float(r["combined_liquidity_usd"]) > 0
    assert len(r["data"]) > 0
    for i in r["data"][0]:
        assert not isinstance(i, Decimal)
    assert r["data"][0]["ticker_id"] < r["data"][2]["ticker_id"]
