import time
import pytest
from decimal import Decimal
from tests.fixtures_db import setup_swaps_db_data, setup_time
from tests.fixtures_data import swap_item
from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.generic import Generic
from util.logger import logger


def test_orderbook(setup_swaps_db_data):
    generic = Generic(db=setup_swaps_db_data, netid="ALL")
    r_all = generic.orderbook("KMD_LTC")
    r_all2 = generic.orderbook("KMD_LTC-segwit")
    assert r_all["bids"][0] == r_all2["bids"][0]
    assert r_all["asks"][0] == r_all2["asks"][0]
    assert r_all["pair"] == r_all2["pair"]
    assert r_all["quote"] == r_all2["quote"]
    assert r_all["base"] == r_all2["base"]
    assert r_all["liquidity_usd"] == r_all2["liquidity_usd"]

    generic = Generic(db=setup_swaps_db_data, netid="8762")
    r = generic.orderbook("KMD_DOGE")
    r2 = generic.orderbook("DOGE_KMD")
    assert r["volume_usd_24hr"] == r2["volume_usd_24hr"]

    generic = Generic()
    r3 = generic.orderbook("KMD_DGB", depth=2)
    assert len(r3["asks"]) == 2
    assert len(r3["bids"]) == 2

    r5 = generic.orderbook("KMD/DGB", depth=2)
    assert "error" in r5

    r6 = generic.orderbook("KMD_XXX", depth=2)
    assert r6["bids"] == []

    with pytest.raises(Exception):
        r6 = generic.orderbook("KMDXX", depth=2)
        assert r6["bids"] == []


def test_traded_pairs(setup_swaps_db_data):
    generic = Generic(db=setup_swaps_db_data)
    r = generic.traded_pairs_info()
    assert isinstance(r, list)
    assert isinstance(r[0], dict)
    for i in r:
        if i["pool_id"] == "MORTY_KMD":
            assert not i["priced"]
        if i["pool_id"] == "KMD_BTC":
            assert i["priced"]


def test_traded_tickers(setup_swaps_db_data):
    generic = Generic(db=setup_swaps_db_data)
    r = generic.traded_tickers()
    assert r["last_update"] > time.time() - 60
    assert r["pairs_count"] > 0
    assert r["swaps_count"] > 0
    assert float(r["combined_volume_usd"]) > 0
    assert float(r["combined_liquidity_usd"]) > 0
    assert len(r["data"]) > 0
    for i in r["data"][0]:
        assert not isinstance(i, Decimal)
    assert r["data"][0]["ticker_id"] < r["data"][2]["ticker_id"]
