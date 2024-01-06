import time
import pytest
from decimal import Decimal
from fixtures_db import setup_swaps_db_data, setup_time
from fixtures_data import swap_item
from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.generics import Generics
from util.logger import logger


def test_get_orderbook(setup_swaps_db_data):
    generics = Generics(db=setup_swaps_db_data, testing=True, netid="ALL")
    r_all = generics.get_orderbook("KMD_LTC")
    r_all2 = generics.get_orderbook("KMD_LTC-segwit")
    assert r_all["bids"][0] == r_all2["bids"][0]
    assert len(r_all["asks"]) == len(r_all2["asks"])
    assert len(r_all["pair"]) != len(r_all2["pair"])
    assert len(r_all["quote"]) != len(r_all2["quote"])
    assert len(r_all["base"]) == len(r_all2["base"])
    assert r_all["liquidity_usd"] == r_all2["liquidity_usd"]

    generics = Generics(netid="8762")
    r3 = generics.get_orderbook("KMD_DGB", depth=2)
    assert len(r3["asks"]) == 2
    assert len(r3["bids"]) == 2

    generics = Generics(netid="7777")
    r4 = generics.get_orderbook("KMD_DGB", depth=2)
    assert r4["asks"][0]["volume"] != r3["asks"][0]["volume"]
    assert r4["bids"][0]["volume"] != r3["bids"][0]["volume"]

    r5 = generics.get_orderbook("KMD/DGB", depth=2)
    assert "error" in r5

    r6 = generics.get_orderbook("KMD_XXX", depth=2)
    assert r6["bids"] == []

    with pytest.raises(Exception):
        r6 = generics.get_orderbook("KMDXX", depth=2)
        assert r6["bids"] == []


def test_traded_pairs(setup_swaps_db_data):
    generics = Generics(db=setup_swaps_db_data, testing=True)
    r = generics.traded_pairs(include_all_kmd=False)
    assert len(r) == 5
    assert isinstance(r, list)
    assert isinstance(r[0], dict)

    r2 = generics.traded_pairs(include_all_kmd=True)
    assert len(r2) > len(r)


def test_traded_tickers(setup_swaps_db_data):
    generics = Generics(db=setup_swaps_db_data, testing=True)
    r = generics.traded_tickers()
    logger.info(r)
    assert r["last_update"] > time.time() - 60
    assert r["pairs_count"] > 0
    assert r["swaps_count"] > 0
    assert float(r["combined_volume_usd"]) > 0
    assert float(r["combined_liquidity_usd"]) > 0
    assert len(r["data"]) > 0
    for i in r["data"][0]:
        assert not isinstance(i, Decimal)
    assert r["data"][0]["ticker_id"] < r["data"][2]["ticker_id"]
