import time
import pytest
from decimal import Decimal
from util.logger import logger
from fixtures_data import swap_item
from fixtures_db import setup_swaps_db_data, setup_time
from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.markets import Markets


def test_pairs():
    markets = Markets(netid="8762", testing=True)
    data = markets.pairs()
    assert isinstance(data, list)
    r = [i["ticker_id"] for i in data]
    # Markets includes test coins
    assert "MARTY_DOC" not in r
    assert "DOC_MARTY" in r
    # Same mcap, could be either :shrug:
    assert "KMD-BEP20_KMD" in r or "KMD_KMD-BEP20" in r
    assert "KMD_ETH" in r
    assert "ETH_KMD" not in r
    assert "KMD_FTM" in r
    assert "KMD_QTUM" in r
    assert "KMD_DASH" in r
    assert "KMD_RVN" in r
    assert "KMD_XXX" not in r


def test_tickers(setup_swaps_db_data):
    markets = Markets(netid="8762", testing=True, db=setup_swaps_db_data)
    r = markets.tickers()
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


def test_last_trade():
    markets = Markets(netid="8762", testing=True)
    data = markets.last_trade()
    logger.info(data)
    assert len(data) > 0
    assert "KMD_LTC" in data.keys()
    assert data["KMD_LTC"]["last_swap"] > time.time() - 86400 * 7
    assert data["KMD_LTC"]["swap_count"] > 0
