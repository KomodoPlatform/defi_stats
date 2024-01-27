import util.cron as cron
import pytest
from decimal import Decimal
from util.logger import logger
from tests.fixtures_data import swap_item
from tests.fixtures_db import setup_swaps_db_data, setup_time
from util.helper import (
    get_mm2_rpc_port,
    get_chunks,
    get_price_at_finish,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.markets import Markets


def test_pairs():
    markets = Markets()
    data = markets.pairs()
    assert isinstance(data, list)
    r = [i["ticker_id"] for i in data]
    # Markets includes test coins
    assert "MORTY_KMD" in r
    # Same mcap, could be either :shrug:
    assert "KMD_KMD-BEP20" in r
    assert "KMD_BTC" in r
    assert "BTC_KMD" not in r
    assert "KMD_LTC" in r
    assert "LTC_KMD" not in r


def test_tickers():
    markets = Markets()
    r = markets.tickers()
    assert r["last_update"] > cron.now_utc() - 60
    assert r["pairs_count"] > 0
    assert r["swaps_count"] > 0
    assert float(r["combined_volume_usd"]) > 0
    assert float(r["combined_liquidity_usd"]) > 0
    assert len(r["data"]) > 0
    for i in r["data"][0]:
        assert not isinstance(i, Decimal)
    assert r["data"][0]["ticker_id"] < r["data"][2]["ticker_id"]
