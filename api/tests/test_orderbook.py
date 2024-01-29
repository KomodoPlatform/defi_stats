#!/usr/bin/env python3
import util.cron as cron
from decimal import Decimal
from tests.fixtures_class import setup_files
from tests.fixtures_db import setup_swaps_db_data, setup_time
from tests.fixtures_pair import (
    setup_kmd_dgb_pair,
    setup_dgb_kmd_pair,
    setup_kmd_ltc_pair,
    setup_ltc_kmd_pair,
    setup_dgb_doge_pair,
    setup_doge_dgb_pair,
    setup_kmd_btc_pair,
    setup_not_existing_pair,
)

from tests.fixtures_orderbook import (
    setup_dgb_kmd_orderbook,
    setup_kmd_dgb_orderbook,
    setup_kmd_ltc_orderbook,
    setup_ltc_kmd_orderbook,
    setup_dgb_doge_orderbook,
    setup_doge_dgb_orderbook,
    setup_kmd_btc_orderbook,
    setup_kmd_btc_orderbook_data,
)
from lib.orderbook import get_and_parse
from util.logger import logger
import util.helper as helper
import util.memcache as memcache
import util.transform as transform
import tests


coins_config = memcache.get_coins_config()


def test_get_and_parse():
    r = get_and_parse("KMD", "LTC", coins_config)
    assert r["pair"] == "KMD_LTC"
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert isinstance(r["asks"][0], dict)
    assert isinstance(r["bids"][0], dict)
    assert "volume" in r["asks"][0]
    assert "price" in r["bids"][0]

    # TODO: Inversion for orderbook
    r = get_and_parse("LTC", "KMD", coins_config)
    assert r["pair"] == "LTC_KMD"
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert len(r["asks"][0]) == 3
    assert len(r["bids"][0]) == 3
    assert isinstance(r["asks"][0], dict)
    assert isinstance(r["bids"][0], dict)
    assert "volume" in r["asks"][0]
    assert "price" in r["bids"][0]
