#!/usr/bin/env python3
import util.cron as cron
from decimal import Decimal
from util.logger import logger
import util.memcache as memcache
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
import util.transform as transform
import tests


def test_get_and_parse(setup_kmd_ltc_orderbook, setup_ltc_kmd_orderbook):
    orderbook = setup_kmd_ltc_orderbook
    r = orderbook.get_and_parse()
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
    orderbook = setup_ltc_kmd_orderbook
    r = orderbook.get_and_parse()
    logger.info(r)
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


def test_for_pair(setup_dgb_doge_orderbook, setup_doge_dgb_orderbook):
    orderbook = setup_dgb_doge_orderbook
    r = orderbook.for_pair(all=True)
    assert r["pair"] == "DGB_DOGE"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 6
    assert len(r["bids"]) == 6
    assert Decimal(r["total_asks_base_vol"]) == Decimal(4959)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3348)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(3348)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4959)

    orderbook = setup_dgb_doge_orderbook
    r = orderbook.for_pair(all=False)
    assert r["pair"] == "DGB_DOGE"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4848)

    orderbook = setup_doge_dgb_orderbook
    r = orderbook.for_pair(all=False)
    assert r["pair"] == "DOGE_DGB"
    assert int(r["timestamp"]) > int(cron.now_utc()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(4848)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(111)


def test_find_lowest_ask(setup_kmd_btc_orderbook, setup_kmd_btc_pair):
    orderbook = setup_kmd_btc_orderbook
    x = orderbook.for_pair(all=True)
    r = orderbook.find_lowest_ask(x)
    assert r == transform.format_10f(1 / 24)


def test_find_highest_bid(setup_kmd_btc_orderbook, setup_kmd_btc_pair):
    orderbook = setup_kmd_btc_orderbook
    x = orderbook.for_pair(all=True)
    r = orderbook.find_highest_bid(x)
    assert r == transform.format_10f(1 / 26)
