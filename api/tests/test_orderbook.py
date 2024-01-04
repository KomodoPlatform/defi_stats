#!/usr/bin/env python3
import time
from decimal import Decimal
from util.logger import logger
from fixtures_class import setup_files

from fixtures_db import setup_swaps_db_data, setup_time

from fixtures_pair import (
    setup_kmd_dgb_pair,
    setup_dgb_kmd_pair,
    setup_kmd_ltc_pair,
    setup_ltc_kmd_pair,
    setup_dgb_doge_pair,
    setup_doge_dgb_pair,
    setup_kmd_btc_pair,
    setup_not_existing_pair,
)

from fixtures_orderbook import (
    setup_dgb_kmd_orderbook,
    setup_kmd_dgb_orderbook,
    setup_kmd_ltc_orderbook,
    setup_ltc_kmd_orderbook,
    setup_dgb_doge_orderbook,
    setup_doge_dgb_orderbook,
    setup_kmd_btc_orderbook,
    setup_kmd_btc_orderbook_data,
)


def test_get_and_parse(setup_kmd_ltc_orderbook, setup_ltc_kmd_orderbook):
    orderbook = setup_kmd_ltc_orderbook
    r = orderbook.get_and_parse()
    logger.merge(r)
    assert r["pair"] == "KMD_LTC"
    assert not orderbook.pair.inverse_requested
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert isinstance(r["asks"][0], dict)
    assert isinstance(r["bids"][0], dict)
    assert "volume" in r["asks"][0]
    assert "price" in r["bids"][0]

    orderbook = setup_ltc_kmd_orderbook
    r = orderbook.get_and_parse()
    logger.loop(r)
    assert r["pair"] == "LTC_KMD"
    assert orderbook.pair.inverse_requested
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    logger.loop(r["asks"])
    assert len(r["asks"][0]) == 2
    assert len(r["bids"][0]) == 2
    assert isinstance(r["asks"][0], dict)
    assert isinstance(r["bids"][0], dict)
    assert "volume" in r["asks"][0]
    assert "price" in r["bids"][0]


def test_for_pair(setup_dgb_doge_orderbook, setup_doge_dgb_orderbook):
    orderbook = setup_dgb_doge_orderbook
    r = orderbook.for_pair()
    logger.info(r)
    assert r["ticker_id"] == "DGB_DOGE"
    assert not orderbook.pair.inverse_requested
    assert int(r["timestamp"]) > int(time.time()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4848)

    orderbook = setup_doge_dgb_orderbook
    r = orderbook.for_pair()
    assert r["ticker_id"] == "DOGE_DGB"
    assert orderbook.pair.inverse_requested
    assert int(r["timestamp"]) > int(time.time()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4848)


def test_find_lowest_ask(setup_kmd_btc_orderbook, setup_kmd_btc_pair):
    orderbook = setup_kmd_btc_orderbook
    r = orderbook.find_lowest_ask(setup_kmd_btc_pair.orderbook_data)
    assert r == "26.0000000000"


def test_find_highest_bid(setup_kmd_btc_orderbook, setup_kmd_btc_pair):
    orderbook = setup_kmd_btc_orderbook
    r = orderbook.find_highest_bid(setup_kmd_btc_pair.orderbook_data)
    assert r == "24.0000000000"
