#!/usr/bin/env python3
import time
from decimal import Decimal
from fixtures import (
    logger
)

from fixtures_db import (
    setup_swaps_db_data,
    setup_swaps_db_data,
)

from fixtures_orderbook import (
    setup_dgb_kmd_orderbook,
    setup_kmd_dgb_orderbook,
    setup_kmd_ltc_orderbook,
)


def test_get_and_parse_orderbook(setup_kmd_ltc_orderbook):
    orderbook = setup_kmd_ltc_orderbook
    r = orderbook.get_and_parse_orderbook()
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert "base_max_volume" in r["asks"][0]
    assert "base_max_volume" in r["bids"][0]
    r = orderbook.get_and_parse_orderbook(endpoint=True)
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert len(r["asks"][0]) == 2
    assert len(r["bids"][0]) == 2
    assert isinstance(r["asks"][0], list)
    assert isinstance(r["bids"][0], list)


def test_for_pair(
    setup_dgb_kmd_orderbook,
    setup_kmd_dgb_orderbook
):
    orderbook = setup_dgb_kmd_orderbook
    r = orderbook.for_pair()
    logger.info(r)
    assert r["ticker_id"] == "DGB_KMD"
    assert int(r["timestamp"]) > int(time.time()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4848)

    orderbook = setup_kmd_dgb_orderbook
    r = orderbook.for_pair()
    assert r["ticker_id"] == "DGB_KMD"
    assert int(r["timestamp"]) > int(time.time()) - 86400
    assert len(r["asks"]) == 3
    assert len(r["bids"]) == 3
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111)
    assert Decimal(r["total_asks_quote_vol"]) == Decimal(3126)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222)
    assert Decimal(r["total_bids_quote_vol"]) == Decimal(4848)
