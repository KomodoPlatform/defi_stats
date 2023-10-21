#!/usr/bin/env python3
import sys
import time
from decimal import Decimal
from fixtures import (
    setup_swaps_test_data,
    setup_database,
    setup_time,
    setup_dgb_kmd_orderbook,
    setup_kmd_dgb_orderbook,
    setup_kmd_ltc_orderbook,
    setup_kmd_ltc_str_pair_with_db,
    setup_kmd_dgb_tuple_pair_with_db,
    setup_dgb_kmd_str_pair_with_db,
    logger,
)


def test_get_and_parse(setup_kmd_ltc_orderbook):
    orderbook = setup_kmd_ltc_orderbook
    r = orderbook.get_and_parse()
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert "base_max_volume" in r["asks"][0]
    assert "base_max_volume" in r["bids"][0]
    r = orderbook.get_and_parse(endpoint=True)
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
    r = orderbook.for_pair(endpoint=False)
    logger.info(r)
    assert r["ticker_id"] == "DGB_KMD"
    assert int(r["timestamp"]) > int(time.time()) - 86400
    assert len(r["asks"]) == 6
    assert len(r["bids"]) == 6
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111 * 2)
    assert Decimal(r["total_asks_rel_vol"]) == Decimal(3126 * 2)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222 * 2)
    assert Decimal(r["total_bids_rel_vol"]) == Decimal(4848 * 2)

    orderbook = setup_kmd_dgb_orderbook
    r = orderbook.for_pair(endpoint=False)
    assert r["ticker_id"] == "DGB_KMD"
    assert int(r["timestamp"]) > int(time.time()) - 86400
    assert len(r["asks"]) == 6
    assert len(r["bids"]) == 6
    assert Decimal(r["total_asks_base_vol"]) == Decimal(111 * 2)
    assert Decimal(r["total_asks_rel_vol"]) == Decimal(3126 * 2)
    assert Decimal(r["total_bids_base_vol"]) == Decimal(222 * 2)
    assert Decimal(r["total_bids_rel_vol"]) == Decimal(4848 * 2)


def test_related_orderbooks_list():
    pass
