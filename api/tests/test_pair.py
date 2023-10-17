#!/usr/bin/env python3
from fixtures import (
    setup_database,
    setup_swaps_test_data,
    setup_time,
    setup_kmd_dgb_tuple_pair_with_db,
    setup_dgb_kmd_str_pair_with_db,
    setup_not_a_real_pair,
    setup_not_existing_pair,
    setup_kmd_ltc_str_pair_with_db,
    logger,
)
from helper import format_10f


def test_trades_for_pair(
    setup_dgb_kmd_str_pair_with_db,
    setup_kmd_dgb_tuple_pair_with_db,
    setup_not_a_real_pair,
    setup_not_existing_pair,
):
    pair = setup_dgb_kmd_str_pair_with_db
    r = pair.historical_trades('all')["buy"]
    logger.info(r)
    assert len(r) == 2
    assert r[0]["type"] == "buy"
    assert r[0]["timestamp"] > r[1]["timestamp"]
    assert r[0]["base_volume"] == "500.0000000000"
    assert r[0]["quote_volume"] == "0.9000000000"
    assert r[0]["price"] == format_10f(0.0018)

    pair = setup_kmd_dgb_tuple_pair_with_db
    r = pair.historical_trades('all')["buy"]
    assert len(r) == 0
    r = pair.historical_trades('all')["sell"]
    assert len(r) == 2
    assert r[0]["type"] == "sell"
    assert r[0]["base_volume"] == "0.9000000000"
    assert r[0]["quote_volume"] == "500.0000000000"
    assert r[0]["price"] == format_10f(1 / 0.0018)

    pair = setup_not_a_real_pair
    r = pair.historical_trades('all')["buy"]
    assert r == []

    pair = setup_not_existing_pair
    r = pair.historical_trades('all')["buy"]
    assert r == []
    # TODO: Add extra tests once linked to fixtures for test db


def test_get_volumes_and_prices(setup_kmd_ltc_str_pair_with_db,
                                setup_not_existing_pair):
    pair = setup_kmd_ltc_str_pair_with_db
    r = pair.get_volumes_and_prices()
    assert float(r["base_volume"]) == 110
    assert r["base_volume"] == format_10f(110)
    assert float(r["highest_price_24h"]) == 0.1
    assert float(r["last_price"]) == 0.1
    assert float(r["lowest_price_24h"]) == 0.01
    assert float(r["price_change_percent_24h"]) == 0.0009
    assert float(r["price_change_24h"]) == 0.09

    pair = setup_not_existing_pair
    r = pair.get_volumes_and_prices()
    assert float(r["last_price"]) == 0
