#!/usr/bin/env python3
import time
import pytest
from decimal import Decimal
from fixtures import (
    setup_database,
    setup_swaps_test_data,
    setup_time,
    setup_kmd_dgb_tuple_pair_with_db,
    setup_dgb_kmd_str_pair_with_db,
    setup_kmd_ltc_str_pair_with_db,
    setup_kmd_ltc_list_pair_with_db,
    setup_not_existing_pair,
    trades_info,
    no_trades_info,
    logger
)
from models import Pair
from helper import format_10f, set_pair_as_tuple


def test_get_pairs_type_error():
    with pytest.raises(TypeError):
        set_pair_as_tuple(15)


def test_get_pairs_value_error():
    with pytest.raises(ValueError):
        set_pair_as_tuple("Pair_of_three")


def test_trades_for_pair(
    setup_dgb_kmd_str_pair_with_db,
    setup_kmd_dgb_tuple_pair_with_db
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
    r = pair.historical_trades('all')["sell"]
    assert len(r) == 0
    r = pair.historical_trades('all')["buy"]
    assert len(r) == 2
    assert r[0]["type"] == "buy"
    assert r[0]["base_volume"] == "500.0000000000"
    assert r[0]["quote_volume"] == "0.9000000000"
    assert r[0]["price"] == format_10f(0.0018)


def test_get_average_price(setup_not_existing_pair, trades_info, no_trades_info):
    pair = setup_not_existing_pair
    r = pair.get_average_price(trades_info)
    assert r == 1
    r = pair.get_average_price(no_trades_info)
    assert r == 0


def test_get_volumes_and_prices(
    setup_kmd_ltc_str_pair_with_db,
    setup_not_existing_pair
):
    pair = setup_kmd_ltc_str_pair_with_db
    r = pair.get_volumes_and_prices()
    logger.info(r)
    assert r["base"] == "KMD"
    assert r["quote"] == "LTC"
    assert r["base_price"] == 1
    assert r["quote_price"] == 100
    assert float(r["base_volume"]) == 110
    assert float(r["quote_volume"]) == 2
    assert r["base_volume"] == format_10f(110)
    assert r["quote_volume"] == format_10f(2.0)
    assert float(r["highest_price_24h"]) == 0.1
    assert float(r["last_price"]) == 0.1
    assert float(r["lowest_price_24h"]) == 0.01
    assert float(r["price_change_percent_24h"]) == 0.0009
    assert float(r["price_change_24h"]) == 0.09
    assert r["trades_24hr"] == 2
    assert float(r["base_volume_usd"]) == 110.0
    assert float(r["quote_volume_usd"]) == 200
    assert float(r["combined_volume_usd"]) == 310
    assert float(r["last_trade"]) > int(time.time() - 86400)

    pair = setup_not_existing_pair
    r = pair.get_volumes_and_prices()
    assert float(r["last_price"]) == 0
    assert float(r["trades_24hr"]) == 0


def test_pair(
    setup_kmd_ltc_list_pair_with_db,
    setup_kmd_ltc_str_pair_with_db,
    setup_kmd_dgb_tuple_pair_with_db
):
    pair = setup_kmd_ltc_list_pair_with_db
    assert pair.base == "KMD"
    assert pair.quote == "LTC"
    assert pair.as_str == "KMD_LTC"
    assert pair.as_tuple == ("KMD", "LTC")
    pair = setup_kmd_ltc_str_pair_with_db
    assert pair.base == "KMD"
    assert pair.quote == "LTC"
    assert pair.as_str == "KMD_LTC"
    assert pair.as_tuple == ("KMD", "LTC")
    pair = setup_kmd_dgb_tuple_pair_with_db
    assert pair.as_str == "DGB_KMD"
    assert pair.base == "DGB"
    assert pair.quote == "KMD"
