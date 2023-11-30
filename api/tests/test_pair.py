#!/usr/bin/env python3
import time
import pytest
from decimal import Decimal
from fixtures import (
    setup_swaps_db_data,
    setup_fake_db,
    setup_time,
    setup_kmd_dgb_tuple_pair,
    setup_dgb_kmd_str_pair,
    setup_kmd_ltc_str_pair,
    setup_ltc_kmd_list_pair,
    setup_not_existing_pair,
    trades_info,
    no_trades_info,
    logger,
    helper
)


def test_get_pairs_type_error():
    with pytest.raises(TypeError):
        helper.set_pair_as_tuple(15)


def test_get_pairs_value_error():
    with pytest.raises(ValueError):
        helper.set_pair_as_tuple("Pair_of_three")


def test_historical_trades(
    setup_kmd_ltc_str_pair,
    setup_ltc_kmd_list_pair,
    setup_swaps_db_data,
):
    DB = setup_swaps_db_data
    pair = setup_kmd_ltc_str_pair
    r = pair.historical_trades(trade_type='all', DB=DB, netid=7777)["buy"]
    assert len(r) == 1
    logger.info(r[0])
    assert r[0]["type"] == "buy"
    assert r[0]["base_volume"] == helper.format_10f(1)
    assert r[0]["target_volume"] == helper.format_10f(5)
    assert r[0]["price"] == helper.format_10f(5)

    pair = setup_ltc_kmd_list_pair
    r = pair.historical_trades('all', DB=DB, netid=7777)["buy"]
    assert len(r) == 1
    r = pair.historical_trades('all', DB=DB, netid=7777)["sell"]
    assert len(r) == 2
    logger.info(r[0])
    assert r[0]["type"] == "sell"
    assert r[0]["timestamp"] > r[1]["timestamp"]
    assert r[0]["base_volume"] == helper.format_10f(1)
    assert r[0]["target_volume"] == helper.format_10f(10)
    assert r[0]["price"] == helper.format_10f(10)


def test_get_average_price(setup_not_existing_pair, trades_info, no_trades_info):
    pair = setup_not_existing_pair
    r = pair.get_average_price(trades_info)
    assert r == 1
    r = pair.get_average_price(no_trades_info)
    assert r == 0


def test_get_volumes_and_prices(
    setup_kmd_ltc_str_pair,
    setup_not_existing_pair,
    setup_swaps_db_data
):
    pair = setup_kmd_ltc_str_pair
    r = pair.get_volumes_and_prices(DB=setup_swaps_db_data)
    logger.info(r)
    assert r["base"] == "KMD"
    assert r["quote"] == "LTC"
    assert r["base_price"] == 1
    assert r["trades_24hr"] == 3
    assert r["quote_price"] == 100
    assert float(r["base_volume"]) == 3
    assert float(r["quote_volume"]) == 35
    assert r["base_volume"] == helper.format_10f(3)
    assert r["quote_volume"] == helper.format_10f(35)
    assert float(r["highest_price_24h"]) == 20
    assert float(r["last_price"]) == 5
    assert float(r["lowest_price_24h"]) == 5
    assert float(r["price_change_24h"]) == -15
    assert float(r["price_change_percent_24h"]) == -0.75
    assert float(r["base_volume_usd"]) == 3
    assert float(r["quote_volume_usd"]) == 3500
    assert float(r["combined_volume_usd"]) == 3503
    assert float(r["last_trade"]) > int(time.time() - 86400)

    pair = setup_not_existing_pair
    r = pair.get_volumes_and_prices(DB=setup_swaps_db_data)
    assert float(r["last_price"]) == 0
    assert float(r["trades_24hr"]) == 0


def test_pair(
    setup_ltc_kmd_list_pair,
    setup_kmd_ltc_str_pair,
    setup_kmd_dgb_tuple_pair
):
    pair = setup_ltc_kmd_list_pair
    assert pair.base == "KMD"
    assert pair.quote == "LTC"
    assert pair.as_str == "KMD_LTC"
    assert pair.as_tuple == ("KMD", "LTC")
    pair = setup_kmd_ltc_str_pair
    assert pair.base == "KMD"
    assert pair.quote == "LTC"
    assert pair.as_str == "KMD_LTC"
    assert pair.as_tuple == ("KMD", "LTC")
    pair = setup_kmd_dgb_tuple_pair
    assert pair.as_str == "DGB_KMD"
    assert pair.base == "DGB"
    assert pair.quote == "KMD"
