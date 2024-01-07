#!/usr/bin/env python3
import time
import pytest
from decimal import Decimal
from copy import deepcopy

from fixtures_db import (
    setup_time,
    setup_swaps_db_data,
)
from fixtures_pair import (
    setup_kmd_dgb_pair,
    setup_dgb_kmd_pair,
    setup_kmd_btc_pair,
    setup_ltc_kmd_pair,
    setup_kmd_ltc_pair,
    setup_not_existing_pair,
    setup_1inch_usdc_pair,
)

from util.logger import logger
from fixtures_class import helper
from fixtures_data import (
    trades_info,
    ticker_item,
    no_trades_info,
)
from util.transform import merge_orderbooks, format_10f


from lib.cache_load import (
    load_gecko_source,
    load_coins_config,
)

coins_config = load_coins_config(testing=True)
gecko_source = load_gecko_source(testing=True)


def test_kmd_ltc_pair(setup_kmd_ltc_pair, setup_ltc_kmd_pair):
    pair = setup_kmd_ltc_pair
    assert pair.is_tradable
    assert pair.info["ticker_id"] == "KMD_LTC"
    assert not pair.inverse_requested

    pair = setup_ltc_kmd_pair
    assert pair.inverse_requested


def test_historical_trades(
    setup_kmd_ltc_pair,
    setup_ltc_kmd_pair,
):
    pair = setup_kmd_ltc_pair
    r = pair.historical_trades(trade_type="all")
    r["trades_count"] = len(r["buy"]) + len(r["sell"])
    logger.info(f"buy: {r['buy']}")
    logger.info(f"sell: {r['sell']}")
    assert r["trades_count"] == 3
    assert r["buy"][0]["type"] == "buy"
    assert r["buy"][0]["base_volume"] == format_10f(1)
    assert r["buy"][0]["target_volume"] == format_10f(5)
    assert r["buy"][0]["price"] == format_10f(5)

    pair = setup_ltc_kmd_pair
    r2 = pair.historical_trades("buy")["buy"]
    assert len(r2) == len(r["sell"])
    assert len(r2) == 2
    assert r2[0]["timestamp"] > r2[1]["timestamp"]
    r3 = pair.historical_trades("all")["sell"]
    assert len(r3) == len(r["buy"])
    assert len(r3) == 1
    assert r3[0]["type"] == "sell"
    assert r3[0]["base_volume"] == format_10f(5)
    assert r3[0]["target_volume"] == format_10f(1)
    assert r3[0]["price"] == format_10f(0.2)


def test_get_average_price(setup_not_existing_pair):
    pair = setup_not_existing_pair
    r = pair.get_average_price(trades_info)
    assert r == 1
    r = pair.get_average_price(no_trades_info)
    assert r == 0


def test_get_volumes_and_prices(setup_kmd_ltc_pair, setup_not_existing_pair):
    pair = setup_kmd_ltc_pair
    logger.calc(pair.testing)
    r = pair.get_volumes_and_prices()
    logger.info(r)
    assert r["base"] == "KMD"
    assert r["quote"] == "LTC"
    assert r["base_price"] == 1
    assert r["trades_24hr"] == 3
    assert r["quote_price"] == 100
    assert float(r["base_volume"]) == 3
    assert float(r["quote_volume"]) == 35
    assert r["base_volume"] == format_10f(3)
    assert r["quote_volume"] == format_10f(35)
    assert float(r["highest_price_24hr"]) == 20
    assert float(r["last_price"]) == 5
    assert float(r["lowest_price_24hr"]) == 5
    assert float(r["price_change_24hr"]) == -15
    assert float(r["price_change_percent_24hr"]) == -0.75
    assert float(r["base_volume_usd"]) == 3
    assert float(r["quote_volume_usd"]) == 3500
    # average of base and rel volume
    assert float(r["combined_volume_usd"]) == 3503 / 2
    assert float(r["last_trade"]) > int(time.time() - 86400)

    pair = setup_not_existing_pair
    r = pair.get_volumes_and_prices()
    assert float(r["last_price"]) == 0
    assert float(r["trades_24hr"]) == 0


def test_pair(setup_ltc_kmd_pair, setup_kmd_dgb_pair):
    pair = setup_ltc_kmd_pair
    assert pair.base == "KMD"
    assert pair.quote == "LTC"
    assert pair.as_str == "KMD_LTC"
    assert pair.as_tuple == ("KMD", "LTC")
    pair = setup_kmd_dgb_pair
    assert not pair.as_str == "DGB_KMD"
    assert pair.as_str == "KMD_DGB"
    assert not pair.quote == "KMD"
    assert not pair.base == "DGB"
    assert pair.base == "KMD"
    assert pair.quote == "DGB"


def test_merge_orderbooks(setup_kmd_ltc_pair):
    orderbook_data = setup_kmd_ltc_pair.orderbook_data
    book = deepcopy(orderbook_data)
    book2 = deepcopy(orderbook_data)
    x = merge_orderbooks(book, book2)
    assert x["ticker_id"] == orderbook_data["ticker_id"]
    assert x["base"] == orderbook_data["base"]
    assert x["quote"] == orderbook_data["quote"]
    assert x["timestamp"] == orderbook_data["timestamp"]
    assert len(x["bids"]) == len(orderbook_data["bids"]) * 2
    assert len(x["asks"]) == len(orderbook_data["asks"]) * 2
    assert x["liquidity_usd"] == orderbook_data["liquidity_usd"] * 2
    assert x["total_asks_base_vol"] == orderbook_data["total_asks_base_vol"] * 2
    assert x["total_bids_base_vol"] == orderbook_data["total_bids_base_vol"] * 2
    assert x["total_asks_quote_vol"] == orderbook_data["total_asks_quote_vol"] * 2
    assert x["total_bids_quote_vol"] == orderbook_data["total_bids_quote_vol"] * 2
    assert x["total_asks_base_usd"] == orderbook_data["total_asks_base_usd"] * 2
    assert x["total_bids_quote_usd"] == orderbook_data["total_bids_quote_usd"] * 2


def test_swap_uuids(setup_kmd_ltc_pair):
    r = setup_kmd_ltc_pair.swap_uuids()
    assert "77777777-2762-4633-8add-6ad2e9b1a4e7" in r
    assert len(r) == 3


def test_related_pairs(setup_kmd_ltc_pair, setup_1inch_usdc_pair):
    pair = setup_kmd_ltc_pair
    r = pair.related_pairs
    logger.warning(r)
    assert "KMD_LTC" in r
    assert "KMD-BEP20_LTC" in r
    assert len(r) == 4

    pair = setup_1inch_usdc_pair
    r = pair.related_pairs
    logger.info(pair.related_pairs)
    assert ("1INCH-AVX20_USDC-ERC20") in r
    assert ("1INCH-PLG20_USDC-BEP20") in r
    assert len(r) == 50
