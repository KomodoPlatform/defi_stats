#!/usr/bin/env python3
import util.cron as cron
import pytest
from decimal import Decimal
from copy import deepcopy
from tests.fixtures_class import helper
from tests.fixtures_data import (
    trades_info,
    no_trades_info,
)
from tests.fixtures_db import (
    setup_time,
    setup_swaps_db_data,
)
from tests.fixtures_pair import (
    setup_kmd_dgb_pair,
    setup_dgb_kmd_pair,
    setup_kmd_btc_pair,
    setup_ltc_kmd_pair,
    setup_kmd_ltc_pair,
    setup_not_existing_pair,
    setup_1inch_usdc_pair,
    setup_morty_kmd_pair,
)
from util.logger import logger
import util.memcache as memcache
import util.transform as transform


gecko_source = memcache.get_gecko_source()
coins_config = memcache.get_coins_config()
last_traded_cache = memcache.get_last_traded()


def test_kmd_ltc_pair(setup_kmd_ltc_pair, setup_ltc_kmd_pair):
    pair = setup_kmd_ltc_pair
    assert pair.is_tradable
    assert pair.info["ticker_id"] == "KMD_LTC"


def test_historical_trades(
    setup_kmd_ltc_pair,
    setup_ltc_kmd_pair,
):
    pair = setup_kmd_ltc_pair
    r = pair.historical_trades()["ALL"]
    assert len(r["sell"]) == 1
    assert len(r["buy"]) == 2
    assert r["buy"][0]["type"] == "buy"
    assert r["sell"][0]["type"] == "sell"
    assert r["buy"][0]["base_volume"] == transform.format_10f(100)
    assert r["buy"][0]["target_volume"] == transform.format_10f(1)
    assert r["buy"][0]["price"] == transform.format_10f(100)
    assert r["sell"][0]["base_volume"] == transform.format_10f(100)
    assert r["sell"][0]["target_volume"] == transform.format_10f(1)
    assert r["sell"][0]["price"] == transform.format_10f(100)
    assert r["buy"][0]["timestamp"] > r["buy"][1]["timestamp"]

    pair = setup_kmd_ltc_pair
    r = pair.historical_trades()["KMD_LTC"]

    assert len(r["buy"]) == 1
    assert len(r["sell"]) == 0
    assert r["buy"][0]["type"] == "buy"
    assert r["buy"][0]["base_volume"] == transform.format_10f(200)
    assert r["buy"][0]["target_volume"] == transform.format_10f(2)
    assert r["buy"][0]["price"] == transform.format_10f(100)

    pair = setup_kmd_ltc_pair
    r = pair.historical_trades()["KMD_LTC-segwit"]

    assert len(r["buy"]) == 1
    assert len(r["sell"]) == 1

    pair = setup_ltc_kmd_pair
    r2 = pair.historical_trades()["ALL"]
    assert len(r2["buy"]) == len(r["sell"])
    assert len(r2["buy"]) == 1
    r2 = pair.historical_trades()["ALL"]
    assert r2["ticker_id"] == "LTC_KMD"
    # TODO: Is inversion propogated?
    r2b = pair.historical_trades()
    assert "LTC_KMD" in r2b.keys()

    pair = setup_kmd_ltc_pair
    r3 = pair.historical_trades()["ALL"]
    assert r3["ticker_id"] == "KMD_LTC"
    assert len(r3) == len(r2)
    assert r3["sell"][0]["type"] == "sell"
    assert r2["buy"][0]["type"] == "buy"
    assert r3["sell"][0]["base_volume"] == r2["buy"][0]["target_volume"]
    assert r3["sell"][0]["target_volume"] == r2["buy"][0]["base_volume"]
    assert Decimal(r3["sell"][0]["price"]) == 1 / Decimal(r2["buy"][0]["price"])


def test_get_average_price(setup_not_existing_pair):
    pair = setup_not_existing_pair
    r = pair.get_average_price(trades_info)
    assert r == 1
    r = pair.get_average_price(no_trades_info)
    assert r == 0


def test_get_volumes_and_prices(
    setup_kmd_ltc_pair, setup_ltc_kmd_pair, setup_not_existing_pair
):
    pair = setup_kmd_ltc_pair
    r = pair.get_volumes_and_prices()
    r = transform.clean_decimal_dict(r)
    assert r["base"] == "KMD"
    assert r["quote"] == "LTC"
    assert r["base_price"] == 1
    assert r["trades_24hr"] == 3
    assert r["quote_price"] == 100
    assert float(r["base_volume"]) == 400
    assert float(r["quote_volume"]) == 4
    assert r["base_volume"] == 400
    assert r["quote_volume"] == 4
    assert float(r["highest_price_24hr"]) == 100
    assert r["last_swap_uuid"] == "666666666-75a2-d4ef-009d-5e9baad162ef"
    assert float(r["lowest_price_24hr"]) == 100
    assert float(r["price_change_24hr"]) == 0
    assert float(r["price_change_pct_24hr"]) == 0
    assert float(r["base_volume_usd"]) == 400
    assert float(r["quote_volume_usd"]) == 400
    # average of base and rel volume
    assert float(r["combined_volume_usd"]) == 800 / 2
    assert float(r["last_swap_time"]) > int(cron.now_utc() - 86400)

    pair = setup_not_existing_pair
    r = pair.get_volumes_and_prices()
    assert float(r["last_swap_price"]) == 0
    assert float(r["trades_24hr"]) == 0

    pair = setup_ltc_kmd_pair
    r = pair.get_volumes_and_prices()
    assert r["base"] == "LTC"
    assert r["quote"] == "KMD"
    assert r["base_price"] == 100
    assert r["quote_price"] == 1
    assert r["trades_24hr"] == 3
    assert r["base_volume"] == 4
    assert r["quote_volume"] == 400
    assert float(r["highest_price_24hr"]) == 0.01


def test_pair(
    setup_ltc_kmd_pair,
    setup_kmd_dgb_pair,
    setup_not_existing_pair,
    setup_morty_kmd_pair,
):
    pair = setup_ltc_kmd_pair
    assert pair.base == "LTC"
    assert pair.quote == "KMD"
    assert pair.as_str == "LTC_KMD"
    assert pair.as_tuple == ("LTC", "KMD")
    assert pair.is_priced
    pair = setup_kmd_dgb_pair
    assert not pair.as_str == "DGB_KMD"
    assert pair.as_str == "KMD_DGB"
    assert not pair.quote == "KMD"
    assert not pair.base == "DGB"
    assert pair.base == "KMD"
    assert pair.quote == "DGB"
    assert pair.is_priced
    pair = setup_not_existing_pair
    assert not pair.is_priced
    pair = setup_morty_kmd_pair
    assert not pair.is_priced


def test_merge_orderbooks(setup_kmd_ltc_pair):
    orderbook_data = setup_kmd_ltc_pair.orderbook.for_pair(depth=100, all=False)
    book = deepcopy(orderbook_data)
    book2 = deepcopy(orderbook_data)
    x = transform.merge_orderbooks(book, book2)
    assert x["pair"] == orderbook_data["pair"]
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
    assert "77777777-2762-4633-8add-6ad2e9b1a4e7" in r["uuids"]
    assert len(r["uuids"]) == 3


def test_first_last_swap(setup_kmd_ltc_pair, setup_ltc_kmd_pair):
    pair = setup_kmd_ltc_pair
    variants = sorted([i for i in pair.swap_uuids()["variants"] if i != "ALL"])
    data = pair.first_last_swap(variants)
    assert data["last_swap_uuid"] == "666666666-75a2-d4ef-009d-5e9baad162ef"
    assert data["last_swap_price"] == 100

    pair = setup_ltc_kmd_pair
    variants = sorted([i for i in pair.swap_uuids()["variants"] if i != "ALL"])
    data = pair.first_last_swap(variants)
    assert data["last_swap_uuid"] == "666666666-75a2-d4ef-009d-5e9baad162ef"
    assert data["last_swap_price"] == 0.01
