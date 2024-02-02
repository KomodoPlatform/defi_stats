#!/usr/bin/env python3
from decimal import Decimal
from tests.fixtures_class import helper
from tests.fixtures_data import sampledata
from tests.fixtures_pair import (
    setup_kmd_dgb_pair,
    setup_ltc_kmd_pair,
    setup_kmd_ltc_pair,
    setup_not_existing_pair,
    setup_1inch_usdc_pair,
    setup_morty_kmd_pair,
)
from util.logger import logger
from util.transform import clean
import util.cron as cron
import util.transform as transform


# TODO: Tests for USDC and testcoins


def test_get_swap_prices(setup_ltc_kmd_pair):
    pair = setup_ltc_kmd_pair
    r1 = pair.get_swap_prices(sampledata.swaps_for_pair, is_reversed=False)
    logger.calc(r1)
    r2 = pair.get_swap_prices(sampledata.swaps_for_pair, is_reversed=True)
    logger.info(r2)
    assert r1 == r2


def test_historical_trades(
    setup_kmd_ltc_pair,
    setup_ltc_kmd_pair,
):
    # Test ALL, then variants
    pair = setup_kmd_ltc_pair
    r_all = pair.historical_trades()["ALL"]
    assert len(r_all["sell"]) == 1
    assert len(r_all["buy"]) == 2
    assert r_all["ticker_id"] == "KMD_LTC"
    assert r_all["buy"][0]["type"] == "buy"
    assert r_all["buy"][0]["base_volume"] == transform.format_10f(100)
    assert r_all["buy"][0]["quote_volume"] == transform.format_10f(1)
    assert r_all["buy"][0]["timestamp"] > r_all["buy"][1]["timestamp"]
    assert r_all["sell"][0]["type"] == "sell"
    assert r_all["sell"][0]["base_coin_ticker"] == "KMD"
    assert r_all["sell"][0]["quote_coin_ticker"] == "LTC"
    assert r_all["sell"][0]["base_volume"] == transform.format_10f(100)
    assert r_all["sell"][0]["quote_volume"] == transform.format_10f(1)
    assert Decimal(r_all["buy"][0]["price"]) == Decimal("0.01")
    assert Decimal(r_all["sell"][0]["price"]) == Decimal("0.01")

    # Test std pair
    pair = setup_kmd_ltc_pair
    r_std = pair.historical_trades()["KMD_LTC"]
    assert len(r_std["buy"]) == 1
    assert len(r_std["sell"]) == 0
    assert r_std["buy"][0]["type"] == "buy"
    assert r_std["buy"][0]["base_volume"] == transform.format_10f(200)
    assert r_std["buy"][0]["quote_volume"] == transform.format_10f(2)
    assert Decimal(r_std["buy"][0]["price"]) == Decimal("0.01")

    # Test segwit pair
    pair = setup_kmd_ltc_pair
    r_segwit = pair.historical_trades()["KMD_LTC-segwit"]
    assert len(r_segwit["buy"]) == 1
    assert len(r_segwit["sell"]) == 1

    # Test reverse pair
    pair = setup_ltc_kmd_pair
    r_inverted = pair.historical_trades()
    assert "LTC_KMD" in r_inverted.keys()
    r_inverted = r_inverted["ALL"]
    assert pair.is_reversed
    assert len(r_inverted["buy"]) == len(r_all["sell"])
    assert len(r_inverted["sell"]) == len(r_all["buy"])
    assert len(r_inverted["buy"]) == 1
    assert len(r_inverted["sell"]) == 2
    assert r_inverted["sell"][0]["pair"] == "LTC-segwit_KMD"
    assert r_inverted["sell"][0]["base_coin_ticker"] == "LTC"
    assert r_inverted["sell"][0]["base_coin_platform"] == "segwit"
    assert r_inverted["sell"][0]["quote_coin_ticker"] == "KMD"
    assert r_inverted["sell"][0]["quote_coin_platform"] == ""
    assert Decimal(r_inverted["buy"][0]["price"]) == Decimal("100")
    assert Decimal(r_inverted["sell"][0]["price"]) == Decimal("100")
    assert r_inverted["ticker_id"] == "LTC_KMD"
    assert len(r_all) == len(r_inverted)
    assert r_all["sell"][0]["base_volume"] == r_inverted["buy"][0]["quote_volume"]
    assert r_all["sell"][0]["quote_volume"] == r_inverted["buy"][0]["base_volume"]
    assert Decimal(r_all["sell"][0]["price"]) == 1 / Decimal(r_inverted["buy"][0]["price"])


def test_get_average_price(setup_kmd_ltc_pair, setup_not_existing_pair):
    pair = setup_not_existing_pair
    r = pair.get_average_price(sampledata.trades_info)
    assert r == 1
    r = pair.get_average_price(sampledata.no_trades_info)
    assert r == 0


def test_get_volumes_and_prices(
    setup_kmd_ltc_pair, setup_ltc_kmd_pair, setup_not_existing_pair
):
    pair = setup_kmd_ltc_pair
    r = pair.get_volumes_and_prices()
    r = clean.decimal_dicts(r)
    assert r["base"] == "KMD"
    assert r["quote"] == "LTC"
    assert r["base_price_usd"] == 1
    assert r["trades_24hr"] == 3
    assert r["quote_price_usd"] == 100
    assert float(r["base_volume"]) == 400
    assert float(r["quote_volume"]) == 4
    assert r["base_volume"] == 400
    assert r["quote_volume"] == 4
    assert float(r["highest_price_24hr"]) == 0.01
    assert r["last_swap_uuid"] == "666666666-75a2-d4ef-009d-5e9baad162ef"
    assert float(r["lowest_price_24hr"]) == 0.01
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
    assert r["base_price_usd"] == 100
    assert r["quote_price_usd"] == 1
    assert r["trades_24hr"] == 3
    assert r["base_volume"] == 4
    assert r["quote_volume"] == 400
    assert float(r["highest_price_24hr"]) == 100
    assert float(r["lowest_price_24hr"]) == 100


def test_pair(
    setup_ltc_kmd_pair,
    setup_kmd_dgb_pair,
):
    pair = setup_ltc_kmd_pair
    assert pair.base == "LTC"
    assert pair.quote == "KMD"
    assert pair.as_str == "LTC_KMD"
    pair = setup_kmd_dgb_pair
    assert not pair.as_str == "DGB_KMD"
    assert pair.as_str == "KMD_DGB"
    assert not pair.quote == "KMD"
    assert not pair.base == "DGB"
    assert pair.base == "KMD"
    assert pair.quote == "DGB"


def test_swap_uuids(setup_kmd_ltc_pair):
    r = setup_kmd_ltc_pair.swap_uuids()
    assert "77777777-2762-4633-8add-6ad2e9b1a4e7" in r["uuids"]
    assert len(r["uuids"]) == 3


def test_first_last_swap(setup_kmd_ltc_pair, setup_ltc_kmd_pair):
    pair = setup_ltc_kmd_pair
    variants = helper.get_pair_variants(pair.as_str, segwit_only=False)
    data = pair.first_last_swap(variants)
    assert data["last_swap_uuid"] == "666666666-75a2-d4ef-009d-5e9baad162ef"
    assert data["last_swap_price"] == 100

    pair = setup_kmd_ltc_pair
    variants = helper.get_pair_variants(pair.as_str)
    data = pair.first_last_swap(variants)
    assert data["last_swap_uuid"] == "666666666-75a2-d4ef-009d-5e9baad162ef"
    assert data["last_swap_price"] == 0.01

    pair = setup_kmd_ltc_pair
    variants = helper.get_pair_variants(pair.as_str, segwit_only=True)
    data = pair.first_last_swap(variants)
    assert data["last_swap_uuid"] == "666666666-75a2-d4ef-009d-5e9baad162ef"
    assert data["last_swap_price"] == 0.01
