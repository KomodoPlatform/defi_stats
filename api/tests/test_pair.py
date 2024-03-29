#!/usr/bin/env python3
from decimal import Decimal
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
from util.transform import clean, derive, convert
from util.cron import cron
import util.helper as helper


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
    assert r_all["buy"][0]["base_volume"] == convert.format_10f(100)
    assert r_all["buy"][0]["quote_volume"] == convert.format_10f(1)
    assert r_all["buy"][0]["timestamp"] > r_all["buy"][1]["timestamp"]
    assert r_all["sell"][0]["type"] == "sell"
    assert r_all["sell"][0]["base_coin_ticker"] == "KMD"
    assert r_all["sell"][0]["quote_coin_ticker"] == "LTC"
    assert r_all["sell"][0]["base_volume"] == convert.format_10f(100)
    assert r_all["sell"][0]["quote_volume"] == convert.format_10f(1)
    assert Decimal(r_all["buy"][0]["price"]) == Decimal("0.01")
    assert Decimal(r_all["sell"][0]["price"]) == Decimal("0.01")

    # Test std pair
    pair = setup_kmd_ltc_pair
    r_std = pair.historical_trades()["KMD_LTC"]
    assert len(r_std["buy"]) == 1
    assert len(r_std["sell"]) == 0
    assert r_std["buy"][0]["type"] == "buy"
    assert r_std["buy"][0]["base_volume"] == convert.format_10f(200)
    assert r_std["buy"][0]["quote_volume"] == convert.format_10f(2)
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
    assert Decimal(r_all["sell"][0]["price"]) == 1 / Decimal(
        r_inverted["buy"][0]["price"]
    )


def test_get_average_price(setup_kmd_ltc_pair, setup_not_existing_pair):
    pair = setup_not_existing_pair
    r = pair.get_average_price(sampledata.historical_trades)
    assert r == 1
    r = pair.get_average_price(sampledata.no_trades_info)
    assert r == 0


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
