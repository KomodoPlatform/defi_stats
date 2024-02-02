#!/usr/bin/env python3
import pytest
from util.logger import logger

from tests.fixtures_db import setup_swaps_db_data, setup_time
from tests.fixtures_class import setup_statsapi


# TODO: Use generic tickers for paur summaries
# Then restore tests
"""
def test_pair_summaries(setup_statsapi):
    stats = setup_statsapi
    r = stats.pair_summaries(1)
    assert len(r) == 6
    r = stats.pair_summaries(300)
    assert len(r) == 6
    for i in r:
        if i["ticker_id"] == "KMD_BTC":
            assert i["pair_swaps_count"] == 2
            assert i["quote_volume"] == 2
            assert i["base_volume"] == 3000000


def test_top_pairs(setup_statsapi):
    stats = setup_statsapi
    summaries = stats.pair_summaries(14)
    r = stats.top_pairs(summaries)
    assert len(r) == 3
    assert r["by_swaps_count"]["KMD_LTC"] == 3
    assert "KMD_BTC" in r["by_value_traded_usd"]


def test_adex_fortnite(setup_statsapi):
    stats = setup_statsapi
    r = stats.adex_fortnite()
    assert r["days"] == 14
    assert r["swaps_count"] == 11  # 15 - 1x failed - 3x > 14 days old
    assert "KMD_LTC" in r["top_pairs"]["by_current_liquidity_usd"]
"""
