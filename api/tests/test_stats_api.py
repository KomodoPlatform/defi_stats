#!/usr/bin/env python3
import pytest
from util.logger import logger

from fixtures_db import setup_swaps_db_data, setup_time
from fixtures_class import setup_statsapi


def test_pair_summaries(setup_statsapi):
    stats = setup_statsapi
    r = stats.pair_summaries(1)
    logger.info(r[0])
    assert len(r) == 7
    r = stats.pair_summaries(300)
    logger.info(r[0])
    assert len(r) == 8


def test_top_pairs(setup_statsapi):
    stats = setup_statsapi
    summaries = stats.pair_summaries(14)
    r = stats.top_pairs(summaries)
    logger.info(r)
    assert len(r) == 3
    assert r["by_swaps_count"]["KMD_LTC"] == 3
    assert "KMD_BTC" in r["by_value_traded_usd"]


def test_adex_fortnite(setup_statsapi):
    stats = setup_statsapi
    r = stats.adex_fortnite()
    logger.info(r)
    assert r["days"] == 14
    assert r["swaps_count"] == 12
    assert "KMD_LTC" in r["top_pairs"]["by_current_liquidity_usd"]
