#!/usr/bin/env python3
import time
import pytest
from fixtures import (
    setup_cache,
    setup_helper,
    setup_swaps_db_data,
    setup_swaps_db_data,
    API_ROOT_PATH,
    logger,
)

from fixtures_cron import (
    setup_time,
)
from util.transform import merge_orderbooks, format_10f

# /////////////////////// #
# Cache.calc class tests  #
# /////////////////////// #


# /////////////////////// #
# Cache.save class tests  #
# /////////////////////// #
def test_save_gecko(setup_cache):
    save = setup_cache.save
    path = f"{API_ROOT_PATH}/tests/fixtures/test_save.json"

    data = "foo bar"
    with pytest.raises(TypeError):
        r = save.save(path, data)

    data = {"foo": "bar"}
    with pytest.raises(Exception):
        r = save.save(path, data)
        assert r is None

    r = save.save(path, data)
    assert "result" in r
    assert r["result"].startswith("Updated")

    with pytest.raises(TypeError):
        r = save.save(path, None)

    with pytest.raises(TypeError):
        r = save.save(None, None)

    with pytest.raises(TypeError):
        r = save.save(None, path)


# /////////////////////// #
# Cache.load class tests  #
# /////////////////////// #


def test_calc_traded_tickers(setup_cache, setup_helper, setup_swaps_db_data):
    helper = setup_helper
    markets = setup_cache.markets
    r = markets.calc_traded_tickers(
        DB=setup_swaps_db_data,
    )
    assert len(r) > 0
    assert isinstance(r, dict)
    assert "last_update" in r
    for i in r["data"]:
        logger.info(f"{i['ticker_id']}: [{i['trades_24hr']}] [{i['volume_usd_24hr']}]")
    assert r["swaps_count"] == 7
    assert r["pairs_count"] == 6
    assert len(r["data"]) == 6
    assert "combined_volume_usd" in r
    assert isinstance(r["data"], list)
    assert isinstance(r["data"][0], dict)
    assert r["data"][0]["ticker_id"] == "DGB_KMD-BEP20"
    assert r["data"][0]["base_currency"] == "DGB"
    assert r["data"][0]["last_price"] == format_10f(0.0018000000)
    assert int(r["data"][0]["last_trade"]) > int(time.time() - 86400)
    assert r["data"][0]["trades_24hr"] == "2"
    assert r["data"][0]["base_volume"] == format_10f(1500)
    assert r["data"][0]["target_volume"] == format_10f(1.9)
    assert r["data"][0]["base_usd_price"] == format_10f(0.01)
    assert r["data"][0]["target_usd_price"] == format_10f(1)
    assert r["data"][0]["high"] == format_10f(0.0018)
    assert r["data"][0]["low"] == format_10f(0.001)
    assert "volume_usd_24hr" in r["data"][0]
    assert "ask" in r["data"][0]
    assert "bid" in r["data"][0]


def test_calc_traded_pairs(setup_cache, setup_swaps_db_data):
    cache = setup_cache
    r = cache.calc.calc_traded_pairs(
        days=7, exclude_unpriced=False, DB=setup_swaps_db_data, include_all_kmd=False
    )
    r2 = cache.calc.calc_traded_pairs(DB=setup_swaps_db_data)
    logger.info(r)
    logger.info(r2)
    assert len(r) == 7
    assert len(r2) == 6
    assert len(r) > len(r2)
    assert isinstance(r, list)
    assert isinstance(r[0], dict)
