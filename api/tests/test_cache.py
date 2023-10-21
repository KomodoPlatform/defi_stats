#!/usr/bin/env python3
import time
import pytest
from fixtures import setup_cache, setup_utils, setup_kmd_btc_orderbook_data, \
    setup_swaps_test_data, setup_database, setup_time, logger
from helper import format_10f


# /////////////////////// #
# Cache.calc class tests  #
# /////////////////////// #


def test_calc_gecko_source(setup_cache):
    calc = setup_cache.calc
    r = calc.gecko_source()
    assert len(r) > 0


# /////////////////////// #
# Cache.save class tests  #
# /////////////////////// #
def test_save_gecko(setup_cache):
    save = setup_cache.save
    path = "tests/fixtures/test_save.json"

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

    r = save.save(setup_cache.files.coins_config, data)
    assert "result" in r
    assert r["result"].startswith("Validated")
    assert r["result"] == f"Validated {setup_cache.files.coins_config} data"

    r = save.save(setup_cache.files.gecko_source, data)
    assert "result" in r
    assert r["result"].startswith("Validated")
    assert r["result"] == f"Validated {setup_cache.files.gecko_source} data"


def test_save_coins(setup_cache):
    save = setup_cache.save
    assert "result" in save.coins()
    r = save.coins("foo")
    assert r is None


def test_save_coins_config(setup_cache):
    save = setup_cache.save
    assert "result" in save.coins_config()
    r = save.coins_config("foo")
    assert r is None


# /////////////////////// #
# Cache.load class tests  #
# /////////////////////// #


def test_load_coins_config(setup_cache):
    load = setup_cache.load
    data = load.coins_config()
    assert "KMD" in data
    assert "KMD-BEP20" in data
    assert "LTC-segwit" in data
    assert "LTC" in data
    for i in data:
        assert i == data[i]["coin"]
        assert "coingecko_id" in data[i]


def test_load_coins(setup_cache):
    load = setup_cache.load
    assert len(load.coins()) > 0


def test_load_gecko(setup_cache):
    load = setup_cache.load
    gecko = load.gecko_source()
    assert "KMD" in gecko
    assert gecko["KMD"]["usd_market_cap"] == gecko["KMD-BEP20"]["usd_market_cap"]
    assert gecko["KMD"]["usd_price"] == gecko["KMD-BEP20"]["usd_price"]
    assert gecko["KMD"]["coingecko_id"] == gecko["KMD-BEP20"]["coingecko_id"]
    for i in gecko["KMD"]:
        assert i in ["usd_market_cap", "usd_price", "coingecko_id"]
    for i in gecko:
        assert gecko[i]["coingecko_id"] != ""


def test_calc_gecko_tickers(setup_cache):
    calc = setup_cache.calc
    r = calc.gecko_tickers()
    assert len(r) > 0
    assert isinstance(r, dict)
    assert "last_update" in r
    assert r["swaps_count"] == 8
    for i in r["data"]:
        logger.info(i)
    assert r["pairs_count"] == 6
    assert len(r["data"]) == 6
    assert "combined_volume_usd" in r
    assert isinstance(r["data"], list)
    assert isinstance(r["data"][1], dict)
    assert r["data"][1]["ticker_id"] == "DGB_KMD"
    assert r["data"][1]["base_currency"] == "DGB"
    assert r["data"][1]["last_price"] == format_10f(0.0018000000)
    assert int(r["data"][1]["last_trade"]) > int(time.time() - 86400)
    assert r["data"][1]["trades_24hr"] == "2"
    assert r["data"][1]["base_volume"] == format_10f(1500)
    assert r["data"][1]["target_volume"] == format_10f(1.9)
    assert r["data"][1]["base_usd_price"] == format_10f(0.01)
    assert r["data"][1]["target_usd_price"] == format_10f(1)
    assert r["data"][1]["high"] == format_10f(0.0018)
    assert r["data"][1]["low"] == format_10f(0.001)
    assert "volume_usd_24hr" in r["data"][1]
    assert "ask" in r["data"][1]
    assert "bid" in r["data"][1]
