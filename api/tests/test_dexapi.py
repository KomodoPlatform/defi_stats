#!/usr/bin/env python3
import time
import lib.dex_api as dex
import util.memcache as memcache

coins_config = memcache.get_coins_config()
gecko_source = memcache.get_gecko_source()


def test_orderbook():
    """
    Test orderbook() method of DexAPI class.
    TODO: Test for more response keys, see if
    any other values can be reliably static
    """
    api = dex.DexAPI()
    r = api.orderbook_rpc("KMD", "LTC")
    assert "bids" in r
    assert "asks" in r

    r = api.orderbook_rpc("KMD", "DASH")
    assert "bids" in r
    assert "asks" in r

    r = api.orderbook_rpc("XXX", "YYY")
    assert len(r["asks"]) == 0


def test_get_orderbook():
    r = dex.get_orderbook(
        "KMD",
        "LTC",
        coins_config=coins_config,
        variant_cache_name="KMD_LTC",
        gecko_source=gecko_source,
    )
    time.sleep(1)
    r = dex.get_orderbook(
        "KMD",
        "LTC",
        coins_config=coins_config,
        variant_cache_name="KMD_LTC",
        gecko_source=gecko_source,
    )
    
    assert r["pair"] == "KMD_LTC"
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert isinstance(r["asks"][0], dict)
    assert isinstance(r["bids"][0], dict)
    assert "volume" in r["asks"][0]
    assert "price" in r["bids"][0]

    # TODO: Inversion for orderbook
    r = dex.get_orderbook(
        "LTC",
        "KMD",
        coins_config,
        variant_cache_name="LTC_KMD",
        gecko_source=gecko_source,
        coins_config=coins_config,
    )
    assert r["pair"] == "LTC_KMD"
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert len(r["asks"][0]) == 3
    assert len(r["bids"][0]) == 3
    assert isinstance(r["asks"][0], dict)
    assert isinstance(r["bids"][0], dict)
    assert "volume" in r["asks"][0]
    assert "price" in r["bids"][0]
