#!/usr/bin/env python3
from decimal import Decimal
from fixtures import (
    API_ROOT_PATH,
    setup_kmd_ltc_pair,
    setup_kmd_btc_pair,
)

from fixtures_data import (
    dirty_dict,
    setup_kmd_btc_orderbook_data,
)
from fixtures_db import (
    setup_swaps_db_data,
    setup_swaps_db_data,
)
from fixtures_cron import (
    setup_time,
)


def test_find_lowest_ask(setup_kmd_btc_orderbook_data):
    orderbook = setup_kmd_btc_orderbook_data
    r = utils.find_lowest_ask(orderbook)
    assert r == "26.0000000000"


def test_find_highest_bid(setup_kmd_btc_orderbook_data):
    orderbook = setup_kmd_btc_orderbook_data
    r = utils.find_highest_bid(orderbook)
    assert r == "24.0000000000"



def test_get_gecko_usd_price(setup_cache):
    cache = setup_cache
    source_path = f"{API_ROOT_PATH}/tests/fixtures/gecko/source_cache.json"
    assert cache.files.gecko_source == source_path
    assert utils.get_gecko_usd_price("KMD", cache.gecko_source_cache) == 1
    assert utils.get_gecko_usd_price(
        "BTC", cache.gecko_source_cache) == 1000000
    price1 = utils.get_gecko_usd_price("LTC-segwit", cache.gecko_source_cache)
    price2 = utils.get_gecko_usd_price("LTC", cache.gecko_source_cache)
    assert price1 == price2


def test_load_jsonfile():
    assert "error" in utils.load_jsonfile("foo", 2)


def test_download_json():
    data = utils.download_json("https://api.coingecko.com/api/v3/coins/list")
    assert len(data) > 0
    data = utils.download_json("foo")
    assert data is None


def test_get_related_coins():

    coins = utils.get_related_coins("LTC", exclude_segwit=False)
    assert "LTC" in coins
    assert "LTC-segwit" in coins

    coins = utils.get_related_coins("LTC", exclude_segwit=True)
    assert "LTC-segwit" not in coins

    coins = utils.get_related_coins("LTC")
    assert "LTC-segwit" not in coins

    coins = utils.get_related_coins("KMD")
    assert "KMD" in coins
    assert "KMD-BEP20" in coins

    coins = utils.get_related_coins("USDC-BEP20")
    assert "USDC" not in coins
    assert "USDC-BEP20" in coins
    assert "USDC-PLG20" in coins

    coins = utils.get_related_coins("BTC")
    assert len(coins) == 2

    coins = utils.get_related_coins("BTC", exclude_segwit=False)
    assert len(coins) == 3


def test_get_related_pairs(
    setup_utils,
    setup_kmd_ltc_pair,
    setup_kmd_btc_list_pair
):
    pair = setup_kmd_ltc_pair
    r = utils.get_related_pairs(pair)
    assert ('KMD', 'LTC') in r
    assert ('KMD-BEP20', 'LTC') in r
    assert len(r) == 2

    pair = setup_kmd_btc_list_pair
    r = utils.get_related_pairs(pair)
    assert ('KMD', 'BTC') in r
    assert ('KMD-BEP20', 'BTC') in r
    assert len(r) == 4



def test_valid_coins(coins_config):
    config = coins_config
    coins = get_valid_coins(config)
    assert len(coins) == 1
    assert coins[0] == "OK"
    assert "TEST" not in coins
    assert "NOSWAP" not in coins


def test_order_pair_by_market_cap(setup_gecko):
    a = order_pair_by_market_cap(("BTC-segwit_KMD"), setup_gecko.gecko_source)
    b = order_pair_by_market_cap(("BTC_KMD"), setup_gecko.gecko_source)
    c = order_pair_by_market_cap(("KMD_BTC-segwit"), setup_gecko.gecko_source)
    d = order_pair_by_market_cap(("KMD_BTC"), setup_gecko.gecko_source)

    assert a == c
    assert b == d


def test_get_gecko_source(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_gecko_source()
    if "error" not in r:
        assert round(r["USDC"]["usd_price"]) == 1


def test_get_gecko_source(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_gecko_source()
    assert r["KMD"]["usd_price"] == 1
    assert r["KMD"]["usd_market_cap"] == 777
    assert r["KMD"]["coingecko_id"] == "komodo"