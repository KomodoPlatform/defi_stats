#!/usr/bin/env python3
from decimal import Decimal
from fixtures import (
    API_ROOT_PATH, setup_utils,
    setup_kmd_btc_orderbook_data,
    setup_kmd_ltc_str_pair,
    setup_kmd_btc_list_pair,
    setup_swaps_db_data,
    setup_fake_db, setup_time,
    setup_cache, dirty_dict
)


def test_find_lowest_ask(setup_utils, setup_kmd_btc_orderbook_data):
    utils = setup_utils
    orderbook = setup_kmd_btc_orderbook_data
    r = utils.find_lowest_ask(orderbook)
    assert r == "26.0000000000"


def test_find_highest_bid(setup_utils, setup_kmd_btc_orderbook_data):
    utils = setup_utils
    orderbook = setup_kmd_btc_orderbook_data
    r = utils.find_highest_bid(orderbook)
    assert r == "24.0000000000"


def test_get_suffix(setup_utils):
    utils = setup_utils
    assert utils.get_suffix(1) == "24h"
    assert utils.get_suffix(8) == "8d"


def test_get_chunks(setup_utils):
    utils = setup_utils
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunks = list(utils.get_chunks(data, 3))
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[3]) == 1


def test_get_gecko_usd_price(setup_utils, setup_cache):
    utils = setup_utils
    cache = setup_cache
    source_path = f"{API_ROOT_PATH}/tests/fixtures/gecko/source_cache.json"
    assert cache.files.gecko_source_file == source_path
    assert utils.get_gecko_usd_price("KMD", cache.gecko_source_cache) == 1
    assert utils.get_gecko_usd_price(
        "BTC", cache.gecko_source_cache) == 1000000
    price1 = utils.get_gecko_usd_price("LTC-segwit", cache.gecko_source_cache)
    price2 = utils.get_gecko_usd_price("LTC", cache.gecko_source_cache)
    assert price1 == price2


def test_round_to_str(setup_utils):
    utils = setup_utils
    assert utils.round_to_str(1.23456789, 4) == "1.2346"
    assert utils.round_to_str("1.23456789", 8) == "1.23456789"
    assert utils.round_to_str(Decimal(), 2) == "0.00"
    assert utils.round_to_str("foo", 4) == "0.0000"
    assert utils.round_to_str({"foo": "bar"}, 1) == "0.0"


def test_load_jsonfile(setup_utils):
    utils = setup_utils
    assert "error" in utils.load_jsonfile("foo", 2)


def test_download_json(setup_utils):
    utils = setup_utils
    data = utils.download_json("https://api.coingecko.com/api/v3/coins/list")
    assert len(data) > 0
    data = utils.download_json("foo")
    assert data is None


def test_clean_decimal_dict(setup_utils, dirty_dict):
    utils = setup_utils
    assert isinstance(dirty_dict["a"], Decimal)
    r = utils.clean_decimal_dict(dirty_dict.copy())
    assert isinstance(r["a"], float)
    r = utils.clean_decimal_dict(dirty_dict.copy())
    assert r["a"] == 1.23456789
    r = utils.clean_decimal_dict(dirty_dict.copy(), to_string=True)
    assert isinstance(r["a"], str)
    r = utils.clean_decimal_dict(dirty_dict.copy(), to_string=True, rounding=2)
    assert r["a"] == "1.23"

    for i in ["b", "c", "d", "e"]:
        assert r[i] == dirty_dict[i]


def test_get_related_coins(setup_utils):
    utils = setup_utils

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
    setup_kmd_ltc_str_pair,
    setup_kmd_btc_list_pair
):
    utils = setup_utils
    pair = setup_kmd_ltc_str_pair
    r = utils.get_related_pairs(pair)
    assert ('KMD', 'LTC') in r
    assert ('KMD-BEP20', 'LTC') in r
    assert len(r) == 2

    pair = setup_kmd_btc_list_pair
    r = utils.get_related_pairs(pair)
    assert ('KMD', 'BTC') in r
    assert ('KMD-BEP20', 'BTC') in r
    assert len(r) == 4


def test_clean_decimal_dict_list(setup_utils, dirty_dict):
    x = [dirty_dict.copy(), dirty_dict.copy()]
    utils = setup_utils
    r = utils.clean_decimal_dict_list(x)
    assert isinstance(r[0]["a"], float)
    assert isinstance(r[0]["b"], str)
    assert isinstance(r[0]["c"], int)
    assert isinstance(r[0]["d"], bool)
    assert isinstance(r[0]["e"], list)
    assert isinstance(r[0]["f"], dict)
