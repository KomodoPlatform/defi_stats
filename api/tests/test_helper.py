import pytest
from decimal import Decimal
from tests.fixtures_data import swap_item
from tests.fixtures_class import setup_last_traded_cache
from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
    get_pairs_info,
    get_pair_info_sorted,
    get_last_trade_item,
    get_coin_variants,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from util.logger import logger

from lib.cache import load_gecko_source, load_coins_config, load_generic_last_traded

coins_config = load_coins_config()


def test_get_mm2_rpc_port():
    assert get_mm2_rpc_port("7777") == 7877
    assert get_mm2_rpc_port(7777) == 7877
    assert get_mm2_rpc_port("8762") == 7862
    assert get_mm2_rpc_port("ALL") == 7862

    with pytest.raises(Exception):
        assert get_mm2_rpc_port("nope") == 7862


def test_get_netid_filename():
    assert get_netid_filename("filename.ext", "7777") == "filename_7777.ext"


def test_get_chunks():
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunks = list(get_chunks(data, 3))
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[3]) == 1


def test_get_price_at_finish():
    r = get_price_at_finish(swap_item)
    assert "1700000777" in r
    assert r["1700000777"] == Decimal(4) / Decimal(5)


def test_get_pairs_info():
    pairs = ["KMD_LTC", "LTC-segwit_KMD", "DOC_LTC"]
    r = get_pairs_info(pairs, True)
    assert len(r) == 3
    assert r[0]["priced"]


def test_get_pair_info_sorted():
    pairs = ["KMD_LTC", "LTC-segwit_KMD", "DOC_LTC", "BTC_DGB-segwit"]
    r = get_pair_info_sorted(pairs, False)
    assert len(r) == 4
    assert r[0]["ticker_id"] == "BTC_DGB-segwit"
    assert not r[0]["priced"]


def test_get_last_trade_item(setup_last_traded_cache):
    cache = setup_last_traded_cache
    r = get_last_trade_item("DOGE_LTC", cache, "last_swap_uuid")
    assert r == "EEEEEEEE-ee4b-494f-a2fb-48467614b613"
    r2 = get_last_trade_item("LTC_DOGE", cache, "last_swap_uuid")
    assert r == r2
    r = get_last_trade_item("DOGE_XXX", cache, "last_swap_uuid")
    assert r == ""

    r = get_last_trade_item("DOGE_LTC-segwit", cache, "last_swap")
    assert r > 0
    r2 = get_last_trade_item("LTC_DOGE", cache, "last_swap")
    assert r == r2
    r = get_last_trade_item("DOGE_XXX", cache, "last_swap")
    assert r == 0

    r = get_last_trade_item("KMD_LTC", cache, "last_price")
    assert r == Decimal("20.0000000000")
    r2 = get_last_trade_item("LTC_KMD", cache, "last_price")
    assert r2 == Decimal("20.0000000000")
    assert r == r2
    r = get_last_trade_item("DOGE_XXX", cache, "last_price")
    assert r == 0


def test_get_coin_variants():
    r = get_coin_variants("BTC", coins_config)
    assert "BTC-BEP20" in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    logger.merge(r)
    assert len(r) > 2
    r = get_coin_variants("BTC", coins_config, True)
    assert "BTC-BEP20" not in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    logger.merge(r)
    assert len(r) == 2
