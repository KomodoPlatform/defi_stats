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
    get_last_trade_time,
    get_last_trade_price,
    get_last_trade_uuid,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from util.logger import logger


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


def test_get_last_trade_price(setup_last_traded_cache):
    cache = setup_last_traded_cache
    r = get_last_trade_price("KMD_LTC", cache)
    assert r == Decimal("5.0000000000")
    r2 = get_last_trade_price("LTC_KMD", cache)
    assert r == r2
    r = get_last_trade_price("DOGE_XXX", cache)
    assert r == 0


def test_get_last_trade_time(setup_last_traded_cache):
    cache = setup_last_traded_cache
    r = get_last_trade_time("DOGE_LTC-segwit", cache)
    assert r > 0
    r2 = get_last_trade_time("LTC_DOGE", cache)
    assert r == r2
    r = get_last_trade_time("DOGE_XXX", cache)
    assert r == 0


def test_get_last_trade_uuid(setup_last_traded_cache):
    cache = setup_last_traded_cache
    r = get_last_trade_uuid("DOGE_LTC", cache)
    assert r == "EEEEEEEE-ee4b-494f-a2fb-48467614b613"
    r2 = get_last_trade_uuid("LTC_DOGE", cache)
    assert r == r2
    r = get_last_trade_uuid("DOGE_XXX", cache)
    assert r == ""
