import pytest
from decimal import Decimal
from tests.fixtures_data import swap_item
from util.helper import (
    get_mm2_rpc_port,
    get_chunks,
    get_price_at_finish,
    base_quote_from_pair,
)
from lib.coins import get_pairs_info, get_pair_info_sorted
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL

from util.logger import logger
import util.helper as helper
import util.memcache as memcache


def test_get_mm2_rpc_port():
    assert get_mm2_rpc_port("7777") == 7877
    assert get_mm2_rpc_port(7777) == 7877
    assert get_mm2_rpc_port("8762") == 7862
    assert get_mm2_rpc_port("ALL") == 7862

    with pytest.raises(Exception):
        assert get_mm2_rpc_port("nope") == 7862


def test_get_netid_filename():
    assert helper.get_netid_filename("filename.ext", "7777") == "filename_7777.ext"


def test_get_chunks():
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunks = list(get_chunks(data, 3))
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[3]) == 1


def test_get_price_at_finish():
    r = get_price_at_finish(swap_item)
    assert "1700000777" in r
    assert r["1700000777"] == Decimal(5) / Decimal(4)


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


def test_base_quote_from_pair():
    base, quote = base_quote_from_pair("XXX-PLG20_OLD_YYY-PLG20_OLD")
    assert base == "XXX-PLG20_OLD"
    assert quote == "YYY-PLG20_OLD"


def test_get_coin_variants():
    r = helper.get_coin_variants("BTC")
    assert "BTC-BEP20" in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    assert len(r) > 2
    r = helper.get_coin_variants("BTC", True)
    assert "BTC-BEP20" not in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    assert len(r) == 2

    r = helper.get_coin_variants("USDC")
    assert "USDC-BEP20" in r
    assert "USDC-PLG20" in r
    assert "USDC-PLG20_OLD" in r
    assert "BTC-segwit" not in r
    assert "USDC" not in r
    assert len(r) > 6


def test_get_pair_variants():
    r = helper.get_pair_variants("KMD_LTC")
    assert "KMD_LTC" in r
    assert "KMD_LTC-segwit" in r
    assert "KMD-BEP20_LTC" in r
    assert "KMD-BEP20_LTC-segwit" in r
    assert len(r) == 4

    r = helper.get_pair_variants("LTC_KMD")
    assert "LTC_KMD" in r
    assert "LTC-segwit_KMD" in r
    assert "LTC_KMD-BEP20" in r
    assert "LTC-segwit_KMD-BEP20" in r
    assert len(r) == 4

    r = helper.get_pair_variants("KMD_USDC")
    assert "KMD_USDC" not in r
    assert "KMD_USDC-PLG20" in r
    assert "KMD_USDC-PLG20_OLD" in r
    assert "KMD-BEP20_USDC-PLG20_OLD" in r
