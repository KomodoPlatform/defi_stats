import time
import pytest
from decimal import Decimal
from lib.pair import Pair
from tests.fixtures_data import swap_item, swap_item2
from util.logger import logger
import util.helper as helper
import util.memcache as memcache
import util.transform as transform


def test_get_mm2_rpc_port():
    assert helper.get_mm2_rpc_port("7777") == 7877
    assert helper.get_mm2_rpc_port(7777) == 7877
    assert helper.get_mm2_rpc_port("8762") == 7862
    assert helper.get_mm2_rpc_port("ALL") == 7862

    with pytest.raises(Exception):
        assert helper.get_mm2_rpc_port("nope") == 7862


def test_get_netid_filename():
    assert helper.get_netid_filename("filename.ext", "7777") == "filename_7777.ext"


def test_get_chunks():
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunks = list(helper.get_chunks(data, 3))
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[3]) == 1


def test_get_pairs_info():
    pairs = ["KMD_LTC", "LTC-segwit_KMD", "DOC_LTC"]
    r = helper.get_pairs_info(pairs, True)
    assert len(r) == 3
    assert r[0]["priced"]


def test_get_pair_info_sorted():
    pairs = ["KMD_LTC", "LTC-segwit_KMD", "DOC_LTC", "BTC_DGB-segwit"]
    r = helper.get_pair_info_sorted(pairs, False)
    assert len(r) == 4
    assert r[0]["ticker_id"] == "BTC_DGB-segwit"
    assert not r[0]["priced"]
