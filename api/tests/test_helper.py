import pytest
from decimal import Decimal
from fixtures_data import swap_item
from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
    is_pair_priced,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL


def test_get_mm2_rpc_port():
    assert get_mm2_rpc_port("7777") == 7877
    assert get_mm2_rpc_port(7777) == 7877
    assert get_mm2_rpc_port("8762") == 7862
    assert get_mm2_rpc_port("ALL") == "ALL"

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
    assert r["1700000777"] == Decimal(4)/Decimal(5)


def test_is_pair_priced():
    assert is_pair_priced("KMD", "LTC")
    assert is_pair_priced("KMD", "LTC-segwit")
    assert not is_pair_priced("KMD", "DOC")
