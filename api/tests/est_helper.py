import pytest
from decimal import Decimal
from fixtures import (
    historical_data,
    historical_trades,
    trades_info,
    coins_config,
    setup_gecko,
)
from util.helper import *
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL



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


def test_get_mm2_rpc_port():
    assert get_mm2_rpc_port("7777") == 7877
    assert get_mm2_rpc_port("8762") == 7862
    assert get_mm2_rpc_port("ALL") == "ALL"


def test_get_sqlite_db_paths():
    assert get_sqlite_db_paths(netid="7777") == MM2_DB_PATH_7777
    assert get_sqlite_db_paths(netid="8762") == MM2_DB_PATH_8762
    assert get_sqlite_db_paths(netid="ALL") == MM2_DB_PATH_ALL


def test_get_netid_filename():
    assert get_netid_filename("filename.ext", "7777") == "filename_7777.ext"


def test_get_netid():
    assert get_netid("file_7777.db") == "7777"
    assert get_netid("7777_file.db") == "7777"
    assert get_netid("file_7777_backup.db") == "7777"
    assert get_netid("file_MM2.db") == "8762"
    assert get_netid("seed_file.db") == "7777"
    assert get_netid("node_file.db") == "ALL"


def test_get_all_coin_pairs():
    pass


def test_is_7777():
    assert is_source_db("seed_MM2.db")
    assert not is_source_db("xyz_seed.db")


def test_is_source_db():
    assert is_source_db("xyz_MM2.db")
    assert not is_source_db("xyz_MM2x.db")


def test_is_pair_priced():
    pass


def test_save_json():
    pass
