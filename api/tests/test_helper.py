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


def test_format_10f():
    assert format_10f(1.234567890123456789) == "1.2345678901"
    assert format_10f(1)                    == "1.0000000000"
    assert format_10f(1.23)               == "1.2300000000"


def test_list_json_key(historical_data, historical_trades):
    assert list_json_key(historical_trades, "type", "buy") == historical_data["buy"]
    assert list_json_key(historical_trades, "type", "sell") == historical_data["sell"]


def test_sum_json_key(historical_data):
    assert sum_json_key(historical_data["buy"], "target_volume") == Decimal("60")
    assert sum_json_key(historical_data["sell"], "target_volume") == Decimal("30")


def test_sum_json_key_10f(historical_data, historical_trades):
    assert sum_json_key_10f(historical_data["buy"], "target_volume") == "60.0000000000"


def test_sort_dict_list(trades_info):
    a = sort_dict_list(trades_info, "target_volume", reverse=False)
    b = sort_dict_list(trades_info, "target_volume", reverse=True)
    c = sort_dict_list(trades_info, "trade_id", reverse=False)
    assert a[0]["target_volume"] == "16"
    assert b[0]["target_volume"] == "24"
    assert c[0]["trade_id"] == "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d"


def test_sort_dict(historical_data):
    a = sort_dict(historical_data, reverse=False)
    b = sort_dict(historical_data, reverse=True)
    assert list(a.keys())[0] == "average_price"
    assert list(b.keys())[0] == "trades_count"


def test_valid_coins(coins_config):
    config = coins_config
    coins = valid_coins(config)
    assert len(coins) == 1
    assert coins[0] == "OK"
    assert "TEST" not in coins
    assert "NOSWAP" not in coins


def test_set_pair_as_tuple():
    expected = ("DOC", "MARTY")
    assert set_pair_as_tuple(["DOC", "MARTY"]) == expected
    assert set_pair_as_tuple("DOC_MARTY") == expected
    with pytest.raises(ValueError) as e:
        assert set_pair_as_tuple("DOC") == expected
    with pytest.raises(TypeError) as e:
        assert set_pair_as_tuple(1) == expected


def test_order_pair_by_market_cap(setup_gecko):
    a = order_pair_by_market_cap(("BTC-segwit", "KMD"), setup_gecko.gecko_source)
    b = order_pair_by_market_cap(("BTC", "KMD"), setup_gecko.gecko_source)
    c = order_pair_by_market_cap(("KMD", "BTC-segwit"), setup_gecko.gecko_source)
    d = order_pair_by_market_cap(("KMD", "BTC"), setup_gecko.gecko_source)

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
