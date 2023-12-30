import pytest
from decimal import Decimal
from fixtures_transform import (
    setup_ticker_to_market_ticker,
    setup_ticker_to_market_ticker_summary,
    setup_historical_trades_to_market_trades,
)

from fixtures_data import (
    historical_data,
    historical_trades,
    trades_info,
    ticker_item,
    dirty_dict,
)

from util.transform import (
    get_suffix,
    clean_decimal_dict,
    clean_decimal_dict_list,
    round_to_str,
    list_json_key,
    sum_json_key,
    sum_json_key_10f,
    sort_dict_list,
    sort_dict,
)


def test_ticker_to_market_ticker_summary(setup_ticker_to_market_ticker_summary):
    x = setup_ticker_to_market_ticker_summary
    assert x["trading_pair"] == "KMD_LTC"
    assert x["quote_currency"] == "LTC"
    assert ticker_item["target_volume"] == x["quote_volume"]
    assert ticker_item["ticker_id"] == x["trading_pair"]
    assert ticker_item["last_trade"] == str(x["last_swap_timestamp"])
    assert ticker_item["high"] == x["highest_price_24h"]
    assert ticker_item["low"] == x["lowest_price_24h"]


def test_ticker_to_market_ticker(setup_ticker_to_market_ticker):
    x = setup_ticker_to_market_ticker
    ticker = ticker_item["ticker_id"]
    assert ticker in x
    assert x[ticker]["isFrozen"] == "0"
    assert x[ticker]["quote_volume"] == ticker_item["target_volume"]
    assert x[ticker]["base_volume"] == ticker_item["base_volume"]
    assert x[ticker]["last_price"] == ticker_item["last_price"]


def test_historical_trades_to_market_trades(setup_historical_trades_to_market_trades):
    x = setup_historical_trades_to_market_trades
    assert trades_info[0]["trade_id"] == "c76ed996-d44a-4e39-998e-acb68681b0f9"
    assert trades_info[0]["trade_id"] == x["trade_id"]
    assert trades_info[0]["price"] == x["price"]
    assert trades_info[0]["base_volume"] == x["base_volume"]
    assert trades_info[0]["target_volume"] == x["quote_volume"]
    assert trades_info[0]["timestamp"] == x["timestamp"]
    assert trades_info[0]["type"] == x["type"]


def test_round_to_str():
    assert round_to_str(1.23456789, 4) == "1.2346"
    assert round_to_str("1.23456789", 8) == "1.23456789"
    assert round_to_str(Decimal(), 2) == "0.00"
    assert round_to_str("foo", 4) == "0.0000"
    assert round_to_str({"foo": "bar"}, 1) == "0.0"


def test_clean_decimal_dict_list():
    x = [dirty_dict.copy(), dirty_dict.copy()]
    r = clean_decimal_dict_list(x)
    assert isinstance(r[0]["a"], float)
    assert isinstance(r[0]["b"], str)
    assert isinstance(r[0]["c"], int)
    assert isinstance(r[0]["d"], bool)
    assert isinstance(r[0]["e"], list)
    assert isinstance(r[0]["f"], dict)
    x = [dirty_dict.copy(), dirty_dict.copy()]
    r = clean_decimal_dict_list(x, True)
    assert isinstance(r[1]["a"], str)


def test_clean_decimal_dict():
    x = dirty_dict.copy()
    r = clean_decimal_dict(x)
    print(r)
    assert isinstance(r["a"], float)
    assert isinstance(r["b"], str)
    assert isinstance(r["c"], int)
    assert isinstance(r["d"], bool)
    x = dirty_dict.copy()
    r = clean_decimal_dict(x, True, 6)
    assert isinstance(r["a"], str)


def test_list_json_key():
    assert list_json_key(historical_trades, "type", "buy") == historical_data["buy"]
    assert list_json_key(historical_trades, "type", "sell") == historical_data["sell"]


def test_sum_json_key():
    assert sum_json_key(trades_info, "base_volume") == Decimal("60")
    assert sum_json_key(trades_info, "target_volume") == Decimal("59.5")


def test_sum_json_key_10f():
    assert sum_json_key_10f(trades_info, "base_volume") == "60.0000000000"
    assert sum_json_key_10f(trades_info, "target_volume") == "59.5000000000"


def test_get_suffix():
    assert get_suffix(1) == "24h"
    assert get_suffix(8) == "8d"


def test_sort_dict():
    x = sort_dict(trades_info.copy()[0])
    y = list(x.keys())
    assert y[0] == "base_volume"
    x = sort_dict(trades_info.copy()[0], True)
    y = list(x.keys())
    assert y[0] == "type"


def test_sort_dict_list():
    x = sort_dict_list(trades_info.copy(), "trade_id")
    assert x[0]["trade_id"] == "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d"
    x = sort_dict_list(trades_info.copy(), "trade_id", True)
    assert x[0]["trade_id"] == "d2602fa9-6680-42f9-9cb8-20f76275f587"
