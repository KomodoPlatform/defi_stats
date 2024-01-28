import pytest
from decimal import Decimal
from tests.fixtures_transform import (
    setup_ticker_to_statsapi_24h,
    setup_ticker_to_statsapi_7d,
    setup_ticker_to_market_ticker,
    setup_ticker_to_market_ticker_summary,
    setup_historical_trades_to_market_trades,
    setup_ticker_to_gecko,
)
from tests.fixtures_db import setup_swaps_db_data, setup_time
from tests.fixtures_class import setup_gecko

from tests.fixtures_data import (
    historical_data,
    historical_trades,
    trades_info,
    get_ticker_item,
    dirty_dict,
    orderbook_as_string,
    orderbook_as_coords,
)


from util.logger import logger
from util.transform import merge, sortdata, clean
import util.transform as transform
import util.memcache as memcache


def test_format_10f():
    assert transform.format_10f(1.234567890123456789) == "1.2345678901"
    assert transform.format_10f(1) == "1.0000000000"
    assert transform.format_10f(1.23) == "1.2300000000"


def test_ticker_to_market_ticker_summary(setup_ticker_to_market_ticker_summary):
    x = setup_ticker_to_market_ticker_summary
    ticker_item = get_ticker_item()
    assert x["ticker_id"] == "DGB_LTC"
    assert x["quote_currency"] == "LTC"
    assert ticker_item["quote_volume"] == x["quote_volume"]
    assert ticker_item["ticker_id"] == x["ticker_id"]
    assert ticker_item["last_swap_time"] == str(x["last_swap"])
    assert ticker_item["highest_price_24hr"] == x["highest_price_24hr"]
    assert ticker_item["lowest_price_24hr"] == x["lowest_price_24hr"]


def test_ticker_to_market_ticker(setup_ticker_to_market_ticker):
    x = setup_ticker_to_market_ticker
    ticker_item = get_ticker_item()
    ticker = ticker_item["ticker_id"]
    assert ticker in x
    assert x[ticker]["isFrozen"] == "0"
    assert x[ticker]["quote_volume"] == ticker_item["quote_volume"]
    assert x[ticker]["base_volume"] == ticker_item["base_volume"]
    assert x[ticker]["last_swap_price"] == ticker_item["last_swap_price"]


def test_ticker_to_gecko(setup_ticker_to_gecko):
    x = setup_ticker_to_gecko
    assert x["ticker_id"] == x["pool_id"]


def test_ticker_to_statsapi(setup_ticker_to_statsapi_24h, setup_ticker_to_statsapi_7d):
    x = setup_ticker_to_statsapi_7d
    y = setup_ticker_to_statsapi_24h
    assert x["ticker_id"] == y["ticker_id"]
    assert "price_change_24h" in y
    assert "price_change_7d" in x
    assert "quote_price_usd" in x
    assert "quote_price_usd" in y
    assert "pair_liquidity_usd" in x
    assert "pair_liquidity_usd" in y
    assert isinstance(x["last_swap_time"], int)
    assert isinstance(y["lowest_ask"], Decimal)


def test_historical_trades_to_market_trades(setup_historical_trades_to_market_trades):
    x = setup_historical_trades_to_market_trades
    assert trades_info[0]["trade_id"] == "c76ed996-d44a-4e39-998e-acb68681b0f9"
    assert trades_info[0]["trade_id"] == x["trade_id"]
    assert trades_info[0]["price"] == x["price"]
    assert trades_info[0]["base_volume"] == x["base_volume"]
    assert trades_info[0]["quote_volume"] == x["quote_volume"]
    assert trades_info[0]["timestamp"] == x["timestamp"]
    assert trades_info[0]["type"] == x["type"]


def test_historical_trades_to_gecko():
    x = transform.historical_trades_to_gecko(trades_info[0])
    assert trades_info[0]["trade_id"] == "c76ed996-d44a-4e39-998e-acb68681b0f9"
    assert trades_info[0]["trade_id"] == x["trade_id"]
    assert trades_info[0]["price"] == x["price"]
    assert trades_info[0]["base_volume"] == x["base_volume"]
    assert trades_info[0]["quote_volume"] == x["quote_volume"]
    assert trades_info[0]["timestamp"] == x["timestamp"]
    assert trades_info[0]["type"] == x["type"]


def test_round_to_str():
    assert transform.round_to_str(1.23456789, 4) == "1.2346"
    assert transform.round_to_str("1.23456789", 8) == "1.23456789"
    assert transform.round_to_str(Decimal(), 2) == "0.00"
    assert transform.round_to_str("foo", 4) == "0.0000"
    assert transform.round_to_str({"foo": "bar"}, 1) == "0.0"


def test_clean_decimal_dict_list():
    x = [dirty_dict.copy(), dirty_dict.copy()]
    r = clean.decimal_dict_list(x)
    assert isinstance(r[0]["a"], float)
    assert isinstance(r[0]["b"], str)
    assert isinstance(r[0]["c"], int)
    assert isinstance(r[0]["d"], bool)
    assert isinstance(r[0]["e"], list)
    assert isinstance(r[0]["f"], dict)
    x = [dirty_dict.copy(), dirty_dict.copy()]
    r = clean.decimal_dict_list(x, True)
    assert isinstance(r[1]["a"], str)


def test_clean_decimal_dict():
    x = dirty_dict.copy()
    r = clean.decimal_dict(x)
    assert isinstance(r["a"], float)
    assert isinstance(r["b"], str)
    assert isinstance(r["c"], int)
    assert isinstance(r["d"], bool)
    x = dirty_dict.copy()
    r = clean.decimal_dict(x, True, 6)
    assert isinstance(r["a"], str)


def test_list_json_key():
    assert (
        transform.list_json_key(historical_trades, "type", "buy")
        == historical_data["buy"]
    )
    assert (
        transform.list_json_key(historical_trades, "type", "sell")
        == historical_data["sell"]
    )


def test_sum_json_key():
    assert transform.sum_json_key(trades_info, "base_volume") == Decimal("60")
    assert transform.sum_json_key(trades_info, "quote_volume") == Decimal("59.5")


def test_sum_json_key_10f():
    assert transform.sum_json_key_10f(trades_info, "base_volume") == "60.0000000000"
    assert transform.sum_json_key_10f(trades_info, "quote_volume") == "59.5000000000"


def test_get_suffix():
    assert transform.get_suffix(1) == "24hr"
    assert transform.get_suffix(8) == "8d"


def test_sort_dict():
    x = sortdata.sort_dict(trades_info.copy()[0])
    y = list(x.keys())
    assert y[0] == "base_volume"
    x = sortdata.sort_dict(trades_info.copy()[0], True)
    y = list(x.keys())
    assert y[0] == "type"


def test_sort_dict_list():
    x = sortdata.sort_dict_list(trades_info.copy(), "trade_id")
    assert x[0]["trade_id"] == "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d"
    x = sortdata.sort_dict_list(trades_info.copy(), "trade_id", True)
    assert x[0]["trade_id"] == "d2602fa9-6680-42f9-9cb8-20f76275f587"


def test_generic_orderbook_to_gecko():
    r = transform.orderbook_to_gecko(orderbook_as_string)
    assert len(r["bids"]) == len(orderbook_as_coords["bids"])
    assert len(r["bids"][0][1]) == len(orderbook_as_coords["bids"][0][1])
    assert len(r["asks"][0][1]) == len(orderbook_as_coords["asks"][0][1])


def test_order_pair_by_market_cap():
    a = sortdata.order_pair_by_market_cap(("BTC-segwit_KMD"))
    b = sortdata.order_pair_by_market_cap(("BTC_KMD"))
    c = sortdata.order_pair_by_market_cap(("KMD_BTC-segwit"))
    d = sortdata.order_pair_by_market_cap(("KMD_BTC"))
    e = sortdata.order_pair_by_market_cap(("BTC-segwit_KMD-BEP20"))

    assert b == d
    assert a == c
    assert e == "KMD-BEP20_BTC-segwit"


def test_get_top_items():
    data = [
        {
            "name": "Bob",
            "age": 1,
        },
        {
            "name": "Alice",
            "age": 2,
        },
        {
            "name": "Jess",
            "age": 3,
        },
        {
            "name": "Zelda",
            "age": 4,
        },
        {
            "name": "April",
            "age": 5,
        },
    ]
    assert sortdata.get_top_items(data, "name", 2)[1]["name"] == "Jess"
    assert sortdata.get_top_items(data, "age", 2)[1]["age"] == 4


def test_strip_pair_platforms():
    r = transform.strip_pair_platforms("KMD-BEP20_DGB-segwit")
    assert r == "KMD_DGB"


def test_strip_coin_platform():
    r = transform.strip_coin_platform("USDC-PLG20")
    assert r == "USDC"
