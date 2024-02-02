#!/usr/bin/env python3
from decimal import Decimal
from copy import deepcopy
from tests.fixtures_transform import (
    setup_ticker_to_statsapi_24h,
    setup_ticker_to_statsapi_7d,
    setup_ticker_to_market_ticker,
    setup_historical_trades_to_market_trades,
)
from tests.fixtures_data import sampledata
from lib.generic import Generic
from util.logger import logger
from util.transform import (
    convert,
    sortdata,
    clean,
    merge,
    deplatform,
    sumdata,
    invert,
    filterdata,
)
import util.transform as transform
import util.memcache as memcache

generic = Generic()
gecko_source = memcache.get_gecko_source()
coins_config = memcache.get_coins_config()
last_traded_cache = memcache.get_last_traded()

logger.info("Testing transformations...")


def test_format_10f():
    assert transform.format_10f(1.234567890123456789) == "1.2345678901"
    assert transform.format_10f(1) == "1.0000000000"
    assert transform.format_10f(1.23) == "1.2300000000"


def test_ticker_to_market_summary_item():
    ticker_item = sampledata.ticker_item()
    x = convert.ticker_to_market_summary_item(ticker_item)
    assert x["trading_pair"] == "DGB_LTC"
    assert x["quote_currency"] == "LTC"
    assert ticker_item["quote_volume"] == x["quote_volume"]
    assert ticker_item["ticker_id"] == x["trading_pair"]
    assert ticker_item["last_swap_time"] == str(x["last_swap"])
    assert ticker_item["highest_price_24hr"] == x["highest_price_24hr"]
    assert ticker_item["lowest_price_24hr"] == x["lowest_price_24hr"]


def test_ticker_to_market_ticker(setup_ticker_to_market_ticker):
    x = setup_ticker_to_market_ticker
    ticker_item = sampledata.ticker_item()
    ticker = ticker_item["ticker_id"]
    assert ticker in x
    assert x[ticker]["isFrozen"] == "0"
    assert x[ticker]["quote_volume"] == ticker_item["quote_volume"]
    assert x[ticker]["base_volume"] == ticker_item["base_volume"]
    assert x[ticker]["last_swap_price"] == ticker_item["last_swap_price"]


def test_ticker_to_gecko_summary():
    x = transform.ticker_to_gecko_summary(sampledata.ticker_item())
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
    assert (
        sampledata.trades_info[0]["trade_id"] == "c76ed996-d44a-4e39-998e-acb68681b0f9"
    )
    assert sampledata.trades_info[0]["trade_id"] == x["trade_id"]
    assert sampledata.trades_info[0]["price"] == x["price"]
    assert sampledata.trades_info[0]["base_volume"] == x["base_volume"]
    assert sampledata.trades_info[0]["quote_volume"] == x["quote_volume"]
    assert sampledata.trades_info[0]["timestamp"] == x["timestamp"]
    assert sampledata.trades_info[0]["type"] == x["type"]


def test_historical_trades_to_gecko():
    x = convert.historical_trades_to_gecko(sampledata.trades_info[0])
    assert (
        sampledata.trades_info[0]["trade_id"] == "c76ed996-d44a-4e39-998e-acb68681b0f9"
    )
    assert sampledata.trades_info[0]["trade_id"] == x["trade_id"]
    assert sampledata.trades_info[0]["price"] == x["price"]
    assert sampledata.trades_info[0]["base_volume"] == x["base_volume"]
    assert sampledata.trades_info[0]["quote_volume"] == x["target_volume"]
    assert sampledata.trades_info[0]["timestamp"] == x["timestamp"]
    assert sampledata.trades_info[0]["type"] == x["type"]


def test_round_to_str():
    assert transform.round_to_str(1.23456789, 4) == "1.2346"
    assert transform.round_to_str("1.23456789", 8) == "1.23456789"
    assert transform.round_to_str(Decimal(), 2) == "0.00"
    assert transform.round_to_str("foo", 4) == "0.0000"
    assert transform.round_to_str({"foo": "bar"}, 1) == "0.0"


def test_clean_s():
    x = [sampledata.dirty_dict.copy(), sampledata.dirty_dict.copy()]
    r = clean.decimal_dict_lists(x)
    assert isinstance(r[0]["a"], float)
    assert isinstance(r[0]["b"], str)
    assert isinstance(r[0]["c"], int)
    assert isinstance(r[0]["d"], bool)
    assert isinstance(r[0]["e"], list)
    assert isinstance(r[0]["f"], dict)
    x = [sampledata.dirty_dict.copy(), sampledata.dirty_dict.copy()]
    r = clean.decimal_dict_lists(x, True)
    assert isinstance(r[1]["a"], str)


def test_clean_decimal_dict():
    x = sampledata.dirty_dict.copy()
    r = clean.decimal_dicts(x)
    assert isinstance(r["a"], float)
    assert isinstance(r["b"], str)
    assert isinstance(r["c"], int)
    assert isinstance(r["d"], bool)
    x = sampledata.dirty_dict.copy()
    r = clean.decimal_dicts(x, True, 6)
    assert isinstance(r["a"], str)


def test_list_json_key():
    assert (
        filterdata.dict_lists(sampledata.historical_trades, "type", "buy")
        == sampledata.historical_data["buy"]
    )
    assert (
        filterdata.dict_lists(sampledata.historical_trades, "type", "sell")
        == sampledata.historical_data["sell"]
    )


def test_sum_json_key():
    assert sumdata.json_key(sampledata.trades_info, "base_volume") == Decimal("60")
    assert sumdata.json_key(sampledata.trades_info, "quote_volume") == Decimal("59.5")


def test_sum_json_key_10f():
    assert (
        sumdata.json_key_10f(sampledata.trades_info, "base_volume") == "60.0000000000"
    )
    assert (
        sumdata.json_key_10f(sampledata.trades_info, "quote_volume") == "59.5000000000"
    )


def test_get_suffix():
    assert transform.get_suffix(1) == "24hr"
    assert transform.get_suffix(8) == "8d"


def test_sort_dicts():
    x = sortdata.dicts(sampledata.trades_info.copy()[0])
    y = list(x.keys())
    assert y[0] == "base_volume"
    x = sortdata.dicts(sampledata.trades_info.copy()[0], True)
    y = list(x.keys())
    assert y[0] == "type"


def test_sort_dict_list():
    x = sortdata.dict_lists(sampledata.trades_info.copy(), "trade_id")
    assert x[0]["trade_id"] == "2b22b6b9-c7b2-48c4-acb7-ed9077c8f47d"
    x = sortdata.dict_lists(sampledata.trades_info.copy(), "trade_id", True)
    assert x[0]["trade_id"] == "d2602fa9-6680-42f9-9cb8-20f76275f587"


def test_generic_orderbook_to_gecko():
    r = transform.orderbook_to_gecko(sampledata.orderbook_as_string)
    assert len(r["bids"]) == len(sampledata.orderbook_as_coords["bids"])
    assert len(r["bids"][0][1]) == len(sampledata.orderbook_as_coords["bids"][0][1])
    assert len(r["asks"][0][1]) == len(sampledata.orderbook_as_coords["asks"][0][1])


def test_pair_by_market_cap():
    a = sortdata.pair_by_market_cap(("BTC-segwit_KMD"))
    b = sortdata.pair_by_market_cap(("BTC_KMD"))
    c = sortdata.pair_by_market_cap(("KMD_BTC-segwit"))
    d = sortdata.pair_by_market_cap(("KMD_BTC"))
    e = sortdata.pair_by_market_cap(("BTC-segwit_KMD-BEP20"))

    assert b == d
    assert a == c
    assert e == "KMD-BEP20_BTC-segwit"


def test_sort_top_items():
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
    assert sortdata.top_items(data, "name", 2)[1]["name"] == "Jess"
    assert sortdata.top_items(data, "age", 2)[1]["age"] == 4


def test_deplatform_pair():
    r = deplatform.pair("KMD-BEP20_DGB-segwit")
    assert r == "KMD_DGB"


def test_deplatform_coin():
    r = deplatform.coin("USDC-PLG20")
    assert r == "USDC"


def test_merge_orderbooks():
    orderbook_data = generic.orderbook("KMD_DOGE", all=False)
    book = deepcopy(orderbook_data)
    book2 = deepcopy(orderbook_data)
    x = merge.orderbooks(book, book2)
    assert x["base"] == orderbook_data["base"]
    assert x["quote"] == orderbook_data["quote"]
    assert x["timestamp"] == orderbook_data["timestamp"]
    assert len(x["bids"]) == len(orderbook_data["bids"]) * 2
    assert len(x["asks"]) == len(orderbook_data["asks"]) * 2
    for i in [
        "liquidity_in_usd",
        "total_asks_base_vol",
        "total_bids_base_vol",
        "total_asks_quote_vol",
        "total_bids_quote_vol",
        "total_asks_base_usd",
        "total_bids_quote_usd",
    ]:
        assert Decimal(x[i]) == Decimal(orderbook_data[i]) * 2


def test_invert_pair():
    assert invert.pair("KMD_LTC") == "LTC_KMD"
    assert invert.pair("KMD_LTC-segwit") == "LTC-segwit_KMD"
    assert invert.pair("LTC_KMD") == "KMD_LTC"
    assert invert.pair("LTC-segwit_KMD") == "KMD_LTC-segwit"


def test_invert_trade_type():
    assert invert.trade_type("buy") == "sell"
    assert invert.trade_type("sell") == "buy"


def test_invert_orderbook():
    # TODO: Prep fixture and tests for invert.orderbook
    pass
