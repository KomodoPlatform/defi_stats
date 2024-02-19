#!/usr/bin/env python3
from decimal import Decimal
from tests.fixtures_data import sampledata, swap_item, swap_item2
from lib.pair import Pair
from util.logger import logger
from util.transform import (
    convert,
    sortdata,
    clean,
    deplatform,
    sumdata,
    invert,
    filterdata,
    derive,
)
import util.memcache as memcache

gecko_source = memcache.get_gecko_source()
coins_config = memcache.get_coins_config()
pairs_last_traded_cache = memcache.get_pairs_last_traded()

logger.info("Testing transformations...")


# Clean
def test_clean_decimal_dict_lists():
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


def test_clean_decimal_dicts():
    x = sampledata.dirty_dict.copy()
    r = clean.decimal_dicts(x)
    assert isinstance(r["a"], float)
    assert isinstance(r["b"], str)
    assert isinstance(r["c"], int)
    assert isinstance(r["d"], bool)
    x = sampledata.dirty_dict.copy()
    r = clean.decimal_dicts(x, True, 6, ["c"])
    assert isinstance(r["a"], str)
    assert isinstance(r["c"], int)


def test_clean_orderbook_data():
    pass


def test_sumdata_ints():
    assert sumdata.ints(1, 2) == 3
    assert sumdata.ints(1, "2") == 3
    assert sumdata.ints("1", "2") == 3
    assert sumdata.ints("1.1", "2.2") == 3


def test_sumdata_lists():
    r = sumdata.lists([1, 5, 7, 3, 5, 2, 1], [0, 3, 3, 2])
    logger.calc(r)
    assert r[0] == 0
    assert len(r) == 6
    r = sumdata.lists(
        [{"x": 3, "y": 67}, {"x": 32, "y": 7}], [{"x": 35, "y": 5}, {"x": 2, "y": 27}]
    )
    assert len(r) == 4


def test_sumdata_dict_lists():
    r = sumdata.dict_lists(
        [{"x": 3, "y": 67}, {"x": 32, "y": 7}], [{"x": 35, "y": 5}, {"x": 2, "y": 27}]
    )
    assert r[0]["x"] == 3
    assert len(r) == 4
    r = sumdata.dict_lists(
        [{"x": 3, "y": 67}, {"x": 32, "y": 7}],
        [{"x": 35, "y": 5}, {"x": 2, "y": 27}],
        sort_key="x",
    )
    assert r[0]["x"] == 2
    assert len(r) == 4


def test_sumdata_numeric_str():
    assert sumdata.numeric_str("12", "4") == "16.0000000000"
    assert sumdata.numeric_str(12, "4") == "16.0000000000"
    assert sumdata.numeric_str(12, 4) == "16.0000000000"


def test_format_10f():
    assert convert.format_10f(1.234567890123456789) == "1.2345678901"
    assert convert.format_10f(1) == "1.0000000000"
    assert convert.format_10f("1.23") == "1.2300000000"


def test_historical_trades_to_gecko():
    x = convert.historical_trades_to_gecko(sampledata.historical_trades[0])
    assert (
        sampledata.historical_trades[0]["trade_id"]
        == "c76ed996-d44a-4e39-998e-acb68681b0f9"
    )
    assert sampledata.historical_trades[0]["trade_id"] == x["trade_id"]
    assert sampledata.historical_trades[0]["price"] == x["price"]
    assert sampledata.historical_trades[0]["base_volume"] == x["base_volume"]
    assert sampledata.historical_trades[0]["quote_volume"] == x["target_volume"]
    assert sampledata.historical_trades[0]["timestamp"] == x["timestamp"]
    assert sampledata.historical_trades[0]["type"] == x["type"]


def test_round_to_str():
    assert convert.round_to_str(1.23456789, 4) == "1.2346"
    assert convert.round_to_str("1.23456789", 8) == "1.23456789"
    assert convert.round_to_str(Decimal(), 2) == "0.00"
    assert convert.round_to_str("foo", 4) == "0.0000"
    assert convert.round_to_str({"foo": "bar"}, 1) == "0.0"


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
    assert sumdata.json_key(sampledata.historical_trades, "base_volume") == Decimal(
        "90"
    )
    assert sumdata.json_key(sampledata.historical_trades, "quote_volume") == Decimal(
        "90"
    )


def test_sum_json_key_10f():
    assert (
        sumdata.json_key_10f(sampledata.historical_trades, "base_volume")
        == "90.0000000000"
    )
    assert (
        sumdata.json_key_10f(sampledata.historical_trades, "quote_volume")
        == "90.0000000000"
    )


def test_get_suffix():
    assert derive.suffix(1) == "24hr"
    assert derive.suffix(8) == "8d"


def test_sort_dict_list():
    x = sortdata.dict_lists(sampledata.historical_trades.copy(), "trade_id")
    assert x[0]["trade_id"] == "09d72ac9-3e55-4e84-9f32-cf22b5b442ad"
    x = sortdata.dict_lists(sampledata.historical_trades.copy(), "trade_id", True)
    assert x[0]["trade_id"] == "d2602fa9-6680-42f9-9cb8-20f76275f587"


def test_convert_orderbook_to_gecko():
    r = convert.orderbook_to_gecko(sampledata.orderbook_as_string)
    logger.calc(r)
    assert len(r["bids"]) == len(sampledata.orderbook_as_coords["bids"])
    assert r["bids"][0][1] == convert.format_10f(
        sampledata.orderbook_as_coords["bids"][0][1]
    )
    assert r["asks"][0][1] == convert.format_10f(
        sampledata.orderbook_as_coords["asks"][0][1]
    )


def test_pair_by_market_cap():
    a = sortdata.pair_by_market_cap(("BTC-segwit_KMD"))
    b = sortdata.pair_by_market_cap(("BTC_KMD"))
    c = sortdata.pair_by_market_cap(("KMD_BTC-segwit"))
    d = sortdata.pair_by_market_cap(("KMD_BTC"))
    e = sortdata.pair_by_market_cap(("BTC-segwit_KMD-BEP20"))

    assert b == d
    assert a == c
    assert e == "KMD-BEP20_BTC-segwit"
    assert sortdata.pair_by_market_cap(("MARTY_DOC")) == "DOC_MARTY"


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


# DERIVE


def test_derive_pair_cachename():
    r = derive.pair_cachename("ticker_info", "KMD_LTC", "24hr")
    assert r == "ticker_info_KMD_LTC_24hr"
    r = derive.pair_cachename("ticker_info", "KMD_LTC-segwit", "24hr")
    assert r == "ticker_info_KMD_LTC-segwit_24hr"
    r = derive.pair_cachename("ticker_info", "USDC_KMD", "14d")
    assert r == "ticker_info_USDC_KMD_14d"


def test_base_quote_from_pair():
    base, quote = derive.base_quote("XXX-PLG20_OLD_YYY-PLG20_OLD")
    assert base == "XXX-PLG20_OLD"
    assert quote == "YYY-PLG20_OLD"

    base, quote = derive.base_quote("XXX_YYY_OLD")
    assert base == "XXX"
    assert quote == "YYY_OLD"

    base, quote = derive.base_quote("XXX_OLD_YYY")
    assert base == "XXX_OLD"
    assert quote == "YYY"

    base, quote = derive.base_quote("XXX_YYY")
    assert base == "XXX"
    assert quote == "YYY"

    base, quote = derive.base_quote("XXX_YYY-PLG20")
    assert base == "XXX"
    assert quote == "YYY-PLG20"

    base, quote = derive.base_quote("XXX-PLG20_YYY")
    assert base == "XXX-PLG20"
    assert quote == "YYY"


def test_derive_coin_variants():
    r = derive.coin_variants("BTC")
    assert "BTC-BEP20" in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    assert len(r) == 3

    r = derive.coin_variants("BTC-segwit")
    assert "BTC-BEP20" in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    assert len(r) == 3

    r = derive.coin_variants("BTC", True)
    assert "BTC-BEP20" not in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    assert len(r) == 2

    r = derive.coin_variants("BTC-segwit", True)
    assert "BTC-BEP20" not in r
    assert "BTC-segwit" in r
    assert "BTC" in r
    assert len(r) == 2

    r = derive.coin_variants("BTC-BEP20", True)
    assert "BTC-BEP20" in r
    assert "BTC-segwit" not in r
    assert "BTC" not in r
    assert len(r) == 1

    r = derive.coin_variants("USDC")
    assert "USDC-BEP20" in r
    assert "USDC-PLG20" in r
    assert "USDC-PLG20_OLD" in r
    assert "USDC" not in r
    assert len(r) > 6


def test_get_pair_variants():
    r = derive.pair_variants("KMD-BEP20_BTC", segwit_only=True)
    assert "KMD-BEP20_BTC" in r
    assert "KMD-BEP20_BTC-segwit" in r

    assert "KMD-BEP20_BTC-BEP20" not in r
    assert "KMD_BTC" not in r
    assert "KMD_BTC-BEP20" not in r
    assert "KMD_BTC-segwit" not in r
    assert len(r) == 2

    r = derive.pair_variants("KMD_BTC", segwit_only=True)
    assert "KMD_BTC" in r
    assert "KMD_BTC-segwit" in r

    assert "KMD-BEP20_BTC-BEP20" not in r
    assert "KMD_BTC-BEP20" not in r
    assert "KMD-BEP20_BTC" not in r
    assert "KMD-BEP20_BTC-segwit" not in r
    assert len(r) == 2

    r = derive.pair_variants("KMD_BTC", segwit_only=False)
    assert "KMD_BTC" in r
    assert "KMD_BTC-BEP20" in r
    assert "KMD_BTC-segwit" in r
    assert "KMD-BEP20_BTC" in r
    assert "KMD-BEP20_BTC-segwit" in r
    assert "KMD-BEP20_BTC-BEP20" in r
    assert len(r) == 6

    r = derive.pair_variants("DGB_BTC", segwit_only=True)
    assert "DGB_BTC" in r
    assert "DGB_BTC-segwit" in r
    assert "DGB-segwit_BTC" in r
    assert "DGB-segwit_BTC-segwit" in r
    assert len(r) == 4

    r = derive.pair_variants("DGB_BTC")
    assert "DGB_BTC" in r
    assert "DGB_BTC-segwit" in r
    assert "DGB_BTC-BEP20" in r
    assert "DGB-segwit_BTC-BEP20" in r
    assert "DGB-segwit_BTC" in r
    assert "DGB-segwit_BTC-segwit" in r
    assert len(r) > 4

    r = derive.pair_variants("KMD-BEP20_LTC-segwit", segwit_only=True)
    assert "KMD-BEP20_LTC" in r
    assert "KMD-BEP20_LTC-segwit" in r
    assert len(r) == 2

    r = derive.pair_variants("KMD_LTC")
    assert "KMD_LTC" in r
    assert "KMD_LTC-segwit" in r
    assert "KMD-BEP20_LTC" in r
    assert "KMD-BEP20_LTC-segwit" in r
    assert len(r) == 4

    r = derive.pair_variants("KMD_LTC-segwit")
    assert "KMD_LTC" in r
    assert "KMD_LTC-segwit" in r
    assert "KMD-BEP20_LTC" in r
    assert "KMD-BEP20_LTC-segwit" in r
    assert len(r) == 4

    r = derive.pair_variants("LTC_KMD")
    assert "LTC_KMD" in r
    assert "LTC-segwit_KMD" in r
    assert "LTC_KMD-BEP20" in r
    assert "LTC-segwit_KMD-BEP20" in r
    assert len(r) == 4

    r = derive.pair_variants("KMD_USDC")
    assert "KMD_USDC" not in r
    assert "KMD_USDC-PLG20" in r
    assert "KMD_USDC-PLG20_OLD" in r
    assert "KMD-BEP20_USDC-PLG20_OLD" in r

    r = derive.pair_variants("KMD_USDC-PLG20")
    assert "KMD_USDC" not in r
    assert "KMD_USDC-PLG20" in r
    assert "KMD_USDC-PLG20_OLD" in r
    assert "KMD-BEP20_USDC-PLG20_OLD" in r

    r = derive.pair_variants("KMD_USDC-PLG20", segwit_only=True)
    assert "KMD_USDC" not in r
    assert "KMD_USDC-PLG20" in r
    assert "KMD-BEP20_USDC-PLG20" not in r
    assert len(r) == 1


def test_derive_price_at_finish():
    r = derive.price_at_finish(swap_item)
    assert "1700000777" in r
    assert r["1700000777"] == 0.01
    r = derive.price_at_finish(swap_item, is_reverse=True)
    assert "1700000777" in r
    assert r["1700000777"] == 100

    r = derive.price_at_finish(swap_item2)
    assert "1700000000" in r
    assert r["1700000000"] == 0.01
    r = derive.price_at_finish(swap_item2, is_reverse=True)
    assert "1700000000" in r
    assert r["1700000000"] == 100


def test_derive_gecko_price():
    price = derive.gecko_price("LTC")
    assert isinstance(price, Decimal)
    assert price == Decimal(100)

    price = derive.gecko_price("DOC")
    assert isinstance(price, Decimal)
    assert price == Decimal(0)


def test_derive_gecko_mcap():
    mcap = derive.gecko_mcap("LTC", gecko_source=gecko_source)
    assert isinstance(mcap, Decimal)
    assert mcap == Decimal(7000000000)

    mcap = derive.gecko_mcap("USDC", gecko_source=gecko_source)
    assert isinstance(mcap, Decimal)
    assert mcap == Decimal(7000000000)

    mcap = derive.gecko_mcap("USDC-PLG20", gecko_source=gecko_source)
    assert isinstance(mcap, Decimal)
    assert mcap == Decimal(7000000000)

    mcap = derive.gecko_mcap("DOC", gecko_source=gecko_source)
    assert isinstance(mcap, Decimal)
    assert mcap == Decimal(0)
