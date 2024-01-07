import pytest
from decimal import Decimal
from util.exceptions import BadPairFormatError
from fixtures_validate import (
    setup_reverse_ticker_kmd_ltc,
)
from fixtures_data import valid_tickers

from util.validate import (
    validate_ticker_id,
    validate_positive_numeric,
    validate_orderbook_pair,
    validate_loop_data,
    validate_json,
    validate_pair,
)
import lib

coins_config = lib.load_coins_config(testing=True)
gecko_source = lib.load_gecko_source(testing=True)


def test_reverse_ticker(
    setup_reverse_ticker_kmd_ltc,
):
    assert setup_reverse_ticker_kmd_ltc == "LTC_KMD"


def test_validate_pair():
    assert validate_pair("KMD_LTC")
    with pytest.raises(BadPairFormatError):
        validate_pair("KMDLTC")
    with pytest.raises(TypeError):
        validate_pair(6521)


def test_validate_ticker_id():
    assert validate_ticker_id("KMD_LTC", valid_tickers) == "standard"
    assert validate_ticker_id("BTC_KMD", valid_tickers, True) == "reversed"
    assert validate_ticker_id("BTC_XXX", valid_tickers, True, True) == "failed"

    with pytest.raises(Exception):
        assert validate_ticker_id("BTC_XXX", valid_tickers)


def test_validate_positive_numeric():
    assert validate_positive_numeric(5, "var")
    assert validate_positive_numeric("5", "var")
    assert validate_positive_numeric(5.5, "var")
    assert validate_positive_numeric("5.5", "var")
    assert validate_positive_numeric(5, "var", True)
    assert validate_positive_numeric("5", "var", True)

    with pytest.raises(ValueError):
        validate_positive_numeric(-5, "var")
    with pytest.raises(ValueError):
        validate_positive_numeric(-5.5, "var")
    with pytest.raises(ValueError):
        validate_positive_numeric(5.5, "var", True)
    with pytest.raises(ValueError):
        validate_positive_numeric("5.5", "var", True)
    with pytest.raises(ValueError):
        validate_positive_numeric("foo", "var")


def test_validate_orderbook_pair():
    assert not validate_orderbook_pair("KMD", "KMD", coins_config)
    assert not validate_orderbook_pair("LTC", "LTC-segwit", coins_config)
    assert not validate_orderbook_pair("KMD", "XXX", coins_config)
    assert not validate_orderbook_pair("XXX", "KMD", coins_config)
    assert not validate_orderbook_pair("KMD", "ATOM", coins_config)
    assert not validate_orderbook_pair("ATOM", "KMD", coins_config)
    assert validate_orderbook_pair("KMD-BEP20", "KMD", coins_config)
    assert validate_orderbook_pair("KMD", "LTC-segwit", coins_config)
    assert validate_orderbook_pair("KMD", "LTC", coins_config)
    assert validate_orderbook_pair("LTC-segwit", "KMD", coins_config)
    assert validate_orderbook_pair("LTC", "KMD", coins_config)


def test_validate_loop_data():
    cache_item = lib.CacheItem("test", testing=True)
    data = {"error": "foo"}
    assert not validate_loop_data(data, cache_item)
    data = {}
    assert not validate_loop_data(data, cache_item)
    data = {"data": "good"}
    assert validate_loop_data(data, cache_item)


def test_validate_json():
    data = "string"
    assert not validate_json(data)
    data = {"decimal": Decimal(3)}
    assert not validate_json(data)
    data = {"decimal": 3}
    assert validate_json(data)
    data = {"list": [3, 9, 7]}
    assert validate_json(data)
