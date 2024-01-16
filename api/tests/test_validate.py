import pytest
from decimal import Decimal
from util.exceptions import BadPairFormatError
from tests.fixtures_validate import (
    setup_invert_pair_kmd_ltc,
)
from tests.fixtures_data import valid_tickers
import util.validate as validate
import lib

coins_config = lib.load_coins_config()
gecko_source = lib.load_gecko_source()


def test_invert_pair(
    setup_invert_pair_kmd_ltc,
):
    assert setup_invert_pair_kmd_ltc == "LTC_KMD"


def test_validate_pair():
    assert validate.pair("KMD_LTC")
    with pytest.raises(BadPairFormatError):
        validate.pair("KMDLTC")
    with pytest.raises(TypeError):
        validate.pair(6521)


def test_validate_ticker_id():
    assert validate.ticker_id("KMD_LTC", valid_tickers) == "standard"
    assert validate.ticker_id("BTC_KMD", valid_tickers, True) == "reversed"
    assert validate.ticker_id("BTC_XXX", valid_tickers, True, True) == "failed"

    with pytest.raises(Exception):
        assert validate.ticker_id("BTC_XXX", valid_tickers)


def test_validate_positive_numeric():
    assert validate.positive_numeric(5, "var")
    assert validate.positive_numeric("5", "var")
    assert validate.positive_numeric(5.5, "var")
    assert validate.positive_numeric("5.5", "var")
    assert validate.positive_numeric(5, "var", True)
    assert validate.positive_numeric("5", "var", True)

    with pytest.raises(ValueError):
        validate.positive_numeric(-5, "var")
    with pytest.raises(ValueError):
        validate.positive_numeric(-5.5, "var")
    with pytest.raises(ValueError):
        validate.positive_numeric(5.5, "var", True)
    with pytest.raises(ValueError):
        validate.positive_numeric("5.5", "var", True)
    with pytest.raises(ValueError):
        validate.positive_numeric("foo", "var")


def test_validate_orderbook_pair():
    assert not validate.orderbook_pair("KMD", "KMD", coins_config)
    assert not validate.orderbook_pair("LTC", "LTC-segwit", coins_config)
    assert not validate.orderbook_pair("KMD", "XXX", coins_config)
    assert not validate.orderbook_pair("XXX", "KMD", coins_config)
    assert not validate.orderbook_pair("KMD", "ATOM", coins_config)
    assert not validate.orderbook_pair("ATOM", "KMD", coins_config)
    assert validate.orderbook_pair("KMD-BEP20", "KMD", coins_config)
    assert validate.orderbook_pair("KMD", "LTC-segwit", coins_config)
    assert validate.orderbook_pair("KMD", "LTC", coins_config)
    assert validate.orderbook_pair("LTC-segwit", "KMD", coins_config)
    assert validate.orderbook_pair("LTC", "KMD", coins_config)


def test_validate_loop_data():
    cache_item = lib.CacheItem("test")
    data = {"error": "foo"}
    assert not validate.loop_data(data, cache_item)
    data = {}
    assert not validate.loop_data(data, cache_item)
    data = {"data": "good"}
    assert validate.loop_data(data, cache_item)
    assert not validate.loop_data(None, cache_item)


def test_validate_json():
    data = "string"
    assert not validate.json_obj(data)
    data = {"decimal": Decimal(3)}
    assert not validate.json_obj(data)
    data = {"decimal": 3}
    assert validate.json_obj(data)
    data = {"list": [3, 9, 7]}
    assert validate.json_obj(data)
    data = [1, 2, 3, 4, 5]
    assert not validate.json_obj(data)
