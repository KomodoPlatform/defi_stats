import pytest
from decimal import Decimal
from util.exceptions import BadPairFormatError
from tests.fixtures_validate import (
    setup_invert_pair_kmd_ltc,
)
from tests.fixtures_data import valid_tickers
import util.validate as validate
import util.memcache as memcache
import lib


gecko_source = memcache.get_gecko_source()
coins_config = memcache.get_coins_config()
last_traded_cache = memcache.get_last_traded()


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
