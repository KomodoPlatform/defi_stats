import pytest
from decimal import Decimal
from util.exceptions import BadPairFormatError
from tests.fixtures_data import sampledata
from tests.fixtures_validate import setup_invert_pair_kmd_ltc
import util.validate as validate
import util.memcache as memcache
from lib.cache import CacheItem


gecko_source = memcache.get_gecko_source()
coins_config = memcache.get_coins_config()
pairs_last_trade_cache = memcache.get_pairs_last_traded()


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
    cache_item = CacheItem("test")
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
    data = sampledata.historical_data
    assert validate.json_obj(data)
    data = (5, 8)
    assert not validate.json_obj(data)


def test_is_valid_hex():
    assert not validate.is_valid_hex("ojqwvu02")
    assert validate.is_valid_hex("23BEEF")


def test_is_bridge_swap():
    assert validate.is_bridge_swap("KMD_KMD-BEP20")
    assert not validate.is_bridge_swap("KMD_LTC")


def test_is_7777():
    assert validate.is_7777("seed.file")
    assert not validate.is_7777("notseed.file")


def test_is_pair_priced():
    assert validate.is_pair_priced("KMD_MATIC", gecko_source)
    assert not validate.is_pair_priced("KMD_MARTY", gecko_source)
    assert not validate.is_pair_priced("MARTY_PPP", gecko_source)
    assert not validate.is_pair_priced("PPP_OOO", gecko_source)
