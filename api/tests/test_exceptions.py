import pytest
from decimal import Decimal
from lib.cache_item import CacheItem
from fixtures_validate import (
    setup_reverse_ticker_kmd_ltc,
)
from fixtures_data import valid_tickers

from util.exceptions import BadPairFormatError


def test_BadPairFormatError():
    with pytest.raises(BadPairFormatError):
        raise BadPairFormatError(msg="failed")
